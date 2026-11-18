//! The host daemon, `pando serve`, and its client, `pando --socket PATH <command>`.
//!
//! The daemon holds what every command would set up again: the store's client and its
//! connections, the cache, and the checkpoints it has read. A client sends its arguments over
//! the socket, the daemon runs the command and streams its output back, and the client exits
//! with the command's exit code. S3 settings and credentials stay with the daemon.
//!
//! A request is the argument count on a line, then each argument as its length in bytes on a
//! line followed by its bytes. The answer is lines: `o<text>` for standard output, `e<text>` for
//! standard error, and last `x<code>`. A client that goes away cancels its command, except an
//! `attach`, whose device keeps serving until `detach`. Devices the daemon serves stop when
//! the daemon stops; `attach --recover` takes them over.

use std::ffi::OsString;
use std::fs::Permissions;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::Parser;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;

use crate::{Cli, Command, Host};

/// Arguments a request may carry, and bytes per argument.
const MAX_ARGS: usize = 256;
const MAX_ARG: usize = 64 << 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stream {
    Out,
    Err,
}

/// Where a command writes its output: this process's standard output and error, or the
/// client of the daemon that runs it.
#[derive(Clone, Debug)]
pub(crate) struct Out(Option<mpsc::UnboundedSender<(Stream, String)>>);

impl Out {
    pub(crate) fn stdio() -> Out {
        Out(None)
    }

    /// Writes one line to standard output.
    pub(crate) fn line(&self, s: impl Into<String>) {
        self.send(Stream::Out, s.into());
    }

    /// Writes one line to standard error.
    pub(crate) fn err(&self, s: impl Into<String>) {
        self.send(Stream::Err, s.into());
    }

    fn send(&self, to: Stream, s: String) {
        match (&self.0, to) {
            (None, Stream::Out) => println!("{s}"),
            (None, Stream::Err) => eprintln!("{s}"),
            (Some(tx), _) => {
                let _ = tx.send((to, s));
            }
        }
    }
}

/// Serves requests on `socket` until the process stops.
pub(crate) async fn serve(host: Arc<Host>, socket: &Path) -> Result<()> {
    let l = listen(socket).await?;
    eprintln!("serving on {}", socket.display());
    loop {
        let (c, _) = l
            .accept()
            .await
            .with_context(|| format!("accept on {}", socket.display()))?;
        let host = Arc::clone(&host);
        tokio::spawn(async move {
            if let Err(e) = answer(host, c).await {
                eprintln!("request: {e:#}");
            }
        });
    }
}

/// Binds `socket`, readable only by this user, unless a live daemon serves it already.
async fn listen(socket: &Path) -> Result<UnixListener> {
    if UnixStream::connect(socket).await.is_ok() {
        bail!("a daemon serves {} already", socket.display());
    }
    if let Some(dir) = socket.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    }
    match std::fs::remove_file(socket) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            return Err(e).with_context(|| format!("remove {}", socket.display()));
        }
        _ => {}
    }
    let l = UnixListener::bind(socket).with_context(|| format!("bind {}", socket.display()))?;
    std::fs::set_permissions(socket, Permissions::from_mode(0o600))?;
    Ok(l)
}

async fn answer(host: Arc<Host>, c: UnixStream) -> Result<()> {
    let (r, mut w) = c.into_split();
    let mut r = BufReader::new(r);
    if r.fill_buf().await?.is_empty() {
        return Ok(());
    }
    let args = read_args(&mut r).await?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (job, keep) = {
        let out = Out(Some(tx));
        match request(&host, args) {
            Ok(cli) => {
                let keep = matches!(cli.command, Command::Attach(_));
                let job = tokio::spawn(async move {
                    match crate::run(&host, cli.command, &out).await {
                        Ok(()) => 0,
                        Err(e) => {
                            out.err(format!("Error: {e:?}"));
                            1
                        }
                    }
                });
                (job, keep)
            }
            Err(Refused::Clap(e)) => {
                out.send(
                    if e.use_stderr() { Stream::Err } else { Stream::Out },
                    e.render().to_string().trim_end().to_owned(),
                );
                let code = e.exit_code();
                (tokio::spawn(async move { code }), false)
            }
            Err(Refused::Other(e)) => {
                out.err(format!("Error: {e:#}"));
                (tokio::spawn(async { 2 }), false)
            }
        }
    };
    let mut gone = false;
    let mut probe = [0u8; 1];
    loop {
        tokio::select! {
            line = rx.recv() => {
                let Some((to, s)) = line else { break };
                if !gone && write_lines(&mut w, to, &s).await.is_err() {
                    gone = true;
                    if !keep {
                        job.abort();
                    }
                }
            }
            n = r.read(&mut probe), if !gone => {
                if !matches!(n, Ok(n) if n > 0) {
                    gone = true;
                    if !keep {
                        job.abort();
                    }
                }
            }
        }
    }
    let code = job.await.unwrap_or(130);
    if !gone {
        w.write_all(format!("x{code}\n").as_bytes()).await?;
    }
    Ok(())
}

enum Refused {
    Clap(clap::Error),
    Other(anyhow::Error),
}

/// The command a request asks for, when this daemon may run it.
fn request(host: &Host, args: Vec<OsString>) -> Result<Cli, Refused> {
    let cli = Cli::try_parse_from(args).map_err(Refused::Clap)?;
    let other = |why: String| Err(Refused::Other(anyhow::anyhow!(why)));
    if matches!(cli.command, Command::Serve) {
        return other("the daemon is serving already".into());
    }
    if cli.store.identity_or(&host.remote) != host.identity {
        return other("this daemon serves another store; ask it without store options".into());
    }
    if cli.cache.as_ref().is_some_and(|c| *c != host.cache_dir) {
        return other(format!(
            "this daemon's cache is {}; ask it without --cache",
            host.cache_dir.display()
        ));
    }
    Ok(cli)
}

async fn read_args(r: &mut (impl AsyncBufRead + Unpin)) -> Result<Vec<OsString>> {
    let n: usize = read_number(r).await?;
    if n > MAX_ARGS {
        bail!("{n} arguments");
    }
    let mut args = Vec::with_capacity(n);
    for _ in 0..n {
        let len = read_number(r).await?;
        if len > MAX_ARG {
            bail!("an argument of {len} bytes");
        }
        let mut b = vec![0; len];
        r.read_exact(&mut b).await?;
        args.push(OsString::from_vec(b));
    }
    Ok(args)
}

async fn read_number(r: &mut (impl AsyncBufRead + Unpin)) -> Result<usize> {
    let mut line = String::new();
    r.take(32).read_line(&mut line).await?;
    line.trim_end_matches('\n')
        .parse()
        .with_context(|| format!("bad request line {line:?}"))
}

async fn write_lines(w: &mut (impl AsyncWriteExt + Unpin), to: Stream, s: &str) -> std::io::Result<()> {
    let tag = match to {
        Stream::Out => 'o',
        Stream::Err => 'e',
    };
    let mut b = String::with_capacity(s.len() + 2);
    for line in s.split('\n') {
        b.push(tag);
        b.push_str(line);
        b.push('\n');
    }
    w.write_all(b.as_bytes()).await
}

fn encode_args(args: &[OsString]) -> Vec<u8> {
    let mut b = format!("{}\n", args.len()).into_bytes();
    for a in args {
        b.extend_from_slice(format!("{}\n", a.len()).as_bytes());
        b.extend_from_slice(a.as_bytes());
    }
    b
}

/// Sends `args`, this process's arguments, to the daemon on `socket` and prints what it
/// answers. The exit code is the command's.
pub(crate) async fn client(socket: &Path, args: Vec<OsString>) -> Result<ExitCode> {
    let c = UnixStream::connect(socket).await.with_context(|| {
        format!(
            "connect to the daemon on {}; is `pando serve` running?",
            socket.display()
        )
    })?;
    let (r, mut w) = c.into_split();
    w.write_all(&encode_args(&args)).await?;
    let mut lines = BufReader::new(r).lines();
    while let Some(line) = lines.next_line().await? {
        let (tag, text) = line.split_at(line.chars().next().map_or(0, char::len_utf8));
        match tag {
            "o" => println!("{text}"),
            "e" => eprintln!("{text}"),
            "x" => {
                let code: u8 = text.parse().unwrap_or(1);
                return Ok(ExitCode::from(code));
            }
            _ => bail!("the daemon answered {line:?}"),
        }
    }
    bail!("the daemon closed the connection before the command finished")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn arguments_survive_the_socket_encoding() {
        let args: Vec<OsString> = ["pando", "branch", "a b", "", "line\nbreak"]
            .into_iter()
            .map(OsString::from)
            .collect();
        let b = encode_args(&args);
        let got = read_args(&mut BufReader::new(&b[..])).await.unwrap();
        assert_eq!(got, args);
    }

    #[tokio::test]
    async fn a_request_with_too_many_arguments_is_refused() {
        let b = format!("{}\n", MAX_ARGS + 1);
        assert!(read_args(&mut BufReader::new(b.as_bytes())).await.is_err());
        assert!(
            read_args(&mut BufReader::new(&b"1\n99999999\n"[..]))
                .await
                .is_err()
        );
        assert!(read_args(&mut BufReader::new(&b"x\n"[..])).await.is_err());
    }

    #[tokio::test]
    async fn output_lines_carry_their_stream() {
        let mut b = Vec::new();
        write_lines(&mut b, Stream::Out, "one\ntwo").await.unwrap();
        write_lines(&mut b, Stream::Err, "bad").await.unwrap();
        assert_eq!(b, b"oone\notwo\nebad\n");
    }
}
