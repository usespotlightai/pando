#![forbid(unsafe_code)]

mod config;

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use pando_engine::ports::Store;
use pando_format::{Ref, keys};

#[derive(Parser, Debug)]
#[command(version, about = "Branchable copy-on-write block devices on object storage")]
struct Cli {
    #[command(flatten)]
    store: config::StoreArgs,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Lists the branches in the store: name, size, writer generation and root.
    Refs,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let store: Arc<dyn Store> = cli.store.open()?;
    match cli.command {
        Command::Refs => refs(store.as_ref()).await,
    }
}

async fn refs(store: &dyn Store) -> Result<()> {
    for o in store.list(keys::REFS).await? {
        let name = o.key.strip_prefix(&format!("{}/", keys::REFS)).unwrap_or(&o.key);
        let r = Ref::decode(&store.get(&o.key).await?.body).with_context(|| format!("ref {name}"))?;
        let from = r.from.map(|f| format!(" from {f}")).unwrap_or_default();
        println!("{name} {} {} {}{from}", r.size, r.generation, r.root.hash.short());
    }
    Ok(())
}
