# One store per org

Each org gets a store of its own: a prefix of the bucket, `--remote s3://<org>`, with
credentials that reach only that prefix. A host serving an org keeps that org's cache and branch
state apart from every other org's, and `pando purge` deletes an org's prefix when it leaves.

## Credentials

`S3_CREDENTIALS` says where the credentials come from. None of them is ever a flag, so none
shows in a process's arguments, and no error or log line holds a key, a secret or a token.

| `S3_CREDENTIALS` | Credentials | Refreshed |
|---|---|---|
| `keys` (the default) | `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` and, for a temporary key, `S3_SESSION_TOKEN` | never: a temporary key fails once it expires |
| `instance` | the role of this EC2 instance or AgentCore microVM, from the instance metadata service (IMDSv2) | five minutes before they expire |
| `aws` | the AWS chain: web identity (`AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ROLE_ARN`, as EKS sets them), else ECS container credentials (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`), else EKS Pod Identity (`AWS_CONTAINER_CREDENTIALS_FULL_URI` and `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`), else the instance's role | five minutes before they expire |
| `file` | the JSON file `S3_CREDENTIALS_FILE` names, which another process keeps current | whenever the file changes |

The file has the form that `credential_process` prints:

```json
{"Version":1,"AccessKeyId":"…","SecretAccessKey":"…","SessionToken":"…","Expiration":"2026-11-20T18:00:00Z"}
```

Each request to the store checks the file's size, modification time and inode, and reads it
again if any changed, or every 5 seconds regardless. Write the new key to a file next to it, then
rename it over the old one, more than a minute before the old key expires. A key counts as
expired a minute before its `Expiration`. If the key in the file has expired and the file has not
changed, requests fail as transient: uploads wait and retry until the file changes, and the branch reports
`lagging` meanwhile. Keep the file mode 0600, owned by root.

## A prefix per org

Name each org's store by a prefix no other org's store starts with, such as the org's ID:
`--remote s3://org_2x7f`. Pando lists keys under `<prefix>/` only, so `org-a` never sees
`org-ab`.

### AWS S3

A role per org, or one role whose sessions carry a session policy, allows only the org's
prefix:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::BUCKET/ORG/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::BUCKET",
      "Condition": {"StringLike": {"s3:prefix": ["ORG/*"]}}
    }
  ]
}
```

The conditional writes of refs (`If-Match`, `If-None-Match`) need only `s3:PutObject`.

- **A role per org:** give the host that role through the `aws` or `instance` source.
- **One role for the service:** have the control plane call `sts:AssumeRole` with the policy
  above as the session's `Policy` and a `DurationSeconds` of an hour or more. It writes the
  result to the host's credentials file and renews it well before `Expiration`. A session policy
  only narrows the role, so the role itself may allow the whole bucket.

### Cloudflare R2

R2 has no IAM, but its
[temporary credentials](https://developers.cloudflare.com/r2/api/s3/temporary-credentials/)
narrow a parent API token to one bucket, the `object-read-write` permission, and the prefixes in
`prefixes` (`["ORG/"]`). They expire after `ttlSeconds`.

1. Mint them from a parent token that only the control plane holds.
2. Write them to the host's credentials file, setting `Expiration` to the time of issue plus
   `ttlSeconds`.
3. Mint the next set before that.

Revoking the parent token ends every credential minted from it at once.

## On a host

**Separate directories per store.** A cache directory (`--cache`) and a state directory
(`attach --state`) belong to the store that first used them. Pando writes the store's name
(`endpoint|bucket|s3://prefix`, no secret) to a `store` file in each. Any other store's command
on that directory fails before it reads anything there. A host that serves several orgs gives
each org:

- its own `--cache`;
- its own `--state`;
- with the daemon, its own `serve --socket`;
- on one host, its own GC: `pando-gc@<org>.timer`, with the org's prefix and credentials in
  `/etc/pando/gc/<org>.env` ([hosts.md](hosts.md#gc)).

**Base images may be shared.** The `--images` directory can serve every org:

- A base image is named by the hash of the root it holds.
- A block taken from another base is checked against the hash of the chunk it builds.
- The chunks one store's branches learn go under `learned/<hash of the store's name>`, and no
  other store reads them.
- Recordings of first reads live in the store, under the org's prefix.

## When an org leaves

1. Detach every branch of the org on every host, stop the org's daemons, and stop its GC:
   `systemctl disable --now pando-gc@ORG.timer`.
2. Purge the prefix, naming it twice:

   ```
   pando --remote s3://ORG purge --confirm ORG
   ```

   It deletes the refs first, so no host can open a branch of the org from then on. Then it
   deletes the checkpoints, the write logs, the change lists, the segments and anything else
   under `ORG/`. It lists one part at a time, so it holds no more in memory than GC does, and
   reports each part as it finishes, then the objects and bytes in all. It
   lists the prefix again at the end and fails if anything is there, which means a host still
   writes. In that case, detach that host and purge again. It refuses an empty prefix: it never
   deletes a whole bucket.
3. In a versioned bucket, the deleted objects stay as noncurrent versions until the bucket's
   lifecycle rule expires them (`NoncurrentVersionExpiration`). To remove them at once, use
   credentials allowed `s3:ListBucketVersions` and `s3:DeleteObjectVersion`:

   ```
   aws s3api list-object-versions --bucket BUCKET --prefix ORG/ \
     --query '{Objects: [Versions, DeleteMarkers][].{Key: Key, VersionId: VersionId}}' --output json >versions.json
   aws s3api delete-objects --bucket BUCKET --delete file://versions.json
   ```

   `delete-objects` takes at most 1000 keys a call; repeat until the listing is empty.
4. On each host that served the org, remove the org's `--cache` and `--state` directories, and
   its `learned/<hash>` directory under `--images`. On its GC host, remove
   `/etc/pando/gc/ORG.env` and `/var/cache/pando-gc/ORG`. Base images go with the follower's
   `--keep` and `--budget`, or remove them by hand.
5. Revoke the org's role, or stop minting its credentials.
