# TRAVERSE

TRAVERSE is the server-side Firebase-to-MySQL projection service for RBMSv4.
Firestore is authoritative for application mutations. MySQL is a read
projection. TRAVERSE never receives browser credentials and never writes
passwords or password hashes.

## Installation

Run these steps as a deployment administrator on the target host. Replace
paths only when the installed RBMS root is different.

1. Install Node.js, MySQL/MariaDB, and the project dependencies.
2. Copy the RBMS source and run `npm install` in the project root.
3. Place the Firebase Admin SDK service-account JSON outside the web root with
   mode `0600`, owned by the service administrator.
4. Create the dedicated MySQL account using the root account. Do not put the
   password in Git, browser code, logs, or the web-root `.env`:

```sql
CREATE USER 'traverse'@'localhost' IDENTIFIED BY '[SET_A_SECRET_LOCALLY]';
GRANT ALL PRIVILEGES ON `rbmsv4`.* TO 'traverse'@'localhost';
FLUSH PRIVILEGES;
```

The account is scoped to the RBMSv4 database. Do not grant `*.*` or `GRANT
OPTION` unless a separate security review explicitly requires it.

5. Create `/etc/rbmsv4-firebase-mysql-sync.env` as `root:root`, mode `0600`:

```text
FIREBASE_MYSQL_SYNC_DB_HOST=127.0.0.1
FIREBASE_MYSQL_SYNC_DB_PORT=3306
FIREBASE_MYSQL_SYNC_DB_USER=traverse
FIREBASE_MYSQL_SYNC_DB_PASSWORD=[SET_A_SECRET_LOCALLY]
FIREBASE_MYSQL_SYNC_DB_NAME=rbmsv4
```

6. Set `FIREBASE_PROJECT_ID` and
`GOOGLE_APPLICATION_CREDENTIALS` in the project environment. The service
account must be able to read the allowlisted Firestore collections and update
only synchronization metadata during acknowledgement.
7. Install the unit and start it:

```bash
sudo install -o root -g root -m 0644 deploy/systemd/traverse.service /etc/systemd/system/traverse.service
sudo systemctl daemon-reload
sudo systemctl enable --now traverse.service
sudo systemctl status traverse.service
```

If the old unit is installed, stop it before enabling TRAVERSE so two workers
do not process the same queue:

```bash
sudo systemctl disable --now rbmsv4-firebase-mysql-sync.service
```

## Operation and logs

```bash
sudo systemctl restart traverse.service
sudo systemctl is-active traverse.service
sudo journalctl -u traverse.service -f -o cat
```

Normal lifecycle is `QUEUED -> CLAIMED -> ACK_PENDING -> ACKED`. Transient
errors use bounded `RETRY_WAIT`; invalid or exhausted work uses
`DEAD_LETTER`. `SUPERSEDED` means a newer Firebase revision won.

The durable control tables are `firebase_mysql_sync_queue`,
`firebase_mysql_sync_projection_state`,
`firebase_mysql_sync_attempt_history`,
`firebase_mysql_sync_field_registry`,
`firebase_mysql_sync_collection_registry`, and
`firebase_mysql_sync_migration_history`.

## Schema rule

For each Firebase-backed projection table, the mirrored fields are derived
from Firebase. The only service-owned extra field is:

```text
xId INT(10) AUTO_INCREMENT PRIMARY KEY
```

Legacy fields such as `created_at`, `updated_at`, unprefixed status flags,
relationship fields removed from the Firebase contract, and password/hash
fields are not valid projection fields. A schema repair must create a
verified timestamped backup first, preserve no truncated values, and stop if
the Firebase collection has no established schema. Empty collections are not
permission to drop every MySQL column.

Firebase document IDs are authoritative and must equal their corresponding
`*_key` field. New mutations begin with `mysql_sync_status=PENDING`; only
TRAVERSE may acknowledge `SYNCED` after exact MySQL read-back.

## Verification

Offline checks:

```bash
node --check scripts/traverse.mjs
node --check scripts/firebase-mysql-sync-master.mjs
node tests/firebase-mysql-sync-master.test.mjs
git diff --check
```

Live verification must separately prove Firebase read-back, queue discovery,
MySQL projection, `SYNCED` metadata, exact field read-back, retry/dead-letter
handling, and tenant/project isolation. A service being `active` alone is not
proof that a document synchronized.

## Backup and rollback

TRAVERSE records backup metadata in
`firebase_mysql_sync_migration_history`. Backups are not automatically
deleted. Before restoring a table, stop TRAVERSE, verify the intended backup
schema and row count, restore with a separately reviewed SQL operation, then
restart and verify the Firebase revision fence. Never restore by dropping an
unknown table or by copying credentials into a command log.

## Scope

TRAVERSE covers only the compiled Firebase collections in
`scripts/firebase-mysql-sync/registry.mjs`. It does not synchronize BuilderX,
Phase Manager, internal AI tables, or MySQL back to Firebase.
