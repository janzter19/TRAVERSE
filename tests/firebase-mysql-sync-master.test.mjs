import assert from 'node:assert/strict'
import fs from 'node:fs'
import path from 'node:path'
import test from 'node:test'
import { fileURLToPath } from 'node:url'

import { attachPendingListeners, isMainModule, scanPendingPages } from '../scripts/firebase-mysql-sync-master.mjs'
import { acknowledgePending } from '../scripts/firebase-mysql-sync/acknowledger.mjs'
import { backupName, createVerifiedBackup } from '../scripts/firebase-mysql-sync/backup.mjs'
import { COLLECTIONS, collectionContract, quoteIdentifier } from '../scripts/firebase-mysql-sync/registry.mjs'
import { backoffSeconds, claimReady, enqueueRevision, failureOutcome, projectionDecision, projectionSql, transitionQueue } from '../scripts/firebase-mysql-sync/queue-store.mjs'
import { alterStatement, columnStorageDecision, ensureFieldRegistry, existingDefinition, repairParity, repairPlan, sqlType } from '../scripts/firebase-mysql-sync/schema-parity.mjs'
import { finalizeSyncMetadata, processClaimed, projectRevision } from '../scripts/firebase-mysql-sync/worker.mjs'
import { assertDocument, binaryType, compareRevisions, encodeValue, firestoreRevision, inferType, normalizeDatabaseValue, payloadFingerprint, promoteType, stableJson, stringType, valuesEqual } from '../scripts/firebase-mysql-sync/types.mjs'

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)))
const revision = (seconds, nanoseconds) => ({ updateTime: { seconds, nanoseconds } })
const config = { maxDocumentBytes: 1_048_576, maxFieldsPerCollection: 160, maxNewFieldsPerScan: 12, maxAttempts: 4, advisoryLockSeconds: 2, backupTimezone: 'Asia/Manila', backupMaxTableBytes: 1_073_741_824 }
const portalLifecycle = (collection) => ({ firebase_collection: collection, mysql_created_at: '2026-08-26 12:00:00.000000', mysql_updated_at: '2026-08-26 12:00:00.000000', mysql_deleted_at: null, mysql_synced_at: null, mysql_sync_status: 'PENDING', created_at: '2026-08-26 12:00:00.000000', updated_at: '2026-08-26 12:00:00.000000' })

test('direct-run guard canonicalizes the installed systemd entrypoint without starting the service', () => {
  const modulePath = path.join(root, 'scripts/firebase-mysql-sync-master.mjs')
  const installedPath = '/var/www/html/rbmsv4/scripts/firebase-mysql-sync-master.mjs'
  const moduleUrl = new URL('../scripts/firebase-mysql-sync-master.mjs', import.meta.url).href
  assert.notEqual(installedPath, modulePath)
  assert.equal(fs.realpathSync(installedPath), fs.realpathSync(modulePath))
  assert.equal(isMainModule({ invocationPath: installedPath, moduleUrl }), true)
  assert.equal(isMainModule({ invocationPath: null, moduleUrl }), false)
  assert.equal(isMainModule({ invocationPath: '/missing/firebase-mysql-sync-master.mjs', moduleUrl }), false)
  assert.equal(isMainModule({ invocationPath: installedPath, moduleUrl: 'not-a-file-url' }), false)
})

test('allowlist, identifier validation, and string/key SQL normalization are strict', () => {
  assert.equal(collectionContract('project_bed_task').key, 'bed_task_key')
  assert.throws(() => collectionContract('unknown_collection'), /collection_not_allowlisted/)
  assert.equal(quoteIdentifier('safe_field_1'), '`safe_field_1`')
  assert.throws(() => quoteIdentifier('bad-name'), /identifier_invalid/)
  assert.equal(sqlType('string'), 'TEXT')
  assert.equal(sqlType('text', true), 'VARCHAR(255)')
  assert.throws(() => assertDocument(collectionContract('project_bed_task'), { id: 'doc-a', data: () => ({ bed_task_key: 'doc-b' }) }, 1_048_576), /firestore_document_id_key_mismatch/)
  const longId = 'x'.repeat(256)
  assert.throws(() => assertDocument(collectionContract('project_bed_task'), { id: longId, data: () => ({ bed_task_key: longId }) }, 1_048_576), /firestore_document_id_too_long/)
})

test('Portal group, position, assignment, and Auth-owned user contracts are allowlisted and lifecycle-complete', () => {
  assert.deepEqual(
    Object.fromEntries(['project_group', 'project_position', 'project_user_group', 'project_user'].map((name) => [name, [COLLECTIONS[name].table, COLLECTIONS[name].key]])),
    { project_group: ['project_group', 'group_key'], project_position: ['project_position', 'position_key'], project_user_group: ['project_user_group', 'assignment_key'], project_user: ['project_user', 'user_key'] },
  )
  const group = { group_key: 'group-auth-1', project_key: 'project-1', group_name: 'Nursing', group_status: 'ACTIVE', ...portalLifecycle('project_group') }
  assert.deepEqual(assertDocument(collectionContract('project_group'), { id: group.group_key, data: () => group }, 1_048_576), group)
  assert.equal(assertDocument(collectionContract('project_group'), { id: group.group_key, data: () => ({ ...group, members: [] }) }, 1_048_576).members.length, 0)

  const position = { position_key: 'position-auth-1', project_key: 'project-1', group_key: group.group_key, position_code: 'RN', position_name: 'Registered Nurse', position_status: 'ACTIVE', ...portalLifecycle('project_position') }
  assert.deepEqual(assertDocument(collectionContract('project_position'), { id: position.position_key, data: () => position }, 1_048_576), position)
  assert.throws(() => assertDocument(collectionContract('project_position'), { id: position.position_key, data: () => ({ ...position, group_key: undefined }) }, 1_048_576), /firestore_field_identifier_invalid|undefined_value_unsupported|firestore_document_id_key_mismatch/)

  const assignment = { assignment_key: 'assignment-auth-1', project_key: 'project-1', group_key: group.group_key, user_key: 'auth-uid-1', position_key: position.position_key, assignment_status: 'ACTIVE', ...portalLifecycle('project_user_group') }
  assert.deepEqual(assertDocument(collectionContract('project_user_group'), { id: assignment.assignment_key, data: () => assignment }, 1_048_576), assignment)
  assert.equal(assertDocument(collectionContract('project_user_group'), { id: assignment.assignment_key, data: () => ({ ...assignment, members: [] }) }, 1_048_576).members.length, 0)

  const userLifecycle = portalLifecycle('project_user'); delete userLifecycle.created_at; delete userLifecycle.updated_at
  const user = { user_key: 'auth-uid-1', firebase_uid: 'auth-uid-1', project_key: 'project-1', user_login: 'nurse.one', user_name: 'Nurse One', user_status: 'ACTIVE', user_password_change_required: false, user_disabled: false, user_deleted: false, user_locked: false, firebase_created_at: '2026-08-26T12:00:00.000Z', firebase_updated_at: '2026-08-26T12:00:00.000Z', firebase_deleted_at: null, ...userLifecycle }
  assert.deepEqual(assertDocument(collectionContract('project_user'), { id: user.user_key, data: () => user }, 1_048_576), user)
  for (const forbidden of ['group_key', 'position_key', 'user_password_hash', 'password', 'plaintext_password']) {
    assert.throws(() => assertDocument(collectionContract('project_user'), { id: user.user_key, data: () => ({ ...user, [forbidden]: 'secret' }) }, 1_048_576), /firestore_field_not_allowlisted/)
  }
  assert.throws(() => assertDocument(collectionContract('project_user'), { id: user.user_key, data: () => ({ ...user, firebase_uid: 'different-auth-uid' }) }, 1_048_576), /firestore_auth_uid_document_id_mismatch/)
  assert.throws(() => assertDocument(collectionContract('project_user'), { id: user.user_key, data: () => ({ ...user, user_disabled: true }) }, 1_048_576), /firestore_user_disabled_or_deleted/)
  assert.throws(() => assertDocument(collectionContract('project_user'), { id: user.user_key, data: () => ({ ...user, user_deleted: true }) }, 1_048_576), /firestore_user_disabled_or_deleted/)
  assert.throws(() => assertDocument(collectionContract('project_user'), { id: user.user_key, data: () => ({ ...user, mysql_sync_status: 'SYNCED' }) }, 1_048_576), /firestore_sync_status_not_pending/)
  assert.throws(() => assertDocument(collectionContract('project_user'), { id: user.user_key, data: () => ({ ...user, mysql_deleted_at: undefined }) }, 1_048_576), /undefined_value_unsupported/)
})

test('Auth-owned user projection SQL excludes credentials and relationship fields while preserving Firebase identity', () => {
  const contract = collectionContract('project_user')
  const fieldNames = ['user_key', 'firebase_uid', 'project_key', 'user_login', 'user_name']
  const sql = projectionSql(contract, fieldNames)
  assert.match(sql, /`firebase_uid`/)
  for (const forbidden of ['group_key', 'position_key', 'user_password_hash', 'password']) assert.equal(fieldNames.includes(forbidden), false)
  assert.equal(sql.includes('user_password_hash'), false)
  assert.equal(sql.includes('group_key'), false)
})

test('Firestore revisions preserve nanoseconds and compare canonically', () => {
  assert.equal(firestoreRevision(revision(1720000000, 123456789)), '1720000000:123456789')
  assert.equal(compareRevisions('1720000000:123456788', '1720000000:123456789'), -1)
  assert.equal(compareRevisions('1720000001:000000000', '1720000000:999999999'), 1)
  assert.throws(() => compareRevisions('1720000000:123', '1720000000:000000123'), /firestore_revision_invalid/)
})

test('timestamps accept UTC ISO or microsecond Firestore values and reject lossy/invalid input', () => {
  const timestamp = { seconds: 1720000000, nanoseconds: 123456000, toDate() { return new Date(this.seconds * 1000) } }
  assert.equal(encodeValue(timestamp, 'timestamp'), '2024-07-03 09:46:40.123456')
  assert.equal(encodeValue('2024-07-03T09:46:40.1234Z', 'timestamp'), '2024-07-03 09:46:40.123400')
  assert.equal(normalizeDatabaseValue('2024-07-03 09:46:40.123400', 'timestamp'), '2024-07-03 09:46:40.123400')
  assert.throws(() => inferType('not-a-date', 'timestamp'), /timestamp_invalid/)
  assert.throws(() => encodeValue({ ...timestamp, nanoseconds: 123456789 }, 'timestamp'), /timestamp_precision_unsupported/)
  assert.throws(() => promoteType('timestamp', 'text'), /timestamp_type_mismatch/)
})

test('TEXT/BLOB promotion uses MySQL byte limits without slicing', () => {
  assert.equal(stringType(65_535), 'text')
  assert.equal(stringType(65_536), 'mediumtext')
  assert.equal(stringType(16_777_215), 'mediumtext')
  assert.equal(stringType(16_777_216), 'longtext')
  assert.equal(binaryType(65_535), 'blob')
  assert.equal(binaryType(65_536), 'mediumblob')
  assert.equal(promoteType('text', 'mediumtext'), 'mediumtext')
  const multibyte = 'é'.repeat(40_000)
  assert.equal(inferType(multibyte), 'mediumtext')
  assert.equal(encodeValue(multibyte, 'mediumtext'), multibyte)
})

test('integer and decimal encoding is range-checked and non-exponential', () => {
  assert.equal(inferType(1.25), 'decimal')
  assert.equal(encodeValue(1e-7, 'decimal'), '0.0000001')
  assert.equal(normalizeDatabaseValue('1.250000', 'decimal'), '1.25')
  assert.equal(encodeValue(9_223_372_036_854_775_807n, 'integer'), '9223372036854775807')
  assert.throws(() => encodeValue(9_223_372_036_854_775_808n, 'integer'), /integer_range_unsupported/)
  assert.throws(() => encodeValue(1e-40, 'decimal'), /decimal_range_unsupported/)
})

test('nested Firestore values serialize deterministically and losslessly', () => {
  const timestamp = { seconds: 1720000000, nanoseconds: 123000000, toDate() { return new Date(this.seconds * 1000) } }
  const value = { z: null, a: [timestamp, Buffer.from('abc'), { path: 'users/u1' }, { latitude: 14.5, longitude: 121.0 }], n: 12n }
  const json = stableJson(value)
  assert.match(json, /__firestore_type/)
  assert.equal(json, stableJson(value))
  assert.equal(valuesEqual(value, json, 'json'), true)
  assert.equal(valuesEqual(Buffer.from('abc'), Buffer.from('abc'), 'blob'), true)
  assert.throws(() => stableJson({ bad: Number.NaN }), /number_non_finite/)
  assert.throws(() => stableJson({ bad: 9_007_199_254_740_992 }), /integer_precision_unsafe/)
  assert.equal(inferType(null), 'null')
  assert.equal(promoteType('null', 'text'), 'text')
})

test('queue insertion deduplicates a revision and rejects a fingerprint conflict', async () => {
  let insertCalls = 0
  const pool = { async execute(sql) {
    if (sql.startsWith('INSERT IGNORE')) return [{ affectedRows: insertCalls++ === 0 ? 1 : 0 }]
    return [[{ payload_fingerprint: 'same' }]]
  } }
  const item = { collection: 'project_bed_task', documentId: 'id', revision: '1:000000001', fingerprint: 'same' }
  assert.equal(await enqueueRevision(pool, item), 'inserted')
  assert.equal(await enqueueRevision(pool, item), 'duplicate')
  await assert.rejects(() => enqueueRevision(pool, { ...item, fingerprint: 'different' }), /queue_revision_fingerprint_conflict/)
})

test('claim preserves prior ACK_PENDING state and enforces affected-row ownership', async () => {
  const updates = []
  let selected = false
  const connection = {
    async beginTransaction() {}, async commit() {}, async rollback() {}, release() {},
    async execute(sql, parameters) {
      if (sql.startsWith('SELECT x_id')) { if (selected) return [[]]; selected = true; return [[{ x_id: 7, collection_name: 'project_bed_task', document_id: 'id', source_revision: '1:000000001', payload_fingerprint: 'fp', attempt_count: 2, prior_state: 'ACK_PENDING' }]] }
      updates.push({ sql, parameters }); return [{ affectedRows: 1 }]
    },
  }
  const rows = await claimReady({ async getConnection() { return connection } }, 'worker', 90, 1)
  assert.equal(rows[0].prior_state, 'ACK_PENDING')
  assert.equal(rows[0].attempt_count, 3)
  assert.equal(updates[0].parameters[0], 'ACK_PENDING')
  selected = false
  connection.execute = async (sql) => { if (sql.startsWith('SELECT x_id')) { if (selected) return [[]]; selected = true; return [[{ ...rows[0], attempt_count: 3 }]] } return [{ affectedRows: 0 }] }
  await assert.rejects(() => claimReady({ async getConnection() { return connection } }, 'worker', 90, 1), /queue_claim_lost/)
})

test('queue transition and history are atomic and guarded by current lease/state', async () => {
  const calls = []
  let rolledBack = false
  const connection = {
    async beginTransaction() { calls.push('begin') }, async commit() { calls.push('commit') }, async rollback() { rolledBack = true }, release() {},
    async execute(sql) { calls.push(sql); return [{ affectedRows: sql.startsWith('UPDATE') ? 1 : 1 }] },
  }
  const pool = { async getConnection() { return connection } }
  await transitionQueue(pool, { x_id: 3 }, 'worker', 'ACK_PENDING')
  assert.equal(calls.some((entry) => String(entry).includes('attempt_history')), true)
  connection.execute = async (sql) => [{ affectedRows: sql.startsWith('UPDATE') ? 0 : 1 }]
  await assert.rejects(() => transitionQueue(pool, { x_id: 3 }, 'worker', 'ACKED'), /queue_transition_lost/)
  assert.equal(rolledBack, true)
})

test('retry jitter is bounded and deterministic; terminal failures dead-letter', () => {
  assert.equal(backoffSeconds(3, () => 0), 8)
  assert.equal(backoffSeconds(3, () => 0.999), 15)
  assert.equal(failureOutcome('temporary_network', 1, 4), 'RETRY_WAIT')
  assert.equal(failureOutcome('timestamp_invalid', 1, 4), 'DEAD_LETTER')
  assert.equal(failureOutcome('firestore_user_disabled_or_deleted', 1, 4), 'DEAD_LETTER')
  assert.equal(failureOutcome('firestore_sync_status_not_pending', 1, 4), 'DEAD_LETTER')
  assert.equal(failureOutcome('er_no_default_for_field', 1, 4), 'DEAD_LETTER')
  assert.equal(failureOutcome('temporary_network', 4, 4), 'DEAD_LETTER')
})

test('ACK_PENDING replay only acknowledges and records superseded state explicitly', async () => {
  const calls = []
  const row = { x_id: 1, collection_name: 'project_bed_task', document_id: 'id', source_revision: '1:000000001', prior_state: 'ACK_PENDING', attempt_count: 2 }
  const result = await processClaimed({ pool: {}, db: {}, row, workerKey: 'worker', config, dependencies: {
    ensureFieldRegistry: async () => { throw new Error('must_not_project') },
    repairParity: async () => { throw new Error('must_not_repair') },
    projectRevision: async () => { throw new Error('must_not_project') },
    acknowledgePending: async () => 'state_changed',
    transitionQueue: async (...args) => calls.push(args),
  } })
  assert.deepEqual(result, { path: 'ACK_ONLY', result: 'state_changed' })
  assert.equal(calls.length, 1)
  assert.equal(calls[0][3], 'SUPERSEDED')
  assert.equal(calls[0][6], 'CLAIMED')
})

test('acknowledgement failure after commit returns to leased-backoff ACK_PENDING without reprojecting', async () => {
  const transitions = []
  const data = { bed_task_key: 'id' }
  const snapshot = { id: 'id', exists: true, data: () => data, ...revision(1, 1) }
  const row = { x_id: 1, collection_name: 'project_bed_task', document_id: 'id', source_revision: '1:000000001', payload_fingerprint: payloadFingerprint(data), prior_state: 'QUEUED', attempt_count: 1 }
  const result = await processClaimed({ pool: {}, db: { collection: () => ({ doc: () => ({ get: async () => snapshot }) }) }, row, workerKey: 'worker', config, dependencies: {
    ensureFieldRegistry: async () => [{ field_name: 'bed_task_key', inferred_type: 'text' }], repairParity: async () => {}, projectRevision: async () => 'APPLY',
    acknowledgePending: async () => { throw new Error('firebase_temporarily_unavailable') }, transitionQueue: async (...args) => transitions.push(args),
  } })
  assert.equal(result.path, 'ACK_PENDING')
  assert.equal(transitions.length, 2)
  assert.equal(transitions[0][3], 'ACK_PENDING')
  assert.equal(transitions[1][3], 'ACK_PENDING')
  assert.equal(transitions[1][6], 'ACK_PENDING')
  assert.equal(transitions[1][7], true)
})

test('repeated ACK_PENDING failures dead-letter at the configured attempt bound without projection', async () => {
  const transitions = []
  const row = { x_id: 1, collection_name: 'project_bed_task', document_id: 'id', source_revision: '1:000000001', prior_state: 'ACK_PENDING', attempt_count: 4 }
  const result = await processClaimed({ pool: {}, db: {}, row, workerKey: 'worker', config, dependencies: {
    ensureFieldRegistry: async () => { throw new Error('must_not_project') }, projectRevision: async () => { throw new Error('must_not_project') }, repairParity: async () => { throw new Error('must_not_project') },
    acknowledgePending: async () => { throw new Error('firebase_temporarily_unavailable') }, transitionQueue: async (...args) => transitions.push(args),
  } })
  assert.equal(result.path, 'DEAD_LETTER')
  assert.equal(transitions[0][3], 'DEAD_LETTER')
  assert.equal(transitions[0][6], 'CLAIMED')
})

test('projection fencing rejects stale revisions and treats exact replay as idempotent', () => {
  const committed = { source_revision: '10:000000100', payload_fingerprint: 'fp' }
  assert.equal(projectionDecision(committed, '10:000000099', 'old', compareRevisions), 'SUPERSEDED')
  assert.equal(projectionDecision(committed, '10:000000100', 'fp', compareRevisions), 'IDEMPOTENT')
  assert.equal(projectionDecision(committed, '10:000000101', 'new', compareRevisions), 'APPLY')
  assert.equal(projectionDecision(committed, '10:000000100', 'other', compareRevisions), 'REVISION_CONFLICT')
})

test('stale projectRevision cannot write a target row', async () => {
  let writes = 0; let rolledBack = 0
  const connection = {
    async query() {}, async beginTransaction() {}, async rollback() { rolledBack++ }, release() {},
    async execute(sql) {
      if (sql.startsWith('SELECT source_revision') && sql.includes('FOR UPDATE')) return [[{ source_revision: '2:000000000', payload_fingerprint: 'new' }]]
      writes++; return [{ affectedRows: 1 }]
    },
  }
  const result = await projectRevision({ pool: { async getConnection() { return connection } }, row: { collection_name: 'project_bed_task', document_id: 'id', source_revision: '1:999999999', payload_fingerprint: 'old' }, contract: collectionContract('project_bed_task'), document: { bed_task_key: 'id' }, fields: [{ field_name: 'bed_task_key', inferred_type: 'text' }] })
  assert.equal(result, 'SUPERSEDED')
  assert.equal(writes, 0)
  assert.equal(rolledBack, 1)
})

test('projectRevision commits only after exact all-field and fence read-back', async () => {
  const timestamp = '2026-08-26T12:34:56.123456Z'
  const document = { bed_task_key: 'id', label: 'é'.repeat(10), active: true, count: 12n, ratio: 1.25, at: timestamp, nested: [{ path: 'users/u1' }], bytes: Buffer.from('abc'), optional: null }
  const fields = [
    ['bed_task_key', 'text'], ['label', 'text'], ['active', 'boolean'], ['count', 'integer'], ['ratio', 'decimal'], ['at', 'timestamp'], ['nested', 'json'], ['bytes', 'blob'], ['optional', 'text'],
  ].map(([field_name, inferred_type]) => ({ field_name, inferred_type }))
  const row = { collection_name: 'project_bed_task', document_id: 'id', source_revision: '8:000000123', payload_fingerprint: payloadFingerprint(document) }
  const encoded = Object.fromEntries(fields.map((field) => [field.field_name, encodeValue(document[field.field_name], field.inferred_type)]))
  const calls = []; let committed = false; let rolledBack = false
  const connection = {
    release() {}, async beginTransaction() {}, async commit() { committed = true }, async rollback() { rolledBack = true }, async query(sql) { calls.push(sql) },
    async execute(sql) {
      calls.push(sql)
      if (sql.includes('FOR UPDATE')) return [[]]
      if (sql.startsWith('INSERT INTO `project_bed_task`')) return [{ affectedRows: 1 }]
      if (sql.startsWith('INSERT INTO firebase_mysql_sync_projection_state')) return [{ affectedRows: 1 }]
      if (sql.startsWith('SELECT `bed_task_key`')) return [[encoded]]
      if (sql.startsWith('SELECT source_revision')) return [[{ source_revision: row.source_revision, payload_fingerprint: row.payload_fingerprint }]]
      throw new Error(`unexpected_projection_sql:${sql}`)
    },
  }
  assert.equal(await projectRevision({ pool: { async getConnection() { return connection } }, row, contract: collectionContract('project_bed_task'), document, fields }), 'APPLY')
  assert.equal(committed, true)
  assert.equal(rolledBack, false)
  assert.equal(calls.some((sql) => String(sql).startsWith('INSERT INTO firebase_mysql_sync_projection_state')), true)
  assert.throws(() => projectionSql(collectionContract('project_bed_task'), ['bed_task_key', 'bad-field']), /identifier_invalid/)
})

test('ACK-only replay finalizes only MySQL synchronization metadata and exact read-back', async () => {
  const timestamp = { seconds: 1_777_000_000, nanoseconds: 123456000, toDate() { return new Date(this.seconds * 1000) } }
  const acknowledgement = { outcome: 'already_synced', mysqlSyncStatus: 'SYNCED', mysqlSyncedAt: timestamp }
  const row = { collection_name: 'project_messenger_chat', document_id: 'chat1', source_revision: '9:000000001', payload_fingerprint: 'f'.repeat(64) }
  const originalUpdatedAt = '2026-08-26 11:00:00.000000'
  const calls = []; let committed = false
  const connection = { release() {}, async beginTransaction() {}, async commit() { committed = true }, async rollback() {}, async query(sql) { calls.push(sql) }, async execute(sql, parameters) {
    calls.push(sql)
    if (sql.startsWith('SELECT `updated_at`')) return [[{ updated_at: originalUpdatedAt }]]
    if (sql.includes('FOR UPDATE')) return [[{ source_revision: row.source_revision, payload_fingerprint: row.payload_fingerprint }]]
    if (sql.startsWith('UPDATE `project_messenger_chat`')) { assert.deepEqual(parameters, ['SYNCED', encodeValue(timestamp, 'timestamp'), 'chat1']); return [{ affectedRows: 1 }] }
    if (sql.startsWith('SELECT `mysql_sync_status`')) return [[{ mysql_sync_status: 'SYNCED', mysql_synced_at: encodeValue(timestamp, 'timestamp'), updated_at: originalUpdatedAt }]]
    throw new Error(`unexpected_ack_finalize_sql:${sql}`)
  } }
  assert.equal(await finalizeSyncMetadata({ pool: { async getConnection() { return connection } }, row, contract: collectionContract('project_messenger_chat'), acknowledgement }), 'FINALIZED')
  assert.equal(committed, true)
  const update = calls.find((sql) => String(sql).startsWith('UPDATE `project_messenger_chat`'))
  assert.equal(update.includes('message_text'), false)
  assert.match(update, /mysql_sync_status/)
  assert.match(update, /mysql_synced_at/)
  assert.equal(update.includes('updated_at = updated_at'), false)
})

test('conditional Firebase acknowledgement returns committed server metadata for MySQL parity', async () => {
  const syncedAt = { seconds: 1_777_000_000, nanoseconds: 123456000, toDate() { return new Date(this.seconds * 1000) } }
  let updated = false
  const current = { exists: true, updateTime: { seconds: 9, nanoseconds: 1 }, get(field) { return field === 'mysql_sync_status' ? 'PENDING' : null } }
  const acknowledged = { exists: true, get(field) { return field === 'mysql_sync_status' ? 'SYNCED' : field === 'mysql_synced_at' ? syncedAt : null } }
  const reference = { async get() { return acknowledged } }
  const db = { collection() { return { doc() { return reference } } }, async runTransaction(callback) { return callback({ async get() { return current }, update() { updated = true } }) } }
  const result = await acknowledgePending(db, 'project_messenger_chat', 'chat1', '9:000000001')
  assert.equal(updated, true)
  assert.equal(result.outcome, 'acknowledged')
  assert.equal(result.mysqlSyncStatus, 'SYNCED')
  assert.equal(result.mysqlSyncedAt, syncedAt)
})

test('crash-safe ACK_PENDING replay recognizes prior Firebase ACK and does not reproject business fields', async () => {
  const transitions = []; let finalized = 0
  const row = { x_id: 9, collection_name: 'project_messenger_chat', document_id: 'chat1', source_revision: '9:000000001', payload_fingerprint: 'f'.repeat(64), prior_state: 'ACK_PENDING', attempt_count: 2 }
  const timestamp = { seconds: 1_777_000_000, nanoseconds: 0, toDate() { return new Date(this.seconds * 1000) } }
  const result = await processClaimed({ pool: {}, db: {}, row, workerKey: 'w', config, dependencies: {
    ensureFieldRegistry: async () => { throw new Error('must_not_reproject') }, repairParity: async () => { throw new Error('must_not_reproject') }, projectRevision: async () => { throw new Error('must_not_reproject') },
    acknowledgePending: async () => ({ outcome: 'already_synced', mysqlSyncStatus: 'SYNCED', mysqlSyncedAt: timestamp }),
    finalizeSyncMetadata: async () => { finalized++; return 'FINALIZED' }, transitionQueue: async (...args) => transitions.push(args),
  } })
  assert.deepEqual(result, { path: 'ACK_ONLY', result: 'already_synced' })
  assert.equal(finalized, 1)
  assert.equal(transitions[0][3], 'ACKED')
})

test('paginated startup backlog processes more than one page', async () => {
  const makeDoc = (id, nanos) => ({ id, data: () => ({ chat_key: id }), ...revision(10, nanos) })
  const pages = [[makeDoc('a', 1), makeDoc('b', 2)], [makeDoc('c', 3)]]
  let page = 0; const inserted = []
  const db = { collection() { return {
    where() { return this }, orderBy() { return this }, limit() { return this }, startAfter() { return this },
    async get() { const docs = pages[page++] || []; return { docs, size: docs.length } },
  } } }
  const pool = { async execute(sql, parameters) { if (sql.startsWith('INSERT IGNORE')) { inserted.push(parameters); return [{ affectedRows: 1 }] } throw new Error('unexpected_sql') } }
  assert.equal(await scanPendingPages({ db, pool, collection: 'project_messenger_chat', config: { ...config, scanPageSize: 2 } }), 3)
  assert.deepEqual(inserted.map((row) => row[1]), ['a', 'b', 'c'])
})

test('a listener setup failure does not skip remaining allowlisted collections', () => {
  const attempted = []; const unsubscribed = []
  const db = { collection(collection) { attempted.push(collection); return { where() { return this }, onSnapshot() { if (collection === 'project_bed_task_log') throw new Error('listener setup failed with detail'); return () => unsubscribed.push(collection) } } } }
  const subscriptions = attachPendingListeners({ db, pool: {}, config, discover: async () => 'inserted' })
  assert.equal(attempted.length, Object.keys(COLLECTIONS).length)
  assert.equal(subscriptions.length, Object.keys(COLLECTIONS).length - 1)
  for (const unsubscribe of subscriptions) unsubscribe()
  assert.equal(unsubscribed.length, Object.keys(COLLECTIONS).length - 1)
})

class RegistryPool {
  constructor() { this.rows = []; this.locked = false; this.waiters = [] }
  async acquire() { if (!this.locked) { this.locked = true; return } await new Promise((resolve) => this.waiters.push(resolve)); this.locked = true }
  releaseLock() { this.locked = false; this.waiters.shift()?.() }
  async getConnection() {
    const owner = this
    return { async beginTransaction() {}, async commit() {}, async rollback() {}, release() {}, async execute(sql, parameters) {
      if (sql.includes('GET_LOCK')) { await owner.acquire(); return [[{ acquired: 1 }]] }
      if (sql.includes('RELEASE_LOCK')) { owner.releaseLock(); return [[{ released: 1 }]] }
      if (sql.startsWith('SELECT field_name')) return [owner.rows.map((row) => ({ ...row }))]
      if (sql.startsWith('UPDATE firebase_mysql_sync_field_registry SET ordinal_no')) { for (const row of owner.rows) row.ordinal_no += 10000; return [{ affectedRows: owner.rows.length }] }
      if (sql.startsWith('INSERT INTO firebase_mysql_sync_field_registry')) {
        const [collection_name, field_name, ordinal_no, inferred_type, observed_bytes] = parameters
        const existing = owner.rows.find((row) => row.field_name === field_name)
        if (existing) { existing.ordinal_no = ordinal_no; existing.inferred_type = inferred_type; existing.observed_bytes = Math.max(existing.observed_bytes, observed_bytes) }
        else owner.rows.push({ collection_name, field_name, ordinal_no, inferred_type, observed_bytes })
        return [{ affectedRows: 1 }]
      }
      throw new Error(`unexpected_registry_sql:${sql}`)
    } }
  }
}

test('canonical ordinal allocation is serialized for concurrent new fields', async () => {
  const pool = new RegistryPool()
  await Promise.all([
    ensureFieldRegistry(pool, 'project_bed_task', { bed_task_key: 'id', alpha_field: 'a' }, config),
    ensureFieldRegistry(pool, 'project_bed_task', { bed_task_key: 'id', beta_field: 'b' }, config),
  ])
  const ordinals = pool.rows.map((row) => row.ordinal_no)
  assert.equal(new Set(ordinals).size, ordinals.length)
  assert.equal(pool.rows.find((row) => row.field_name === 'alpha_field').ordinal_no < pool.rows.find((row) => row.field_name === 'beta_field').ordinal_no, true)
})

function currentBedTaskPayload() {
  return {
    tenant_key: 'tenant', project_key: 'project', bed_task_key: 'taskdoc', bed_key: 'bed', bed_source_key: 'source', source_pk_psbeds: '1', bed_no: '101',
    branch_key: 'branch', branch_name: 'Branch', building_key: 'building', building_name: 'Building', floor_key: 'floor', floor_name: 'Floor',
    nurse_station_key: 'station', nurse_station_name: 'Station', room_key: 'room', room_class_key: 'class', room_class: 'Private',
    source_bed_status_key: 'occupied', source_bed_status: 'Occupied', task_key: 'task', task_code: 'CLEAN', task_title: 'Clean', task_type: 'PRIMARY',
    task_color_hex: '#00000000', task_sort_order: 1, task_group_keys: ['group'], task_status: 'PENDING', current_task_stage_key: 'stage',
    current_stage_label: 'NEW', current_stage_color_hex: '#00000000', task_stage_key: 'stage', stage_label: 'NEW', stage_color_hex: '#00000000',
    task_stage_response_key: '', response_label: '', response_description: '', response_color_hex: '#00000000', bed_status_at_request: 'Occupied',
    bed_class: 'Private', bed_treatment_key: 'treatment', bed_treatment_name: 'Standard', bed_source_option_key: 'admission', bed_source_option_name: 'Admission',
    remarks: 'Ready', requester_user_key: 'user', requester_fullname: 'Portal User', firebase_collection: 'project_bed_task', mysql_sync_status: 'PENDING',
    mysql_created_at: '2026-08-26T12:00:00.000Z', mysql_updated_at: '2026-08-26T12:00:00.000Z', mysql_synced_at: null, mysql_deleted_at: null,
    created_at: '2026-08-26T12:00:00.000Z', updated_at: '2026-08-26T12:00:00.000Z',
  }
}

test('first-run task and log registry bootstrap covers the complete current Portal payload without consuming dynamic budget', async () => {
  const task = currentBedTaskPayload(); const taskPool = new RegistryPool()
  const taskFields = await ensureFieldRegistry(taskPool, 'project_bed_task', task, config)
  assert.deepEqual(Object.keys(task).filter((field) => !taskFields.some((registered) => registered.field_name === field)), [])
  const log = { ...task, bed_task_log_key: 'logdoc', firebase_collection: 'project_bed_task_log', event_type: 'CREATED', status_from: '', status_to: 'PENDING', actor_user_key: 'user', actor_fullname: 'Portal User' }
  const logPool = new RegistryPool(); const logFields = await ensureFieldRegistry(logPool, 'project_bed_task_log', log, config)
  assert.deepEqual(Object.keys(log).filter((field) => !logFields.some((registered) => registered.field_name === field)), [])
  assert.equal(taskFields.length, Object.keys(task).length)
  assert.equal(logFields.length, Object.keys(log).length)
})

test('field-limit preflight rejects without partial registry persistence', async () => {
  const pool = new RegistryPool()
  await assert.rejects(() => ensureFieldRegistry(pool, 'project_bed_task', { bed_task_key: 'id', extra: 'x' }, { ...config, maxFieldsPerCollection: 1 }), /firestore_field_limit_exceeded/)
  assert.equal(pool.rows.length, 0)
})

test('repair planning detects ordering independently from SQL type changes', () => {
  const fields = [{ field_name: 'bed_task_key', inferred_type: 'text', ordinal_no: 1 }, { field_name: 'tenant_key', inferred_type: 'text', ordinal_no: 2 }]
  const schema = { columns: [
    { column_name: 'tenant_key', column_type: 'text', ordinal_position: 1 },
    { column_name: 'bed_task_key', column_type: 'varchar(255)', ordinal_position: 2 },
    { column_name: 'legacy_flag', column_type: 'int', ordinal_position: 3 },
  ] }
  const plan = repairPlan(schema, fields, collectionContract('project_bed_task'))
  const fieldPlan = plan.filter((change) => !change.drop)
  assert.equal(fieldPlan.every((change) => change.reorder), true)
  assert.equal(fieldPlan.every((change) => !change.widen), true)
  assert.equal(plan.some((change) => change.drop && change.column.column_name === 'legacy_flag'), true)
})

test('existing Messenger ENUM, VARCHAR, integer, unsigned, and TIMESTAMP columns remain when incoming values fit', () => {
  const cases = [
    [{ column_type: "enum('group','direct')", collation_name: 'utf8mb4_unicode_ci' }, { inferred_type: 'text', observed_bytes: 6 }, 'group'],
    [{ column_type: 'varchar(160)', collation_name: 'utf8mb4_unicode_ci' }, { inferred_type: 'text', observed_bytes: 12 }, 'Portal User'],
    [{ column_type: 'int', collation_name: null }, { inferred_type: 'integer', observed_bytes: 0 }, 12n],
    [{ column_type: 'bigint unsigned', collation_name: null }, { inferred_type: 'integer', observed_bytes: 0 }, 4096n],
    [{ column_type: 'timestamp', collation_name: null }, { inferred_type: 'timestamp', observed_bytes: 0 }, '2026-08-26T12:00:00Z'],
  ]
  for (const [column, field, value] of cases) assert.deepEqual(columnStorageDecision(column, field, value), { compatible: true })
})

test('representative existing Messenger schema produces no type repair for fitting values', () => {
  const contract = collectionContract('project_messenger_chat_attachment')
  const fields = [
    { field_name: 'attachment_key', inferred_type: 'text', observed_bytes: 20, ordinal_no: 1 },
    { field_name: 'image_byte_size', inferred_type: 'integer', observed_bytes: 0, ordinal_no: 2 },
    { field_name: 'sort_order', inferred_type: 'integer', observed_bytes: 0, ordinal_no: 3 },
    { field_name: 'attachment_status', inferred_type: 'text', observed_bytes: 6, ordinal_no: 4 },
    { field_name: 'mysql_sync_status', inferred_type: 'text', observed_bytes: 7, ordinal_no: 5 },
    { field_name: 'created_at', inferred_type: 'timestamp', observed_bytes: 0, ordinal_no: 6 },
    { field_name: 'updated_at', inferred_type: 'timestamp', observed_bytes: 0, ordinal_no: 7 },
  ]
  const columns = [
    { column_name: 'attachment_key', column_type: 'varchar(40)', collation_name: 'utf8mb4_unicode_ci', ordinal_position: 1 },
    { column_name: 'image_byte_size', column_type: 'bigint unsigned', collation_name: null, ordinal_position: 2 },
    { column_name: 'sort_order', column_type: 'int', collation_name: null, ordinal_position: 3 },
    { column_name: 'attachment_status', column_type: "enum('ACTIVE','REMOVED')", collation_name: 'utf8mb4_unicode_ci', ordinal_position: 4 },
    { column_name: 'mysql_sync_status', column_type: "enum('PENDING','SYNCED','FAILED')", collation_name: 'utf8mb4_unicode_ci', ordinal_position: 5 },
    { column_name: 'created_at', column_type: 'timestamp', collation_name: null, ordinal_position: 6 },
    { column_name: 'updated_at', column_type: 'timestamp', collation_name: null, ordinal_position: 7 },
  ]
  const document = { attachment_key: 'abcdefghijklmnopqrst', image_byte_size: 1024n, sort_order: 1n, attachment_status: 'ACTIVE', mysql_sync_status: 'PENDING', created_at: '2026-08-26T12:00:00Z', updated_at: '2026-08-26T12:00:00Z' }
  assert.deepEqual(repairPlan({ columns }, fields, contract, document), [])
  for (const field of fields) assert.equal(valuesEqual(document[field.field_name], encodeValue(document[field.field_name], field.inferred_type), field.inferred_type), true)
})

test('Messenger constrained columns widen only when required and never narrow existing ranges', () => {
  assert.equal(columnStorageDecision({ column_type: "enum('ACTIVE','REMOVED')", collation_name: 'utf8mb4_unicode_ci' }, { inferred_type: 'text', observed_bytes: 8 }, 'ARCHIVED').repairType, 'TEXT')
  assert.equal(columnStorageDecision({ column_type: 'varchar(40)', collation_name: 'utf8mb4_unicode_ci' }, { inferred_type: 'text', observed_bytes: 200 }, 'x'.repeat(80)).repairType, 'TEXT')
  assert.equal(columnStorageDecision({ column_type: 'int', collation_name: null }, { inferred_type: 'integer', observed_bytes: 0 }, 3_000_000_000n).repairType, 'BIGINT')
  assert.deepEqual(columnStorageDecision({ column_type: 'bigint unsigned', collation_name: null }, { inferred_type: 'integer', observed_bytes: 0 }, 18n), { compatible: true })
  assert.equal(columnStorageDecision({ column_type: 'bigint unsigned', collation_name: null }, { inferred_type: 'integer', observed_bytes: 0 }, -1n).repairType, 'DECIMAL(20,0)')
  assert.equal(columnStorageDecision({ column_type: 'timestamp', collation_name: null }, { inferred_type: 'timestamp', observed_bytes: 0 }, '2026-08-26T12:00:00.123456Z').repairType, 'DATETIME(6)')
})

test('real combined DEFAULT_GENERATED ON UPDATE EXTRA is reconstructed without weakening timestamp semantics', () => {
  const column = { column_type: 'timestamp(6)', is_nullable: 'NO', column_default: 'CURRENT_TIMESTAMP(6)', extra: 'DEFAULT_GENERATED on update CURRENT_TIMESTAMP(6)', collation_name: null }
  const timestampDefinition = existingDefinition(column, null)
  const datetimeDefinition = existingDefinition(column, 'DATETIME(6)')
  assert.equal(timestampDefinition, 'TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)')
  assert.equal(datetimeDefinition, 'DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)')
  assert.equal(timestampDefinition.includes('DEFAULT_GENERATED'), false)
  assert.equal(datetimeDefinition.includes('DEFAULT_GENERATED'), false)
  const field = { field_name: 'updated_at', inferred_type: 'timestamp', ordinal_no: 1 }
  const sql = alterStatement(collectionContract('project_messenger_chat'), { field, column, desired: 'DATETIME(6)', widen: true, reorder: false }, [field])
  assert.match(sql, /ALTER TABLE `project_messenger_chat` MODIFY COLUMN `updated_at` DATETIME\(6\) NOT NULL DEFAULT CURRENT_TIMESTAMP\(6\) ON UPDATE CURRENT_TIMESTAMP\(6\) FIRST/)
  assert.equal(sql.includes('DEFAULT_GENERATED'), false)
})

test('MariaDB quoted defaults and timestamp expressions generate valid ALTER statements', () => {
  const contract = collectionContract('project_messenger_chat')
  const chatKey = { field_name: 'chat_key', inferred_type: 'text', ordinal_no: 1 }
  const conversationType = { field_name: 'conversation_type', inferred_type: 'text', ordinal_no: 2 }
  const enumColumn = { column_type: "enum('group','direct')", is_nullable: 'NO', column_default: "'group'", extra: '', collation_name: 'utf8mb4_unicode_ci' }
  const enumSql = alterStatement(contract, { field: conversationType, column: enumColumn, desired: 'TEXT', widen: false, reorder: true }, [chatKey, conversationType])
  assert.equal(enumSql, "ALTER TABLE `project_messenger_chat` MODIFY COLUMN `conversation_type` ENUM('group','direct') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'group' AFTER `chat_key`")
  assert.equal(enumSql.includes("DEFAULT '''group'''"), false)

  const updatedAt = { field_name: 'updated_at', inferred_type: 'timestamp', ordinal_no: 1 }
  const timestampColumn = { column_type: 'timestamp(6)', is_nullable: 'NO', column_default: 'current_timestamp()', extra: 'DEFAULT_GENERATED on update CURRENT_TIMESTAMP()', collation_name: null }
  const timestampSql = alterStatement(contract, { field: updatedAt, column: timestampColumn, desired: 'DATETIME(6)', widen: true, reorder: false }, [updatedAt])
  assert.equal(timestampSql, 'ALTER TABLE `project_messenger_chat` MODIFY COLUMN `updated_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP() FIRST')
  assert.equal(timestampSql.includes("DEFAULT 'CURRENT_TIMESTAMP()'"), false)
  assert.equal(timestampSql.includes('DEFAULT_GENERATED'), false)
  for (const expression of ['CURRENT_TIMESTAMP', 'current_timestamp()', ...Array.from({ length: 7 }, (_, precision) => `current_timestamp(${precision})`)]) {
    const column = { ...timestampColumn, column_default: expression, extra: '' }
    const sql = alterStatement(contract, { field: updatedAt, column, desired: 'DATETIME(6)', widen: true, reorder: false }, [updatedAt])
    const suffix = expression.includes('(') ? expression.slice(expression.indexOf('(')) : ''
    assert.match(sql, new RegExp(` DEFAULT CURRENT_TIMESTAMP${suffix.replace(/[()]/g, '\\$&')} FIRST$`))
    assert.equal(sql.includes("DEFAULT 'CURRENT_TIMESTAMP"), false)
  }

  const title = { field_name: 'conversation_title', inferred_type: 'text', ordinal_no: 1 }
  const stringColumn = { column_type: 'varchar(160)', is_nullable: 'NO', column_default: "'owner''s \\\\ward'", extra: '', collation_name: 'utf8mb4_unicode_ci' }
  const stringSql = alterStatement(contract, { field: title, column: stringColumn, desired: 'TEXT', widen: false, reorder: true }, [title])
  assert.equal(stringSql, "ALTER TABLE `project_messenger_chat` MODIFY COLUMN `conversation_title` VARCHAR(160) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'owner''s \\\\ward' FIRST")
})

test('MariaDB metadata NULL remains distinct from a quoted NULL literal in generated ALTER statements', () => {
  const contract = collectionContract('project_messenger_chat')
  const nullableCases = [
    ['nullable_varchar', 'varchar(160)', 'text', ' COLLATE utf8mb4_unicode_ci'],
    ['nullable_text', 'text', 'text', ' COLLATE utf8mb4_unicode_ci'],
    ['removed_at', 'timestamp', 'timestamp', ''],
    ['mysql_deleted_at', 'datetime(6)', 'timestamp', ''],
  ]
  for (const [fieldName, columnType, inferredType, collation] of nullableCases) {
    const field = { field_name: fieldName, inferred_type: inferredType, ordinal_no: 1 }
    const column = { column_type: columnType, is_nullable: 'YES', column_default: 'NULL', extra: '', collation_name: collation ? 'utf8mb4_unicode_ci' : null }
    const sql = alterStatement(contract, { field, column, desired: sqlType(inferredType), widen: false, reorder: true }, [field])
    assert.equal(sql, `ALTER TABLE \`project_messenger_chat\` MODIFY COLUMN \`${fieldName}\` ${columnType.toUpperCase()}${collation} NULL DEFAULT NULL FIRST`)
    assert.equal(sql.includes("DEFAULT 'NULL'"), false)
  }
  const caseInsensitiveField = { field_name: 'removed_at', inferred_type: 'timestamp', ordinal_no: 1 }
  const caseInsensitiveColumn = { column_type: 'timestamp', is_nullable: 'YES', column_default: 'nUlL', extra: '', collation_name: null }
  assert.match(alterStatement(contract, { field: caseInsensitiveField, column: caseInsensitiveColumn, desired: 'DATETIME(6)', widen: false, reorder: true }, [caseInsensitiveField]), / NULL DEFAULT NULL FIRST$/)

  const literalField = { field_name: 'conversation_title', inferred_type: 'text', ordinal_no: 1 }
  const literalColumn = { column_type: 'varchar(160)', is_nullable: 'NO', column_default: "'NULL'", extra: '', collation_name: 'utf8mb4_unicode_ci' }
  const literalSql = alterStatement(contract, { field: literalField, column: literalColumn, desired: 'TEXT', widen: false, reorder: true }, [literalField])
  assert.equal(literalSql, "ALTER TABLE `project_messenger_chat` MODIFY COLUMN `conversation_title` VARCHAR(160) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'NULL' FIRST")

  const unsafeColumn = { ...literalColumn, column_default: 'NULL' }
  assert.throws(() => alterStatement(contract, { field: literalField, column: unsafeColumn, desired: 'TEXT', widen: false, reorder: true }, [literalField]), /existing_column_default_unsafe/)
})

test('legacy columns interleaved with mirrored fields are moved after canonical ordinals', () => {
  const fields = [{ field_name: 'bed_task_key', inferred_type: 'text', ordinal_no: 1 }, { field_name: 'tenant_key', inferred_type: 'text', ordinal_no: 2 }]
  const schema = { columns: [
    { column_name: 'bed_task_key', column_type: 'varchar(255)', ordinal_position: 1 },
    { column_name: 'legacy_flag', column_type: 'int', ordinal_position: 2 },
    { column_name: 'tenant_key', column_type: 'text', ordinal_position: 3 },
  ] }
  const plan = repairPlan(schema, fields, collectionContract('project_bed_task'))
  assert.deepEqual(plan.map((change) => change.field.field_name), ['tenant_key', 'legacy_flag'])
  assert.equal(plan[0].reorder, true)
  assert.equal(plan[1].drop, true)
})

test('repair path acquires/releases its dedicated table lock even with no DDL', async () => {
  const calls = []
  const fields = [{ field_name: 'bed_task_key', ordinal_no: 1, inferred_type: 'text', observed_bytes: 2 }]
  const connection = { release() { calls.push('connection_release') }, async query() { throw new Error('ddl_not_expected') }, async execute(sql) {
    calls.push(sql)
    if (sql.includes('GET_LOCK')) return [[{ acquired: 1 }]]
    if (sql.includes('RELEASE_LOCK')) return [[{ released: 1 }]]
    if (sql.startsWith('SELECT field_name')) return [fields]
    if (sql.includes('information_schema.columns')) return [[{ column_name: 'bed_task_key', column_type: 'varchar(255)', is_nullable: 'NO', column_default: null, extra: '', collation_name: 'utf8mb4_unicode_ci', ordinal_position: 1 }]]
    if (sql.includes('information_schema.statistics')) return [[]]
    throw new Error(`unexpected_repair_sql:${sql}`)
  } }
  assert.deepEqual(await repairParity({ async getConnection() { return connection } }, 'project_bed_task', config), { repaired: false, fields })
  assert.equal(calls.some((sql) => String(sql).includes('GET_LOCK')), true)
  assert.equal(calls.some((sql) => String(sql).includes('RELEASE_LOCK')), true)
  assert.equal(calls.at(-1), 'connection_release')
})

class BackupConnection {
  constructor() {
    this.table = 'project_bed_task'; this.backup = 'project_bed_task_2026_08_26_12_34'
    this.schemas = new Map([[this.table, [{ column_name: 'bed_task_key', column_type: 'varchar(255)', is_nullable: 'NO', column_default: null, extra: '', collation_name: 'utf8mb4_unicode_ci', ordinal_position: 1 }]]])
    this.counts = new Map([[this.table, 2]]); this.checksums = new Map([[this.table, '55']]); this.baseline = null; this.history = 0; this.failedRecords = 0; this.failCopy = false
  }
  async execute(sql, parameters) {
    const table = parameters?.[0]
    if (sql.includes('information_schema.tables')) return [[{ estimated_bytes: 1000 }]]
    if (sql.startsWith('SELECT source_row_count')) return [this.baseline ? [this.baseline] : []]
    if (sql.includes('information_schema.columns')) return [this.schemas.get(table) || []]
    if (sql.includes('information_schema.statistics')) return [[]]
    if (sql.startsWith('INSERT INTO firebase_mysql_sync_migration_history')) {
      if (sql.includes("'FAILED'")) { this.failedRecords++; return [{ insertId: ++this.history }] }
      this.history++
      if (!this.baseline) this.baseline = { source_row_count: parameters[4], source_checksum: parameters[6], pre_schema_fingerprint: parameters[8] }
      return [{ insertId: this.history }]
    }
    throw new Error(`unexpected_backup_execute:${sql}`)
  }
  async query(sql) {
    if (sql.startsWith('SELECT COUNT')) { const table = sql.includes(this.backup) ? this.backup : this.table; return [[{ total: this.counts.get(table) }]] }
    if (sql.startsWith('CHECKSUM TABLE')) { const table = sql.includes(this.backup) ? this.backup : this.table; return [[{ Checksum: this.checksums.get(table) }]] }
    if (sql.startsWith('CREATE TABLE')) { this.schemas.set(this.backup, this.schemas.get(this.table).map((row) => ({ ...row }))); this.counts.set(this.backup, 0); this.checksums.set(this.backup, '0'); return [{ affectedRows: 0 }] }
    if (sql.startsWith('INSERT INTO')) { if (this.failCopy) throw Object.assign(new Error('copy failed with unsafe detail'), { code: 'ER_BACKUP_COPY_FAILED' }); this.counts.set(this.backup, this.counts.get(this.table)); this.checksums.set(this.backup, this.checksums.get(this.table)); return [{ affectedRows: 2 }] }
    throw new Error(`unexpected_backup_query:${sql}`)
  }
}

test('backup naming, first creation, verified same-minute reuse, and mismatch rejection', async () => {
  const now = new Date('2026-08-26T04:34:15.000Z')
  assert.equal(backupName('project_bed_task', now, 'Asia/Manila'), 'project_bed_task_2026_08_26_12_34')
  const connection = new BackupConnection()
  const first = await createVerifiedBackup(connection, { table: connection.table, collection: connection.table, repairKind: 'schema_parity', timezone: 'Asia/Manila', maxTableBytes: 10_000, now })
  assert.equal(first.reused, false)
  const second = await createVerifiedBackup(connection, { table: connection.table, collection: connection.table, repairKind: 'schema_parity', timezone: 'Asia/Manila', maxTableBytes: 10_000, now })
  assert.equal(second.reused, true)
  connection.counts.set(connection.backup, 1)
  await assert.rejects(() => createVerifiedBackup(connection, { table: connection.table, collection: connection.table, repairKind: 'schema_parity', timezone: 'Asia/Manila', maxTableBytes: 10_000, now }), /backup_same_minute_baseline_mismatch/)
})

test('same-minute reuse rejects and audits current source row-count drift', async () => {
  const now = new Date('2026-08-26T04:34:15.000Z'); const connection = new BackupConnection()
  await createVerifiedBackup(connection, { table: connection.table, collection: connection.table, repairKind: 'schema_parity', timezone: 'Asia/Manila', maxTableBytes: 10_000, now })
  connection.counts.set(connection.table, 3)
  await assert.rejects(() => createVerifiedBackup(connection, { table: connection.table, collection: connection.table, repairKind: 'schema_parity', timezone: 'Asia/Manila', maxTableBytes: 10_000, now }), /backup_same_minute_source_drift/)
  assert.equal(connection.failedRecords, 1)
})

test('same-minute reuse rejects and audits current source schema drift', async () => {
  const now = new Date('2026-08-26T04:34:15.000Z'); const connection = new BackupConnection()
  await createVerifiedBackup(connection, { table: connection.table, collection: connection.table, repairKind: 'schema_parity', timezone: 'Asia/Manila', maxTableBytes: 10_000, now })
  connection.schemas.get(connection.table)[0].column_type = 'varchar(300)'
  await assert.rejects(() => createVerifiedBackup(connection, { table: connection.table, collection: connection.table, repairKind: 'schema_parity', timezone: 'Asia/Manila', maxTableBytes: 10_000, now }), /backup_same_minute_source_drift/)
  assert.equal(connection.failedRecords, 1)
})

test('same-minute reuse rejects and audits current source checksum drift', async () => {
  const now = new Date('2026-08-26T04:34:15.000Z'); const connection = new BackupConnection()
  await createVerifiedBackup(connection, { table: connection.table, collection: connection.table, repairKind: 'schema_parity', timezone: 'Asia/Manila', maxTableBytes: 10_000, now })
  connection.checksums.set(connection.table, '77')
  await assert.rejects(() => createVerifiedBackup(connection, { table: connection.table, collection: connection.table, repairKind: 'schema_parity', timezone: 'Asia/Manila', maxTableBytes: 10_000, now }), /backup_same_minute_source_drift/)
  assert.equal(connection.failedRecords, 1)
})

test('backup copy failure records redacted FAILED history and cannot reach DDL', async () => {
  const connection = new BackupConnection(); connection.failCopy = true
  await assert.rejects(() => createVerifiedBackup(connection, { table: connection.table, collection: connection.table, repairKind: 'schema_parity', timezone: 'Asia/Manila', maxTableBytes: 10_000, now: new Date('2026-08-26T04:34:15.000Z') }), /copy failed/)
  assert.equal(connection.failedRecords, 1)
})

test('bad collection failure is isolated from another row completing', async () => {
  const transitions = []
  const goodData = { bed_task_key: 'good' }
  const good = { x_id: 2, collection_name: 'project_bed_task', document_id: 'good', source_revision: '1:000000001', payload_fingerprint: payloadFingerprint(goodData), prior_state: 'QUEUED', attempt_count: 1 }
  const bad = { ...good, x_id: 1, collection_name: 'not_allowlisted', document_id: 'bad' }
  const db = { collection: () => ({ doc: () => ({ get: async () => ({ id: 'good', exists: true, data: () => goodData, ...revision(1, 1) }) }) }) }
  const dependencies = { ensureFieldRegistry: async () => [{ field_name: 'bed_task_key', inferred_type: 'text' }], repairParity: async () => {}, projectRevision: async () => 'APPLY', acknowledgePending: async () => 'acknowledged', finalizeSyncMetadata: async () => 'FINALIZED', transitionQueue: async (...args) => transitions.push(args) }
  const [badResult, goodResult] = await Promise.all([processClaimed({ pool: {}, db, row: bad, workerKey: 'w', config, dependencies }), processClaimed({ pool: {}, db, row: good, workerKey: 'w', config, dependencies })])
  assert.equal(badResult.path, 'DEAD_LETTER')
  assert.equal(goodResult.path, 'PROJECT_THEN_ACK')
  assert.deepEqual(transitions.filter((call) => call[1].x_id === 2).map((call) => call[3]), ['ACK_PENDING', 'ACKED'])
})

test('new sync source does not import prototype or protected phase paths and never slices values', () => {
  const files = [path.join(root, 'scripts/firebase-mysql-sync-master.mjs'), ...fs.readdirSync(path.join(root, 'scripts/firebase-mysql-sync')).filter((name) => name.endsWith('.mjs')).map((name) => path.join(root, 'scripts/firebase-mysql-sync', name))]
  const source = files.map((file) => fs.readFileSync(file, 'utf8')).join('\n')
  for (const forbidden of ['test.js', 'phases/', 'config.local.php', '.slice(']) assert.equal(source.includes(forbidden), false, forbidden)
})
