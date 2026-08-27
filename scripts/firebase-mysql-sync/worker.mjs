import { acknowledgePending } from './acknowledger.mjs'
import { collectionContract, quoteIdentifier } from './registry.mjs'
import { ensureFieldRegistry, repairParity } from './schema-parity.mjs'
import { backoffSeconds, enqueueRevision, failureOutcome, projectionDecision, projectionSql, transitionQueue } from './queue-store.mjs'
import { assertDocument, compareRevisions, encodeValue, firestoreRevision, payloadFingerprint, valuesEqual } from './types.mjs'
import { safeErrorCode, telemetry } from './telemetry.mjs'

export async function discoverSnapshot(pool, collection, snapshot, config) {
  const contract = collectionContract(collection)
  const document = assertDocument(contract, snapshot, config.maxDocumentBytes)
  return enqueueRevision(pool, { collection, documentId: snapshot.id, revision: firestoreRevision(snapshot), fingerprint: payloadFingerprint(document) })
}

export async function projectRevision({ pool, row, contract, document, fields }) {
  const fieldNames = fields.map((field) => field.field_name)
  const fieldTypes = new Map(fields.map((field) => [field.field_name, field.inferred_type]))
  const values = fieldNames.map((field) => encodeValue(document[field], fieldTypes.get(field)))
  const connection = await pool.getConnection()
  try {
    await connection.query(`SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO'`)
    await connection.beginTransaction()
    const [states] = await connection.execute(`SELECT source_revision, payload_fingerprint FROM firebase_mysql_sync_projection_state WHERE collection_name = ? AND document_id = ? FOR UPDATE`, [row.collection_name, row.document_id])
    const decision = projectionDecision(states[0] || null, row.source_revision, row.payload_fingerprint, compareRevisions)
    if (decision === 'SUPERSEDED') { await connection.rollback(); return decision }
    if (decision === 'REVISION_CONFLICT') throw new Error('projection_revision_fingerprint_conflict')
    if (decision === 'APPLY') {
      await connection.execute(projectionSql(contract, fieldNames), values)
      await connection.execute(`INSERT INTO firebase_mysql_sync_projection_state (collection_name, document_id, source_revision, payload_fingerprint) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE source_revision = VALUES(source_revision), payload_fingerprint = VALUES(payload_fingerprint), projected_at = CURRENT_TIMESTAMP(6)`, [row.collection_name, row.document_id, row.source_revision, row.payload_fingerprint])
    }
    const [readBack] = await connection.execute(`SELECT ${fieldNames.map(quoteIdentifier).join(', ')} FROM ${quoteIdentifier(contract.table)} WHERE ${quoteIdentifier(contract.key)} = ? LIMIT 1`, [row.document_id])
    const [stateBack] = await connection.execute(`SELECT source_revision, payload_fingerprint FROM firebase_mysql_sync_projection_state WHERE collection_name = ? AND document_id = ? LIMIT 1`, [row.collection_name, row.document_id])
    if (readBack.length !== 1 || stateBack.length !== 1 || stateBack[0].source_revision !== row.source_revision || stateBack[0].payload_fingerprint !== row.payload_fingerprint) throw new Error('mysql_projection_state_read_back_failed')
    for (const field of fields) if (!valuesEqual(document[field.field_name], readBack[0][field.field_name], field.inferred_type)) throw new Error(`mysql_field_read_back_mismatch_${field.field_name}`)
    await connection.commit()
    return decision
  } catch (error) { await connection.rollback().catch(() => {}); throw error } finally { connection.release() }
}

export async function finalizeSyncMetadata({ pool, row, contract, acknowledgement }) {
  if (!['acknowledged', 'already_synced'].includes(acknowledgement.outcome) || acknowledgement.mysqlSyncStatus !== 'SYNCED') throw new Error('firebase_ack_metadata_invalid')
  const syncedAt = encodeValue(acknowledgement.mysqlSyncedAt, 'timestamp')
  const connection = await pool.getConnection()
  try {
    await connection.query(`SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO'`)
    await connection.beginTransaction()
    const [states] = await connection.execute(`SELECT source_revision, payload_fingerprint FROM firebase_mysql_sync_projection_state WHERE collection_name = ? AND document_id = ? FOR UPDATE`, [row.collection_name, row.document_id])
    if (states.length !== 1 || states[0].source_revision !== row.source_revision || states[0].payload_fingerprint !== row.payload_fingerprint) { await connection.rollback(); return 'SUPERSEDED' }
    const preserveColumns = contract.ackPreserveColumns || []
    for (const column of preserveColumns) if (!contract.fields.some((field) => field.name === column)) throw new Error('ack_preserve_column_not_allowlisted')
    const preserveSelect = preserveColumns.length > 0 ? preserveColumns.map(quoteIdentifier).join(', ') : quoteIdentifier(contract.key)
    const [before] = await connection.execute(`SELECT ${preserveSelect} FROM ${quoteIdentifier(contract.table)} WHERE ${quoteIdentifier(contract.key)} = ? LIMIT 1 FOR UPDATE`, [row.document_id])
    if (before.length !== 1) throw new Error('mysql_ack_target_missing')
    const preserveAssignments = preserveColumns.map((column) => `${quoteIdentifier(column)} = ${quoteIdentifier(column)}`)
    const assignments = [`${quoteIdentifier('mysql_sync_status')} = ?`, `${quoteIdentifier('mysql_synced_at')} = ?`, ...preserveAssignments]
    await connection.execute(`UPDATE ${quoteIdentifier(contract.table)} SET ${assignments.join(', ')} WHERE ${quoteIdentifier(contract.key)} = ?`, ['SYNCED', syncedAt, row.document_id])
    const [readBack] = await connection.execute(`SELECT ${quoteIdentifier('mysql_sync_status')}, ${quoteIdentifier('mysql_synced_at')}, ${preserveSelect} FROM ${quoteIdentifier(contract.table)} WHERE ${quoteIdentifier(contract.key)} = ? LIMIT 1`, [row.document_id])
    if (readBack.length !== 1 || String(readBack[0].mysql_sync_status) !== 'SYNCED' || !valuesEqual(acknowledgement.mysqlSyncedAt, readBack[0].mysql_synced_at, 'timestamp')) throw new Error('mysql_ack_metadata_read_back_failed')
    for (const column of preserveColumns) if (!valuesEqual(before[0][column], readBack[0][column], 'timestamp')) throw new Error('mysql_ack_business_timestamp_changed')
    await connection.commit()
    return 'FINALIZED'
  } catch (error) { await connection.rollback().catch(() => {}); throw error } finally { connection.release() }
}

async function finishAcknowledgement({ pool, db, row, workerKey, fromState, acknowledge = acknowledgePending, finalize = finalizeSyncMetadata, transition = transitionQueue }) {
  const acknowledgement = await acknowledge(db, row.collection_name, row.document_id, row.source_revision)
  const result = typeof acknowledgement === 'string' ? { outcome: acknowledgement } : acknowledgement
  if (['acknowledged', 'already_synced'].includes(result.outcome)) {
    const finalized = await finalize({ pool, row, contract: collectionContract(row.collection_name), acknowledgement: result })
    if (finalized === 'FINALIZED') await transition(pool, row, workerKey, 'ACKED', null, 0, fromState)
    else await transition(pool, row, workerKey, 'SUPERSEDED', 'ack_projection_superseded', 0, fromState)
  } else await transition(pool, row, workerKey, 'SUPERSEDED', `ack_${result.outcome}`, 0, fromState)
  return result.outcome
}

export async function processClaimed({ pool, db, row, workerKey, config, dependencies = {} }) {
  const discoverFields = dependencies.ensureFieldRegistry || ensureFieldRegistry
  const repair = dependencies.repairParity || repairParity
  const project = dependencies.projectRevision || projectRevision
  const acknowledge = dependencies.acknowledgePending || acknowledgePending
  const finalize = dependencies.finalizeSyncMetadata || finalizeSyncMetadata
  const transition = dependencies.transitionQueue || transitionQueue
  let acknowledgementPending = false
  try {
    if (row.prior_state === 'ACK_PENDING') {
      acknowledgementPending = true
      const ack = await finishAcknowledgement({ pool, db, row, workerKey, fromState: 'CLAIMED', acknowledge, finalize, transition })
      telemetry('ack_replayed', { collection: row.collection_name, document_id: row.document_id, result: ack })
      return { path: 'ACK_ONLY', result: ack }
    }
    const contract = collectionContract(row.collection_name)
    const snapshot = await db.collection(row.collection_name).doc(row.document_id).get()
    if (!snapshot.exists || firestoreRevision(snapshot) !== String(row.source_revision)) {
      await transition(pool, row, workerKey, 'SUPERSEDED', snapshot.exists ? 'source_revision_superseded' : 'source_revision_missing')
      return { path: 'SUPERSEDED' }
    }
    const document = assertDocument(contract, snapshot, config.maxDocumentBytes)
    if (payloadFingerprint(document) !== row.payload_fingerprint) throw new Error('queue_revision_fingerprint_conflict')
    const fields = await discoverFields(pool, row.collection_name, document, config)
    if (fields.some((field) => field.inferred_type === 'null')) throw new Error('field_type_unresolved_null')
    await repair(pool, row.collection_name, config, document)
    const projected = await project({ pool, row, contract, document, fields })
    if (projected === 'SUPERSEDED') {
      await transition(pool, row, workerKey, 'SUPERSEDED', 'projection_revision_superseded')
      return { path: 'SUPERSEDED' }
    }
    await transition(pool, row, workerKey, 'ACK_PENDING')
    acknowledgementPending = true
    const ack = await finishAcknowledgement({ pool, db, row, workerKey, fromState: 'ACK_PENDING', acknowledge, finalize, transition })
    telemetry('projection_processed', { collection: row.collection_name, document_id: row.document_id, projected, acknowledgement: ack })
    return { path: 'PROJECT_THEN_ACK', projected, acknowledgement: ack }
  } catch (error) {
    const code = safeErrorCode(error, 'projection_failed')
    if (acknowledgementPending) {
      const exhausted = Number(row.attempt_count) >= Number(config.maxAttempts)
      const state = exhausted ? 'DEAD_LETTER' : 'ACK_PENDING'
      const fromState = row.prior_state === 'ACK_PENDING' ? 'CLAIMED' : 'ACK_PENDING'
      await transition(pool, row, workerKey, state, code, exhausted ? 0 : backoffSeconds(row.attempt_count), fromState, true).catch((transitionError) => telemetry('queue_transition_failed', { collection: row.collection_name, document_id: row.document_id, code: safeErrorCode(transitionError, 'queue_transition_failed') }))
      telemetry('acknowledgement_deferred', { collection: row.collection_name, document_id: row.document_id, code, state })
      return { path: state, code }
    }
    const state = failureOutcome(code, row.attempt_count, config.maxAttempts)
    await transition(pool, row, workerKey, state, code, state === 'RETRY_WAIT' ? backoffSeconds(row.attempt_count) : 0).catch((transitionError) => telemetry('queue_transition_failed', { collection: row.collection_name, document_id: row.document_id, code: safeErrorCode(transitionError, 'queue_transition_failed') }))
    telemetry('projection_failed', { collection: row.collection_name, document_id: row.document_id, code, state })
    return { path: state, code }
  }
}
