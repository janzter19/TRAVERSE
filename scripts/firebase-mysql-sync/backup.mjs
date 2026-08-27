import crypto from 'node:crypto'
import { quoteIdentifier } from './registry.mjs'
import { safeErrorCode } from './telemetry.mjs'

export function backupName(table, date = new Date(), timezone = 'Asia/Manila') {
  const parts = new Intl.DateTimeFormat('en-US', { timeZone: timezone, year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hourCycle: 'h23' }).formatToParts(date)
  const get = (type) => parts.find((part) => part.type === type)?.value
  return `${table}_${get('year')}_${get('month')}_${get('day')}_${get('hour')}_${get('minute')}`
}

export async function actualSchema(connection, table) {
  const [columns] = await connection.execute(`SELECT column_name, column_type, is_nullable, column_default, extra, collation_name, ordinal_position FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? ORDER BY ordinal_position`, [table])
  const [indexes] = await connection.execute(`SELECT index_name, non_unique, seq_in_index, column_name, sub_part FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? ORDER BY index_name, seq_in_index`, [table])
  return { columns, indexes }
}
export function schemaFingerprint(schema) { return crypto.createHash('sha256').update(JSON.stringify(schema)).digest('hex') }
async function rowCount(connection, table) { const [[row]] = await connection.query(`SELECT COUNT(*) AS total FROM ${quoteIdentifier(table)}`); return Number(row.total) }
async function checksum(connection, table) { const [rows] = await connection.query(`CHECKSUM TABLE ${quoteIdentifier(table)}`); const value = rows?.[0]?.Checksum; return value === null || value === undefined ? null : String(value) }

export async function createVerifiedBackup(connection, { table, collection, repairKind, timezone, maxTableBytes, now = new Date() }) {
  const backupTable = backupName(table, now, timezone)
  const [capacity] = await connection.execute(`SELECT COALESCE(data_length, 0) + COALESCE(index_length, 0) AS estimated_bytes FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?`, [table])
  if (capacity.length !== 1 || Number(capacity[0].estimated_bytes) > Number(maxTableBytes)) throw new Error('backup_configured_capacity_guard_exceeded')
  const [baselineRows] = await connection.execute(`SELECT source_row_count, pre_schema_fingerprint, source_checksum FROM firebase_mysql_sync_migration_history WHERE table_name = ? AND backup_table_name = ? AND status IN ('BACKED_UP','COMPLETED') ORDER BY x_id ASC LIMIT 1`, [table, backupTable])
  let sourceRowCount = 0; let backupRowCount = 0; let preFingerprint = crypto.createHash('sha256').update('unavailable').digest('hex'); let sourceChecksum = null; let backupChecksum = null; let reused = false
  try {
    if (baselineRows.length > 0) {
      reused = true
      const baseline = baselineRows[0]
      const baselineRowCount = Number(baseline.source_row_count); const baselineFingerprint = baseline.pre_schema_fingerprint; const baselineChecksum = baseline.source_checksum
      const sourceSchema = await actualSchema(connection, table)
      sourceRowCount = await rowCount(connection, table)
      sourceChecksum = await checksum(connection, table)
      preFingerprint = schemaFingerprint(sourceSchema)
      const backupSchema = await actualSchema(connection, backupTable)
      backupRowCount = await rowCount(connection, backupTable)
      backupChecksum = await checksum(connection, backupTable)
      if (preFingerprint !== baselineFingerprint || sourceRowCount !== baselineRowCount || (baselineChecksum !== null && sourceChecksum !== String(baselineChecksum))) throw new Error('backup_same_minute_source_drift')
      if (schemaFingerprint(backupSchema) !== baselineFingerprint || backupRowCount !== baselineRowCount || (baselineChecksum !== null && backupChecksum !== String(baselineChecksum))) throw new Error('backup_same_minute_baseline_mismatch')
    } else {
      const sourceSchema = await actualSchema(connection, table)
      if (sourceSchema.columns.length === 0) throw new Error('backup_source_table_missing')
      preFingerprint = schemaFingerprint(sourceSchema)
      sourceRowCount = await rowCount(connection, table)
      sourceChecksum = await checksum(connection, table)
      await connection.query(`CREATE TABLE ${quoteIdentifier(backupTable)} LIKE ${quoteIdentifier(table)}`)
      await connection.query(`INSERT INTO ${quoteIdentifier(backupTable)} SELECT * FROM ${quoteIdentifier(table)}`)
      const backupSchema = await actualSchema(connection, backupTable)
      backupRowCount = await rowCount(connection, backupTable)
      backupChecksum = await checksum(connection, backupTable)
      if (schemaFingerprint(backupSchema) !== preFingerprint || backupRowCount !== sourceRowCount || (sourceChecksum !== null && backupChecksum !== sourceChecksum)) throw new Error('backup_verification_failed')
    }
  } catch (error) {
    await connection.execute(`INSERT INTO firebase_mysql_sync_migration_history (collection_name, table_name, backup_table_name, repair_kind, status, source_row_count, backup_row_count, source_checksum, backup_checksum, pre_schema_fingerprint, error_code, completed_at) VALUES (?, ?, ?, ?, 'FAILED', ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(6))`, [collection, table, backupTable, repairKind, sourceRowCount, backupRowCount, sourceChecksum, backupChecksum, preFingerprint, safeErrorCode(error, 'backup_failed')]).catch(() => {})
    throw error
  }
  const [history] = await connection.execute(`INSERT INTO firebase_mysql_sync_migration_history (collection_name, table_name, backup_table_name, repair_kind, status, source_row_count, backup_row_count, source_checksum, backup_checksum, pre_schema_fingerprint) VALUES (?, ?, ?, ?, 'BACKED_UP', ?, ?, ?, ?, ?)`, [collection, table, backupTable, repairKind, sourceRowCount, backupRowCount, sourceChecksum, backupChecksum, preFingerprint])
  return { historyId: history.insertId, backupTable, reused, preFingerprint, sourceRowCount, backupRowCount }
}

export async function finishMigration(connection, historyId, status, postFingerprint = null, errorCode = null) {
  if (!['COMPLETED', 'FAILED'].includes(status)) throw new Error('migration_status_invalid')
  const [result] = await connection.execute(`UPDATE firebase_mysql_sync_migration_history SET status = ?, post_schema_fingerprint = ?, error_code = ?, completed_at = CURRENT_TIMESTAMP(6) WHERE x_id = ? AND status = 'BACKED_UP'`, [status, postFingerprint, errorCode, historyId])
  if (Number(result.affectedRows) !== 1) throw new Error('migration_history_update_lost')
}
