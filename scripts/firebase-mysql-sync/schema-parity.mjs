import { collectionContract, quoteIdentifier } from './registry.mjs'
import { actualSchema, createVerifiedBackup, finishMigration, schemaFingerprint } from './backup.mjs'
import { binaryType, encodeValue, inferType, normalizeRegistryType, promoteType, stringType, utf8Bytes } from './types.mjs'
import { safeErrorCode } from './telemetry.mjs'

function observedBytes(value) { return typeof value === 'string' ? utf8Bytes(value) : Buffer.isBuffer(value) ? value.length : value?.toBase64 instanceof Function ? Buffer.from(value.toBase64(), 'base64').length : 0 }
export function sqlType(type, identity = false) {
  if (identity) return 'VARCHAR(255)'
  type = normalizeRegistryType(type)
  return { boolean: 'TINYINT(1)', integer: 'BIGINT', decimal: 'DECIMAL(65,30)', timestamp: 'DATETIME(6)', text: 'TEXT', mediumtext: 'MEDIUMTEXT', longtext: 'LONGTEXT', json: 'JSON', geopoint: 'JSON', reference: 'TEXT', blob: 'BLOB', mediumblob: 'MEDIUMBLOB', longblob: 'LONGBLOB' }[type] || (() => { throw new Error('registry_type_unsupported') })()
}
async function advisoryLock(connection, name, seconds) { const [[row]] = await connection.execute(`SELECT GET_LOCK(?, ?) AS acquired`, [name, seconds]); if (Number(row.acquired) !== 1) throw new Error('advisory_lock_unavailable') }
async function releaseLock(connection, name) { await connection.execute(`SELECT RELEASE_LOCK(?) AS released`, [name]) }

export async function ensureFieldRegistry(pool, collection, document, config) {
  const contract = collectionContract(collection)
  const connection = await pool.getConnection(); const lockName = `rbms_fm_fields_${collection}`
  try {
    await advisoryLock(connection, lockName, config.advisoryLockSeconds)
    await connection.beginTransaction()
    if (contract.dynamicFields && contract.fields.length > 0) {
      const legacyNames = contract.fields.map((field) => field.name)
      await connection.query(`DELETE FROM firebase_mysql_sync_field_registry WHERE collection_name = ? AND field_name IN (${legacyNames.map(() => '?').join(', ')})`, [collection, ...legacyNames])
    }
    const [rows] = await connection.execute(`SELECT field_name, ordinal_no, inferred_type, observed_bytes FROM firebase_mysql_sync_field_registry WHERE collection_name = ? ORDER BY ordinal_no FOR UPDATE`, [collection])
    const known = new Map(rows.map((row) => [row.field_name, { ...row, inferred_type: normalizeRegistryType(row.inferred_type) }]))
    const contractFields = contract.dynamicFields ? [] : contract.fields
    const seeds = contractFields.filter((field) => !known.has(field.name)).map((field) => ({ field_name: field.name, inferred_type: normalizeRegistryType(field.type), observed_bytes: 0 }))
    for (const seed of seeds) known.set(seed.field_name, seed)
    const dynamicNames = Object.keys(document).filter((field) => !known.has(field))
    if (known.size + dynamicNames.length > config.maxFieldsPerCollection) throw new Error('firestore_field_limit_exceeded')
    if (!contract.dynamicFields && dynamicNames.length > config.maxNewFieldsPerScan) throw new Error('firestore_new_field_rate_limit_exceeded')
    const next = []
    for (const seed of contractFields) next.push({ ...(known.get(seed.name) || { field_name: seed.name, inferred_type: normalizeRegistryType(seed.type), observed_bytes: 0 }) })
    for (const row of rows) if (!contractFields.some((seed) => seed.name === row.field_name)) next.push({ ...known.get(row.field_name) })
    for (const field of dynamicNames.sort()) next.push({ field_name: field, inferred_type: inferType(document[field]), observed_bytes: observedBytes(document[field]) })
    for (const field of Object.keys(document)) {
      const entry = next.find((item) => item.field_name === field)
      const incoming = inferType(document[field], entry.inferred_type)
      entry.inferred_type = promoteType(entry.inferred_type, incoming)
      entry.observed_bytes = Math.max(Number(entry.observed_bytes || 0), observedBytes(document[field]))
      if (entry.inferred_type === 'text') entry.inferred_type = stringType(entry.observed_bytes)
      if (entry.inferred_type === 'blob') entry.inferred_type = binaryType(entry.observed_bytes)
    }
    next.forEach((entry, index) => { entry.ordinal_no = index + 1 })
    if (rows.length > 0) await connection.execute(`UPDATE firebase_mysql_sync_field_registry SET ordinal_no = ordinal_no + 10000 WHERE collection_name = ?`, [collection])
    for (const entry of next) await connection.execute(`INSERT INTO firebase_mysql_sync_field_registry (collection_name, field_name, ordinal_no, inferred_type, observed_bytes) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE ordinal_no = VALUES(ordinal_no), inferred_type = VALUES(inferred_type), observed_bytes = GREATEST(observed_bytes, VALUES(observed_bytes))`, [collection, entry.field_name, entry.ordinal_no, entry.inferred_type, entry.observed_bytes])
    await connection.commit()
    return next
  } catch (error) { await connection.rollback().catch(() => {}); throw error } finally { await releaseLock(connection, lockName).catch(() => {}); connection.release() }
}

const TEXT_CAPACITY = { text: 65_535, mediumtext: 16_777_215, longtext: 4_294_967_295, blob: 65_535, mediumblob: 16_777_215, longblob: 4_294_967_295 }
const INTEGER_RANGES = {
  tinyint: [-128n, 127n, 255n], smallint: [-32_768n, 32_767n, 65_535n], mediumint: [-8_388_608n, 8_388_607n, 16_777_215n],
  int: [-2_147_483_648n, 2_147_483_647n, 4_294_967_295n], bigint: [-9_223_372_036_854_775_808n, 9_223_372_036_854_775_807n, 18_446_744_073_709_551_615n],
}
function enumMembers(type) {
  if (!/^enum\(/i.test(type)) return null
  const body = type.substring(type.indexOf('(') + 1, type.length - 1); const values = []; let index = 0
  while (index < body.length) {
    if (body[index] !== "'") throw new Error('existing_enum_definition_unsafe')
    index++; let value = ''
    while (index < body.length) {
      if (body[index] === "'" && body[index + 1] === "'") { value += "'"; index += 2; continue }
      if (body[index] === "'") { index++; break }
      if (body[index] === '\\' && index + 1 < body.length) { value += body[index + 1]; index += 2; continue }
      value += body[index++]
    }
    values.push(value)
    if (index === body.length) break
    if (body[index] !== ',') throw new Error('existing_enum_definition_unsafe')
    index++
  }
  return values
}
function textRequirement(field, value, column = null) {
  let bytes = Number(field.observed_bytes || 0)
  if (value !== null && value !== undefined) {
    const encoded = encodeValue(value, field.inferred_type)
    bytes = Math.max(bytes, Buffer.isBuffer(encoded) ? encoded.length : utf8Bytes(encoded))
  }
  if (column) {
    const varchar = String(column.column_type).match(/^(?:var)?char\((\d+)\)/i)
    if (varchar) {
      const multiplier = String(column.collation_name || '').startsWith('utf8mb4_') ? 4 : String(column.collation_name || '').startsWith('utf8_') ? 3 : 1
      bytes = Math.max(bytes, Number(varchar[1]) * multiplier)
    }
    const members = enumMembers(String(column.column_type))
    if (members) for (const member of members) bytes = Math.max(bytes, utf8Bytes(member))
  }
  return field.inferred_type.includes('blob') ? binaryType(bytes) : stringType(bytes)
}
function timestampCompatible(columnType, value) {
  if (value === null || value === undefined) return true
  const normalized = encodeValue(value, 'timestamp'); const year = Number(normalized.substring(0, 4)); const micros = Number(normalized.substring(20, 26))
  const match = String(columnType).match(/^(timestamp|datetime)(?:\(([0-6])\))?$/i)
  if (!match) return false
  const precision = Number(match[2] || 0); const exactPrecision = micros % (10 ** (6 - precision)) === 0
  if (!exactPrecision) return false
  if (match[1].toLowerCase() === 'datetime') return year >= 1000 && year <= 9999
  const epoch = Date.parse(`${normalized.substring(0, 10)}T${normalized.substring(11, 19)}Z`)
  return epoch >= Date.parse('1970-01-01T00:00:01Z') && epoch <= Date.parse('2038-01-19T03:14:07Z')
}
function integerDecision(columnType, value) {
  const match = String(columnType).match(/^(tinyint|smallint|mediumint|int|bigint)(?:\(\d+\))?( unsigned)?$/i)
  if (!match) {
    if (/^decimal\((?:20|65),0\)$/i.test(String(columnType))) return { compatible: true }
    return { compatible: false, repairType: 'BIGINT' }
  }
  if (value === null || value === undefined) return { compatible: true }
  const integer = typeof value === 'bigint' ? value : BigInt(value); const ranges = INTEGER_RANGES[match[1].toLowerCase()]; const unsigned = Boolean(match[2])
  const minimum = unsigned ? 0n : ranges[0]; const maximum = unsigned ? ranges[2] : ranges[1]
  if (integer >= minimum && integer <= maximum) return { compatible: true }
  if (match[1].toLowerCase() === 'bigint' && unsigned && integer < 0n) return { compatible: false, repairType: 'DECIMAL(20,0)' }
  if (integer >= INTEGER_RANGES.bigint[0] && integer <= INTEGER_RANGES.bigint[1]) return { compatible: false, repairType: 'BIGINT' }
  throw new Error('integer_range_unsupported')
}
export function columnStorageDecision(column, field, value, identity = false) {
  const rawType = String(column.column_type); const actual = rawType.toLowerCase(); const registryType = normalizeRegistryType(field.inferred_type)
  if (identity) {
    const varchar = actual.match(/^varchar\((\d+)\)$/)
    if (varchar && (value === null || value === undefined || Array.from(String(value)).length <= Number(varchar[1]))) return { compatible: true }
    if (varchar && Number(varchar[1]) <= 255) return { compatible: false, repairType: 'VARCHAR(255)' }
    if (actual === 'varchar(255)') return { compatible: true }
    throw new Error('identity_column_type_incompatible')
  }
  if (registryType === 'timestamp') return timestampCompatible(actual, value) ? { compatible: true } : { compatible: false, repairType: 'DATETIME(6)' }
  if (registryType === 'integer') return integerDecision(actual, value)
  if (registryType === 'decimal') {
    if (/^decimal\((?:65),30\)$/i.test(actual)) return { compatible: true }
    if (/^(?:tinyint|smallint|mediumint|int|bigint)(?:\(\d+\))?(?: unsigned)?$/i.test(actual)) return { compatible: false, repairType: 'DECIMAL(65,30)' }
  }
  if (registryType === 'boolean' && /^tinyint\(1\)(?: unsigned)?$/i.test(actual)) return { compatible: true }
  const members = enumMembers(rawType)
  if (members && ['text', 'mediumtext', 'longtext', 'reference'].includes(registryType)) {
    if (value === null || value === undefined || members.includes(String(value))) return { compatible: true }
    return { compatible: false, repairType: sqlType(textRequirement(field, value, column)) }
  }
  const varchar = actual.match(/^(?:var)?char\((\d+)\)$/)
  if (varchar && !registryType.includes('blob')) {
    const encoded = value === null || value === undefined ? '' : String(encodeValue(value, registryType))
    if (value === null || value === undefined || Array.from(encoded).length <= Number(varchar[1])) return { compatible: true }
    return { compatible: false, repairType: sqlType(textRequirement(field, value, column)) }
  }
  if (TEXT_CAPACITY[actual] !== undefined) {
    const encoded = value === null || value === undefined ? null : encodeValue(value, registryType)
    const bytes = encoded === null ? 0 : Buffer.isBuffer(encoded) ? encoded.length : utf8Bytes(encoded)
    const needed = Math.max(Number(field.observed_bytes || 0), bytes)
    if (needed <= TEXT_CAPACITY[actual]) return { compatible: true }
    return { compatible: false, repairType: sqlType(registryType.includes('blob') ? binaryType(needed) : stringType(needed)) }
  }
  const desired = sqlType(registryType).toLowerCase()
  if (actual === desired) return { compatible: true }
  throw new Error('existing_column_type_incompatible')
}

export function repairPlan(schema, fields, contract, document = {}) {
  if (!Array.isArray(fields) || fields.length === 0) return []
  const columns = schema.columns || []; const byName = new Map(columns.map((column) => [column.column_name, column])); const hasInternalKey = byName.has('xId')
  const expectedNames = new Set(fields.map((field) => field.field_name));
  const fieldChanges = fields.map((field, index) => {
    const column = byName.get(field.field_name); const desired = sqlType(field.inferred_type, field.field_name === contract.key)
    const decision = column ? columnStorageDecision(column, field, Object.hasOwn(document, field.field_name) ? document[field.field_name] : undefined, field.field_name === contract.key) : null
    const expectedOrdinal = index + (hasInternalKey ? 2 : 1)
    return { field, column, desired: decision?.repairType || desired, add: !column, widen: Boolean(column && !decision.compatible), reorder: Boolean(column && Number(column.ordinal_position) !== expectedOrdinal) }
  }).filter((change) => change.add || change.widen || change.reorder)
  const legacyChanges = columns.filter((column) => column.column_name !== 'xId' && !expectedNames.has(column.column_name)).map((column) => ({ drop: true, column, field: { field_name: column.column_name } }))
  return [...fieldChanges, ...legacyChanges]
}

function decodeSqlQuotedDefault(value) {
  if (!(value.length >= 2 && value.startsWith("'") && value.endsWith("'"))) return value
  let decoded = ''
  for (let index = 1; index < value.length - 1; index++) {
    const character = value[index]
    if (character === "'") {
      if (value[index + 1] === "'" && index + 1 < value.length - 1) { decoded += "'"; index++; continue }
      throw new Error('existing_column_default_unsafe')
    }
    if (character === '\\') {
      if (index + 1 >= value.length - 1) throw new Error('existing_column_default_unsafe')
      const escaped = value[++index]
      decoded += { '0': '\0', b: '\b', n: '\n', r: '\r', t: '\t', Z: '\x1a', "'": "'", '"': '"', '\\': '\\' }[escaped] ?? escaped
      continue
    }
    decoded += character
  }
  return decoded
}
function quoteSqlDefault(value) {
  const escapes = { '\0': '\\0', '\b': '\\b', '\n': '\\n', '\r': '\\r', '\t': '\\t', '\x1a': '\\Z', '\\': '\\\\', "'": "''" }
  return `'${Array.from(value, (character) => escapes[character] ?? character).join('')}'`
}
function defaultClause(column) {
  if (column.column_default === null || column.column_default === undefined) return column.is_nullable === 'YES' ? ' DEFAULT NULL' : ''
  const value = String(column.column_default)
  if (/^NULL$/i.test(value)) {
    if (column.is_nullable !== 'YES') throw new Error('existing_column_default_unsafe')
    return ' DEFAULT NULL'
  }
  const timestamp = value.match(/^CURRENT_TIMESTAMP(?:\(([0-6]?)\))?$/i)
  if (timestamp) return ` DEFAULT CURRENT_TIMESTAMP${value.includes('(') ? `(${timestamp[1]})` : ''}`
  return ` DEFAULT ${quoteSqlDefault(decodeSqlQuotedDefault(value))}`
}
export function existingDefinition(column, desired) {
  let type = desired || String(column.column_type).toUpperCase()
  const members = enumMembers(String(column.column_type))
  if (!desired && members) type = `ENUM(${members.map((member) => `'${member.replaceAll("'", "''")}'`).join(',')})`
  if (!/^(?:(?:VAR)?CHAR\(\d+\)|(?:TINY|SMALL|MEDIUM|BIG)?INT(?:\(\d+\))?(?: UNSIGNED)?|DECIMAL\(\d+,\d+\)(?: UNSIGNED)?|(?:DATE)?TIME(?:STAMP)?(?:\([0-6]\))?|(?:TINY|MEDIUM|LONG)?TEXT|(?:TINY|MEDIUM|LONG)?BLOB|JSON|ENUM\(.+\))$/i.test(type)) throw new Error('existing_column_definition_unsafe')
  const extra = String(column.extra || '').trim().replace(/\s+/g, ' ').toUpperCase()
  const extraMatch = extra.match(/^(?:DEFAULT_GENERATED(?: )?)?(ON UPDATE CURRENT_TIMESTAMP(?:\([0-6]?\))?)?$/)
  if (!extraMatch) throw new Error('existing_column_definition_unsafe')
  const executableExtra = extraMatch[1] || ''
  const collation = column.collation_name === null || column.collation_name === undefined ? '' : String(column.collation_name)
  if (collation && !/^[a-z0-9_]{1,64}$/.test(collation)) throw new Error('existing_column_collation_unsafe')
  return `${type}${collation ? ` COLLATE ${collation}` : ''} ${column.is_nullable === 'YES' ? 'NULL' : 'NOT NULL'}${defaultClause(column)}${executableExtra ? ` ${executableExtra}` : ''}`
}

export function alterStatement(contract, change, fields, schema = { columns: [] }) {
  if (change.drop) return `ALTER TABLE ${quoteIdentifier(contract.table)} DROP COLUMN ${quoteIdentifier(change.column.column_name)}`
  const hasInternalKey = (schema.columns || []).some((column) => column.column_name === 'xId')
  const previous = change.field.ordinal_no === 1 ? (hasInternalKey ? `AFTER ${quoteIdentifier('xId')}` : 'FIRST') : `AFTER ${quoteIdentifier(fields[change.field.ordinal_no - 2].field_name)}`
  const definition = change.column ? existingDefinition(change.column, change.widen ? change.desired : null) : `${change.desired} NULL`
  const action = change.column ? 'MODIFY COLUMN' : 'ADD COLUMN'
  return `ALTER TABLE ${quoteIdentifier(contract.table)} ${action} ${quoteIdentifier(change.field.field_name)} ${definition} ${previous}`
}

export async function repairParity(pool, collection, config, document = {}) {
  const contract = collectionContract(collection); const connection = await pool.getConnection(); const lockName = `rbms_fm_repair_${contract.table}`
  let migration = null
  try {
    await advisoryLock(connection, lockName, config.advisoryLockSeconds)
    const [fields] = await connection.execute(`SELECT field_name, ordinal_no, inferred_type, observed_bytes FROM firebase_mysql_sync_field_registry WHERE collection_name = ? ORDER BY ordinal_no`, [collection])
    let schema = await actualSchema(connection, contract.table)
    if (schema.columns.length === 0) {
      const definitions = fields.map((field) => `${quoteIdentifier(field.field_name)} ${sqlType(field.inferred_type, field.field_name === contract.key)} ${field.field_name === contract.key ? 'NOT NULL' : 'NULL'}`)
      await connection.query(`CREATE TABLE ${quoteIdentifier(contract.table)} (xId INT(10) NOT NULL AUTO_INCREMENT, ${definitions.join(', ')}, PRIMARY KEY (xId), UNIQUE KEY ${quoteIdentifier(`uq_${contract.table}_${contract.key}`)} (${quoteIdentifier(contract.key)})) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`)
      schema = await actualSchema(connection, contract.table)
      if (repairPlan(schema, fields, contract, document).length !== 0) throw new Error('initial_table_schema_verification_failed')
      return { repaired: true, created: true, fields }
    }
      const plan = repairPlan(schema, fields, contract, document)
    if (plan.length === 0) return { repaired: false, fields }
    migration = await createVerifiedBackup(connection, { table: contract.table, collection, repairKind: 'schema_parity', timezone: config.backupTimezone, maxTableBytes: config.backupMaxTableBytes })
    try {
      for (const change of plan) {
        await connection.query(alterStatement(contract, change, fields, schema))
      }
      const post = await actualSchema(connection, contract.table)
      if (repairPlan(post, fields, contract, document).length !== 0) throw new Error('schema_post_verification_failed')
      const fingerprint = schemaFingerprint(post)
      await finishMigration(connection, migration.historyId, 'COMPLETED', fingerprint)
      return { repaired: true, created: false, backup: migration, fields }
    } catch (error) {
      await finishMigration(connection, migration.historyId, 'FAILED', null, safeErrorCode(error, 'schema_repair_failed')).catch(() => {})
      throw error
    }
  } finally { await releaseLock(connection, lockName).catch(() => {}); connection.release() }
}
