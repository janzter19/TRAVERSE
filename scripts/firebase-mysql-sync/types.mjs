import crypto from 'node:crypto'
import { validIdentifier } from './registry.mjs'

const TEXT_MAX = 65_535
const MEDIUM_MAX = 16_777_215
const LONG_MAX = 4_294_967_295
const INT64_MIN = -9_223_372_036_854_775_808n
const INT64_MAX = 9_223_372_036_854_775_807n

export function utf8Bytes(value) { return Buffer.byteLength(String(value), 'utf8') }
export function normalizeRegistryType(type) { return type === 'string' ? 'text' : type }
export function firestoreRevision(snapshot) {
  const timestamp = snapshot?.updateTime
  const seconds = timestamp?.seconds ?? timestamp?._seconds
  const nanoseconds = timestamp?.nanoseconds ?? timestamp?._nanoseconds
  if (!Number.isInteger(seconds) || !Number.isInteger(nanoseconds) || nanoseconds < 0 || nanoseconds > 999_999_999) throw new Error('firestore_update_time_required')
  return `${seconds}:${String(nanoseconds).padStart(9, '0')}`
}
export function compareRevisions(left, right) {
  const parse = (value) => {
    const match = String(value).match(/^(0|[1-9][0-9]*):([0-9]{9})$/)
    if (!match || Number(match[2]) > 999_999_999) throw new Error('firestore_revision_invalid')
    return [BigInt(match[1]), BigInt(match[2])]
  }
  const a = parse(left); const b = parse(right)
  return a[0] === b[0] ? (a[1] === b[1] ? 0 : a[1] < b[1] ? -1 : 1) : a[0] < b[0] ? -1 : 1
}

function timestampParts(value) {
  if (value?.toDate instanceof Function) {
    const seconds = value.seconds ?? value._seconds
    const nanos = value.nanoseconds ?? value._nanoseconds
    if (!Number.isInteger(seconds) || !Number.isInteger(nanos) || nanos % 1000 !== 0) throw new Error('timestamp_precision_unsupported')
    let iso
    try { iso = new Date(seconds * 1000).toISOString() } catch { throw new Error('timestamp_invalid') }
    const year = Number(iso.substring(0, 4))
    if (year < 1000 || year > 9999) throw new Error('timestamp_mysql_range_invalid')
    return `${iso.substring(0, 19).replace('T', ' ')}.${String(nanos / 1000).padStart(6, '0')}`
  }
  const text = String(value ?? '').trim()
  let match = text.match(/^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?Z?$/)
  if (!match) throw new Error('timestamp_invalid')
  const micros = String(match[3] || '').padEnd(6, '0')
  const canonical = `${match[1]}T${match[2]}.${micros}Z`
  if (Number.isNaN(Date.parse(canonical)) || new Date(`${match[1]}T${match[2]}Z`).toISOString().substring(0, 19) !== `${match[1]}T${match[2]}`) throw new Error('timestamp_invalid')
  return `${match[1]} ${match[2]}.${micros}`
}

function tagged(value, stack = new WeakSet()) {
  if (value === null) return null
  if (value === undefined) throw new Error('undefined_value_unsupported')
  if (typeof value === 'boolean' || typeof value === 'string') return value
  if (typeof value === 'bigint') { assertInt64(value); return { __firestore_type: 'integer', value: value.toString() } }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new Error('number_non_finite')
    if (Number.isInteger(value) && !Number.isSafeInteger(value)) throw new Error('integer_precision_unsafe')
    return value
  }
  if (Buffer.isBuffer(value) || value?.toBase64 instanceof Function) return { __firestore_type: 'bytes', value: bytesBuffer(value).toString('base64') }
  if (value?.toDate instanceof Function) return { __firestore_type: 'timestamp', value: timestampParts(value) }
  if (typeof value?.path === 'string') { if (value.path === '') throw new Error('reference_path_invalid'); return { __firestore_type: 'reference', value: value.path } }
  if (Number.isFinite(value?.latitude) && Number.isFinite(value?.longitude)) { assertGeoPoint(value); return { __firestore_type: 'geopoint', latitude: value.latitude, longitude: value.longitude } }
  if (typeof value !== 'object') throw new Error('firestore_type_unsupported')
  if (stack.has(value)) throw new Error('cyclic_value_unsupported')
  stack.add(value)
  const result = Array.isArray(value)
    ? value.map((entry) => tagged(entry, stack))
    : Object.fromEntries(Object.keys(value).sort().map((field) => [field, tagged(value[field], stack)]))
  stack.delete(value)
  return result
}

export function stableJson(value) { return JSON.stringify(tagged(value)) }
export function payloadFingerprint(value) { return crypto.createHash('sha256').update(stableJson(value)).digest('hex') }

export function assertDocument(contract, snapshot, maxDocumentBytes) {
  const data = snapshot?.data?.() ?? snapshot?.data ?? {}
  const documentId = String(snapshot?.id ?? '')
  if (documentId === '' || String(data[contract.key] ?? '') !== documentId) throw new Error('firestore_document_id_key_mismatch')
  if (Array.from(documentId).length > 255) throw new Error('firestore_document_id_too_long')
  if (!contract.dynamicFields) {
    for (const field of contract.requiredFields || []) if (!Object.hasOwn(data, field)) throw new Error(`firestore_required_field_missing_${field}`)
    for (const field of contract.requiredNonEmptyFields || []) if (data[field] === null || data[field] === undefined || String(data[field]).trim() === '') throw new Error(`firestore_required_field_empty_${field}`)
  }
  if (contract.strictFields && !contract.dynamicFields) {
    const allowed = new Set(contract.fields.map((field) => field.name))
    for (const field of Object.keys(data)) if (!allowed.has(field)) throw new Error('firestore_field_not_allowlisted')
  }
  if (contract.dynamicFields) {
    for (const field of contract.forbiddenFields || []) if (Object.hasOwn(data, field)) throw new Error('firestore_field_not_allowlisted')
  }
  if (contract.expectedFirebaseCollection && data.firebase_collection !== contract.expectedFirebaseCollection) throw new Error('firestore_collection_metadata_mismatch')
  if (contract.requirePending && String(data.mysql_sync_status ?? '').toUpperCase() !== 'PENDING') throw new Error('firestore_sync_status_not_pending')
  if (contract.authIdentityField && String(data[contract.authIdentityField] ?? '') !== documentId) throw new Error('firestore_auth_uid_document_id_mismatch')
  if (contract.rejectDisabled && (data.user_disabled === true || data.user_deleted === true || data.user_locked === true || String(data.user_status ?? '').toUpperCase() === 'DELETED')) throw new Error('firestore_user_disabled_or_deleted')
  for (const field of Object.keys(data)) if (!validIdentifier(field)) throw new Error('firestore_field_identifier_invalid')
  if (utf8Bytes(stableJson(data)) > maxDocumentBytes) throw new Error('firestore_document_too_large')
  return data
}

export function inferType(value, declaredType = null) {
  const declared = normalizeRegistryType(declaredType)
  if (value === null || value === undefined) return 'null'
  if (declared === 'timestamp') { timestampParts(value); return 'timestamp' }
  if (typeof value === 'boolean') return 'boolean'
  if (typeof value === 'bigint') { assertInt64(value); return 'integer' }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new Error('number_non_finite')
    if (Number.isInteger(value)) { if (!Number.isSafeInteger(value)) throw new Error('integer_precision_unsafe'); return 'integer' }
    decimalString(value); return 'decimal'
  }
  if (typeof value === 'string') return stringType(utf8Bytes(value))
  if (Buffer.isBuffer(value) || value?.toBase64 instanceof Function) return binaryType(bytesBuffer(value).length)
  if (value?.toDate instanceof Function) { timestampParts(value); return 'timestamp' }
  if (typeof value?.path === 'string') { if (value.path === '') throw new Error('reference_path_invalid'); return 'reference' }
  if (Number.isFinite(value?.latitude) && Number.isFinite(value?.longitude)) { assertGeoPoint(value); return 'geopoint' }
  if (Array.isArray(value) || typeof value === 'object') { stableJson(value); return 'json' }
  throw new Error('firestore_type_unsupported')
}

export function stringType(bytes) { return bytes <= TEXT_MAX ? 'text' : bytes <= MEDIUM_MAX ? 'mediumtext' : bytes <= LONG_MAX ? 'longtext' : (() => { throw new Error('string_too_large') })() }
export function binaryType(bytes) { return bytes <= TEXT_MAX ? 'blob' : bytes <= MEDIUM_MAX ? 'mediumblob' : bytes <= LONG_MAX ? 'longblob' : (() => { throw new Error('bytes_too_large') })() }
const TYPE_RANK = { text: 1, mediumtext: 2, longtext: 3, blob: 1, mediumblob: 2, longblob: 3 }
export function promoteType(current, incoming) {
  current = normalizeRegistryType(current); incoming = normalizeRegistryType(incoming)
  if (!current || current === 'null') return incoming
  if (incoming === 'null' || current === incoming) return current
  if (TYPE_RANK[current] && TYPE_RANK[incoming] && ((current.includes('text') && incoming.includes('text')) || (current.includes('blob') && incoming.includes('blob')))) return TYPE_RANK[current] >= TYPE_RANK[incoming] ? current : incoming
  if (current === 'integer' && incoming === 'decimal') return 'decimal'
  if (current === 'timestamp') throw new Error('timestamp_type_mismatch')
  throw new Error('field_type_incompatible')
}

export function encodeValue(value, type) {
  type = normalizeRegistryType(type)
  if (value === null || value === undefined) return null
  if (type === 'timestamp') return timestampParts(value)
  if (type.includes('blob')) return bytesBuffer(value)
  if (['json', 'geopoint'].includes(type)) return stableJson(value)
  if (type === 'reference') return value.path
  if (type === 'boolean') return value ? 1 : 0
  if (type === 'integer') { const integer = typeof value === 'bigint' ? value : BigInt(value); assertInt64(integer); return integer.toString() }
  if (type === 'decimal') return decimalString(value)
  return String(value)
}

function assertInt64(value) { if (value < INT64_MIN || value > INT64_MAX) throw new Error('integer_range_unsupported') }
function bytesBuffer(value) {
  if (Buffer.isBuffer(value)) return value
  const encoded = String(value.toBase64())
  const decoded = Buffer.from(encoded, 'base64')
  if (decoded.toString('base64').replace(/=+$/, '') !== encoded.replace(/=+$/, '')) throw new Error('bytes_encoding_invalid')
  return decoded
}
function assertGeoPoint(value) {
  if (value.latitude < -90 || value.latitude > 90 || value.longitude < -180 || value.longitude > 180) throw new Error('geopoint_range_invalid')
}
function decimalString(value) {
  if (typeof value !== 'number' || !Number.isFinite(value)) throw new Error('decimal_invalid')
  const source = String(value)
  const match = source.match(/^(-?)(\d+)(?:\.(\d+))?(?:e([+-]?\d+))?$/i)
  if (!match) throw new Error('decimal_invalid')
  const sign = match[1]; const whole = match[2]; const fraction = match[3] || ''; const exponent = Number(match[4] || 0)
  const digits = `${whole}${fraction}`; const point = whole.length + exponent
  const expanded = point <= 0 ? `0.${'0'.repeat(-point)}${digits}` : point >= digits.length ? `${digits}${'0'.repeat(point - digits.length)}` : `${digits.substring(0, point)}.${digits.substring(point)}`
  const [integerPart, fractionPart = ''] = expanded.split('.')
  if (integerPart.replace(/^0+/, '').length > 35 || fractionPart.length > 30 || integerPart.length + fractionPart.length > 65) throw new Error('decimal_range_unsupported')
  return `${sign}${expanded}`
}

export function normalizeDatabaseValue(value, type) {
  type = normalizeRegistryType(type)
  if (value === null || value === undefined) return null
  if (type === 'boolean') return Number(value) === 1 ? 1 : 0
  if (type === 'integer') return String(value)
  if (type === 'decimal') {
    const text = String(value)
    return text.includes('.') ? text.replace(/0+$/, '').replace(/\.$/, '') : text
  }
  if (type === 'timestamp') return timestampParts(String(value))
  if (type.includes('blob')) return Buffer.from(value).toString('base64')
  if (['json', 'geopoint'].includes(type)) return stableJson(typeof value === 'string' ? JSON.parse(value) : value)
  return String(value)
}

export function valuesEqual(source, database, type) {
  const encoded = encodeValue(source, type)
  if (encoded === null) return database === null || database === undefined
  if (normalizeRegistryType(type).includes('blob')) return Buffer.from(encoded).toString('base64') === normalizeDatabaseValue(database, type)
  return normalizeDatabaseValue(encoded, type) === normalizeDatabaseValue(database, type)
}
