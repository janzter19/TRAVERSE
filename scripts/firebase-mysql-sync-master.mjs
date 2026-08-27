import crypto from 'node:crypto'
import { realpathSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import mysql from 'mysql2/promise'
import { cert, getApps, initializeApp } from 'firebase-admin/app'
import { FieldValue, getFirestore } from 'firebase-admin/firestore'
import { loadConfig } from './firebase-mysql-sync/config.mjs'
import { COLLECTIONS } from './firebase-mysql-sync/registry.mjs'
import { claimReady, ensureControlTables } from './firebase-mysql-sync/queue-store.mjs'
import { discoverSnapshot, processClaimed } from './firebase-mysql-sync/worker.mjs'
import { safeErrorCode, telemetry } from './firebase-mysql-sync/telemetry.mjs'

export function isMainModule({ invocationPath = process.argv[1], moduleUrl = import.meta.url, canonicalize = realpathSync } = {}) {
  if (typeof invocationPath !== 'string' || invocationPath.length === 0) return false
  try {
    return canonicalize(invocationPath) === canonicalize(fileURLToPath(moduleUrl))
  } catch {
    return false
  }
}

const mysqlTimestamp = (date = new Date()) => {
  const pad = (value, length = 2) => String(value).padStart(length, '0')
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())} ${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())}.${pad(date.getUTCMilliseconds(), 3)}000`
}

export async function ensureTraverseMetadata({ db, collection, snapshot }) {
  const contract = COLLECTIONS[collection]
  if (!contract?.dynamicFields) return snapshot
  if (typeof db.collection(collection).doc !== 'function') return snapshot
  const data = snapshot.data() || {}
  const now = mysqlTimestamp()
  const patch = {}
  if (!Object.hasOwn(data, 'firebase_collection')) patch.firebase_collection = collection
  if (!Object.hasOwn(data, 'mysql_created_at')) patch.mysql_created_at = now
  if (!Object.hasOwn(data, 'mysql_updated_at')) patch.mysql_updated_at = now
  if (!Object.hasOwn(data, 'mysql_deleted_at')) patch.mysql_deleted_at = null
  if (!Object.hasOwn(data, 'mysql_synced_at')) patch.mysql_synced_at = null
  if (!Object.hasOwn(data, 'mysql_sync_status')) patch.mysql_sync_status = 'PENDING'
  if (Object.keys(patch).length === 0) return snapshot
  await db.collection(collection).doc(snapshot.id).set(patch, { merge: true })
  return db.collection(collection).doc(snapshot.id).get()
}

export async function scanPendingPages({ db, pool, collection, config, shouldStop = () => false }) {
  let cursor = null; let discovered = 0
  do {
    const dynamic = COLLECTIONS[collection]?.dynamicFields
    let query = dynamic ? db.collection(collection).orderBy('__name__').limit(config.scanPageSize) : db.collection(collection).where('mysql_sync_status', '==', 'PENDING').orderBy('__name__').limit(config.scanPageSize)
    if (cursor) query = query.startAfter(cursor)
    const snapshot = await query.get()
    for (const original of snapshot.docs) {
      if (shouldStop()) break
      const document = await ensureTraverseMetadata({ db, collection, snapshot: original })
      if (dynamic && typeof document.get === 'function' && String(document.get('mysql_sync_status') || '').toUpperCase() !== 'PENDING') continue
      await discoverSnapshot(pool, collection, document, config); discovered++
    }
    cursor = snapshot.docs.at(-1) || null
    if (snapshot.size < config.scanPageSize) break
  } while (!shouldStop())
  return discovered
}

export function attachPendingListeners({ db, pool, config, track = (promise) => promise, isStopping = () => false, discover = discoverSnapshot }) {
  const unsubscribers = []
  for (const collection of Object.keys(COLLECTIONS)) {
    try {
      const dynamic = COLLECTIONS[collection]?.dynamicFields
      const source = dynamic ? db.collection(collection) : db.collection(collection).where('mysql_sync_status', '==', 'PENDING')
      const unsubscribe = source.onSnapshot((snapshot) => {
        if (isStopping()) return
        for (const change of snapshot.docChanges()) if (change.type !== 'removed') track((async () => {
          const document = await ensureTraverseMetadata({ db, collection, snapshot: change.doc })
          if (String(document.get('mysql_sync_status') || '').toUpperCase() === 'PENDING') await discover(pool, collection, document, config)
        })().catch((error) => telemetry('discovery_failed', { collection, document_id: change.doc.id, code: safeErrorCode(error, 'discovery_failed') })))
      }, (error) => telemetry('listener_failed', { collection, code: safeErrorCode(error, 'listener_failed') }))
      unsubscribers.push(unsubscribe)
    } catch (error) { telemetry('listener_setup_failed', { collection, code: safeErrorCode(error, 'listener_setup_failed') }) }
  }
  return unsubscribers
}

export async function runService({ config = loadConfig(), mysqlModule = mysql, firebase = null } = {}) {
  const workerKey = crypto.randomUUID()
  const pool = mysqlModule.createPool({ ...config.database, waitForConnections: true, connectionLimit: config.workerConcurrency + 4, charset: 'utf8mb4', dateStrings: true, supportBigNumbers: true, bigNumberStrings: true })
  if (!firebase) {
    if (getApps().length === 0) initializeApp({ credential: cert(config.serviceAccountPath), projectId: config.firebaseProjectId })
    firebase = getFirestore()
    firebase.settings({ ignoreUndefinedProperties: false, useBigInt: true })
  }
  let stopping = false
  const unsubscribers = []; const active = new Set(); const activeCollections = new Set(); const waits = new Map()
  const delay = (milliseconds) => new Promise((resolve) => {
    const timer = setTimeout(() => { waits.delete(timer); resolve() }, milliseconds)
    waits.set(timer, resolve)
  })
  const track = (promise) => { active.add(promise); promise.finally(() => active.delete(promise)); return promise }
  const stop = () => {
    if (stopping) return
    stopping = true
    for (const unsubscribe of unsubscribers) unsubscribe()
    for (const [timer, resolve] of waits) { clearTimeout(timer); resolve() }
    waits.clear()
  }

  async function scannerLoop(collection) {
    while (!stopping) {
      try { await scanPendingPages({ db: firebase, pool, collection, config, shouldStop: () => stopping }) }
      catch (error) { telemetry('scan_failed', { collection, code: safeErrorCode(error, 'scan_failed') }) }
      if (!stopping) await delay(config.scanIntervalMs)
    }
  }
  async function workerLoop() {
    while (!stopping) {
      try {
        const capacity = config.workerConcurrency - activeCollections.size
        if (capacity <= 0) { await delay(config.workPollMs); continue }
        const rows = await claimReady(pool, workerKey, config.leaseSeconds, capacity, [...activeCollections])
        for (const row of rows) {
          activeCollections.add(row.collection_name)
          const work = processClaimed({ pool, db: firebase, row, workerKey, config }).finally(() => activeCollections.delete(row.collection_name))
          track(work)
        }
        if (rows.length === 0) await delay(config.workPollMs)
      } catch (error) { telemetry('worker_poll_failed', { code: safeErrorCode(error, 'worker_poll_failed') }); if (!stopping) await delay(config.workPollMs) }
    }
  }

  await ensureControlTables(pool, COLLECTIONS)
  unsubscribers.push(...attachPendingListeners({ db: firebase, pool, config, track, isStopping: () => stopping }))
  process.once('SIGINT', stop); process.once('SIGTERM', stop)
  telemetry('started', { collections: Object.keys(COLLECTIONS), worker_key: workerKey })
  await Promise.all([...Object.keys(COLLECTIONS).map(scannerLoop), workerLoop()])
  await Promise.allSettled([...active])
  await pool.end()
  telemetry('stopped')
}

if (isMainModule()) runService().catch((error) => { telemetry('startup_failed', { code: safeErrorCode(error, 'startup_failed') }); process.exitCode = 1 })
