import path from 'node:path'
import { fileURLToPath } from 'node:url'
import dotenv from 'dotenv'

const rootDir = path.dirname(path.dirname(path.dirname(fileURLToPath(import.meta.url))))

export function loadConfig(environment = process.env) {
  dotenv.config({ path: path.join(rootDir, '.env'), quiet: true })
  const value = (name, fallback = '') => String(environment[name] ?? process.env[name] ?? fallback).trim()
  const required = (name) => {
    const result = value(name)
    if (result === '') throw new Error(`${name}_required`)
    return result
  }
  const integer = (name, fallback, minimum, maximum) => {
    const parsed = Number(value(name, String(fallback)))
    if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) throw new Error(`${name}_invalid`)
    return parsed
  }
  return {
    rootDir,
    firebaseProjectId: required('FIREBASE_PROJECT_ID'),
    serviceAccountPath: required('GOOGLE_APPLICATION_CREDENTIALS'),
    database: {
      host: required('FIREBASE_MYSQL_SYNC_DB_HOST'),
      port: integer('FIREBASE_MYSQL_SYNC_DB_PORT', 3306, 1, 65535),
      user: required('FIREBASE_MYSQL_SYNC_DB_USER'),
      password: required('FIREBASE_MYSQL_SYNC_DB_PASSWORD'),
      database: required('FIREBASE_MYSQL_SYNC_DB_NAME'),
    },
    scanIntervalMs: integer('FIREBASE_MYSQL_SYNC_SCAN_INTERVAL_MS', 30000, 1000, 3600000),
    workPollMs: integer('FIREBASE_MYSQL_SYNC_WORK_POLL_MS', 500, 50, 60000),
    scanPageSize: integer('FIREBASE_MYSQL_SYNC_SCAN_PAGE_SIZE', 100, 1, 500),
    workerConcurrency: integer('FIREBASE_MYSQL_SYNC_WORKER_CONCURRENCY', 4, 1, 32),
    leaseSeconds: integer('FIREBASE_MYSQL_SYNC_LEASE_SECONDS', 90, 10, 3600),
    maxAttempts: integer('FIREBASE_MYSQL_SYNC_MAX_ATTEMPTS', 12, 1, 100),
    backupTimezone: value('FIREBASE_MYSQL_SYNC_BACKUP_TIMEZONE', 'Asia/Manila'),
    maxDocumentBytes: integer('FIREBASE_MYSQL_SYNC_MAX_DOCUMENT_BYTES', 1048576, 1024, 16777216),
    maxFieldsPerCollection: integer('FIREBASE_MYSQL_SYNC_MAX_FIELDS_PER_COLLECTION', 160, 1, 512),
    maxNewFieldsPerScan: integer('FIREBASE_MYSQL_SYNC_MAX_NEW_FIELDS_PER_SCAN', 12, 1, 64),
    backupMaxTableBytes: integer('FIREBASE_MYSQL_SYNC_BACKUP_MAX_TABLE_BYTES', 1073741824, 1048576, Number.MAX_SAFE_INTEGER),
    advisoryLockSeconds: integer('FIREBASE_MYSQL_SYNC_ADVISORY_LOCK_SECONDS', 30, 1, 300),
  }
}
