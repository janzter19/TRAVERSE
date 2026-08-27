import { FieldValue } from 'firebase-admin/firestore'
import { firestoreRevision } from './types.mjs'

export async function acknowledgePending(db, collection, documentId, revision) {
  const reference = db.collection(collection).doc(documentId)
  const outcome = await db.runTransaction(async (transaction) => {
    const current = await transaction.get(reference)
    if (!current.exists) return 'document_missing'
    const status = String(current.get('mysql_sync_status') || '')
    if (status === 'SYNCED' && current.get('mysql_synced_at')) return 'already_synced'
    if (status !== 'PENDING') return 'state_changed'
    if (!current.updateTime || firestoreRevision(current) !== String(revision)) return 'revision_changed'
    transaction.update(reference, { mysql_sync_status: 'SYNCED', mysql_synced_at: FieldValue.serverTimestamp(), mysql_sync_error: FieldValue.delete() })
    return 'acknowledged'
  })
  if (!['acknowledged', 'already_synced'].includes(outcome)) return { outcome }
  const acknowledged = await reference.get()
  if (!acknowledged.exists || String(acknowledged.get('mysql_sync_status') || '') !== 'SYNCED' || !acknowledged.get('mysql_synced_at')) throw new Error('firebase_ack_read_back_failed')
  return { outcome, mysqlSyncStatus: 'SYNCED', mysqlSyncedAt: acknowledged.get('mysql_synced_at') }
}
