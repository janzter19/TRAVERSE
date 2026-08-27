#!/usr/bin/env node

// Stable TRAVERSE entrypoint. The implementation remains in the tested
// Firebase-to-MySQL worker module so the service name can change without
// duplicating orchestration logic.
import { runService } from './firebase-mysql-sync-master.mjs'

runService().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : 'traverse_failed'}\n`)
  process.exitCode = 1
})
