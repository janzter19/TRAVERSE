export function safeErrorCode(error, fallback = 'operation_failed') {
  const candidate = error && typeof error === 'object' && typeof error.code === 'string' ? error.code : error instanceof Error ? error.message : ''
  return /^[A-Za-z][A-Za-z0-9_]{0,119}$/.test(candidate) ? candidate.toLowerCase() : fallback
}

export function telemetry(event, fields = {}) {
  const safe = Object.fromEntries(Object.entries(fields).filter(([key]) => !/password|secret|payload|message_text/i.test(key)))
  process.stdout.write(`${JSON.stringify({ service: 'firebase-mysql-sync', event, at: new Date().toISOString(), ...safe })}\n`)
}
