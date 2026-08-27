const key = (name, type = 'text') => ({ name, type })

const PORTAL_LIFECYCLE_FIELDS = [
  key('mysql_created_at', 'timestamp'), key('mysql_updated_at', 'timestamp'), key('mysql_deleted_at', 'timestamp'),
  key('mysql_synced_at', 'timestamp'), key('mysql_sync_status'),
]
const portalLifecycle = () => [key('firebase_collection'), ...PORTAL_LIFECYCLE_FIELDS]
const portalContract = (table, primaryKey, fields, options = {}) => ({
  table,
  key: primaryKey,
  fields: [],
  ackPreserveColumns: [],
  dynamicFields: true,
  ...options,
})

const BED_TASK_FIELDS = [
  key('tenant_key'), key('project_key'), key('bed_task_key'), key('bed_key'), key('bed_source_key'), key('source_pk_psbeds'), key('bed_no'),
  key('branch_key'), key('branch_name'), key('building_key'), key('building_name'), key('floor_key'), key('floor_name'),
  key('nurse_station_key'), key('nurse_station_name'), key('room_key'), key('room_class_key'), key('room_class'),
  key('source_bed_status_key'), key('source_bed_status'), key('task_key'), key('task_code'), key('task_title'), key('task_type'), key('task_color_hex'),
  key('task_sort_order', 'integer'), key('task_group_keys', 'json'), key('task_status'), key('current_task_stage_key'), key('current_stage_label'),
  key('current_stage_color_hex'), key('task_stage_key'), key('stage_label'), key('stage_color_hex'), key('task_stage_response_key'),
  key('response_label'), key('response_description'), key('response_color_hex'), key('bed_status_at_request'), key('bed_class'),
  key('bed_treatment_key'), key('bed_treatment_name'), key('bed_source_option_key'), key('bed_source_option_name'), key('remarks'),
  key('requester_user_key'), key('requester_fullname'), key('firebase_collection'), key('mysql_sync_status'), key('mysql_created_at', 'timestamp'),
  key('mysql_updated_at', 'timestamp'), key('mysql_synced_at', 'timestamp'), key('mysql_deleted_at', 'timestamp'),
]
const projection = (table, primaryKey, fields) => ({ table, key: primaryKey, fields: [], ackPreserveColumns: [], dynamicFields: true })

const PROJECT_GROUP_FIELDS = [
  key('group_key'), key('project_key'), key('group_name'), key('group_description'), key('group_status'),
  ...portalLifecycle(),
]
const PROJECT_POSITION_FIELDS = [
  key('position_key'), key('project_key'), key('group_key'), key('position_code'), key('position_name'), key('position_description'), key('position_status'),
  ...portalLifecycle(),
]
const PROJECT_USER_GROUP_ASSIGNMENT_FIELDS = [
  key('assignment_key'), key('project_key'), key('group_key'), key('user_key'), key('position_key'), key('assignment_status'),
  ...portalLifecycle(),
]
const PROJECT_USER_PROFILE_FIELDS = [
  key('user_key'), key('firebase_uid'), key('project_key'), key('user_login'), key('user_auth_username'), key('user_auth_email'),
  key('user_name'), key('user_chat_name'), key('user_mobile_number'), key('user_avatar_path'), key('user_avatar_original_name'),
  key('user_avatar_mime_type'), key('user_avatar_byte_size', 'integer'), key('user_avatar_sha256'), key('user_avatar_uploaded_at', 'timestamp'),
  key('user_status'), key('user_password_change_required', 'boolean'), key('user_disabled', 'boolean'), key('user_deleted', 'boolean'), key('user_locked', 'boolean'),
  key('user_last_login_at', 'timestamp'), key('user_last_login_ip_address'), key('user_last_login_device'), key('user_last_logout_at', 'timestamp'),
  key('user_last_logout_ip_address'), key('user_last_logout_device'), key('user_password_reset_at', 'timestamp'), key('user_activated_at', 'timestamp'),
  key('user_deactivated_at', 'timestamp'), key('user_locked_at', 'timestamp'), key('firebase_collection'), ...PORTAL_LIFECYCLE_FIELDS,
  key('firebase_created_at', 'timestamp'), key('firebase_updated_at', 'timestamp'), key('firebase_deleted_at', 'timestamp'),
]
const PROJECT_USER_LOGIN_HISTORY_FIELDS = [
  key('user_login_history_key'), key('project_key'), key('user_key'), key('user_login'), key('user_action'), key('user_action_status'),
  key('user_action_at', 'timestamp'), key('user_previous_status'), key('user_new_status'), key('user_action_reason'), key('user_performed_by_key'),
  key('user_ip_address'), key('user_device'), key('firebase_collection'), ...PORTAL_LIFECYCLE_FIELDS,
  key('firebase_created_at', 'timestamp'), key('firebase_updated_at', 'timestamp'), key('firebase_deleted_at', 'timestamp'),
]

export const COLLECTIONS = Object.freeze({
  project_bed_task: projection('project_bed_task', 'bed_task_key', BED_TASK_FIELDS),
  project_bed_task_log: projection('project_bed_task_log', 'bed_task_log_key', [key('bed_task_log_key'), ...BED_TASK_FIELDS, key('event_type'), key('status_from'), key('status_to'), key('actor_user_key'), key('actor_fullname')]),
  project_messenger_chat: projection('project_messenger_chat', 'chat_key', []),
  project_messenger_chat_attachment: projection('project_messenger_chat_attachment', 'attachment_key', []),
  project_messenger_chat_reaction: projection('project_messenger_chat_reaction', 'reaction_key', []),
  project_group: portalContract('project_group', 'group_key', PROJECT_GROUP_FIELDS, { expectedFirebaseCollection: 'project_group', strictFields: true, requirePending: true, requiredFields: ['group_key', 'project_key', 'group_name', 'group_status', 'firebase_collection', ...PORTAL_LIFECYCLE_FIELDS.map((field) => field.name)], requiredNonEmptyFields: ['project_key', 'group_name'] }),
  project_position: portalContract('project_position', 'position_key', PROJECT_POSITION_FIELDS, { expectedFirebaseCollection: 'project_position', strictFields: true, requirePending: true, requiredFields: ['position_key', 'project_key', 'group_key', 'position_code', 'position_name', 'position_status', 'firebase_collection', ...PORTAL_LIFECYCLE_FIELDS.map((field) => field.name)], requiredNonEmptyFields: ['project_key', 'group_key', 'position_code', 'position_name'] }),
  project_user_group: portalContract('project_user_group', 'assignment_key', PROJECT_USER_GROUP_ASSIGNMENT_FIELDS, { expectedFirebaseCollection: 'project_user_group', strictFields: true, requirePending: true, requiredFields: ['assignment_key', 'project_key', 'group_key', 'user_key', 'assignment_status', 'firebase_collection', ...PORTAL_LIFECYCLE_FIELDS.map((field) => field.name)], requiredNonEmptyFields: ['project_key', 'group_key', 'user_key'] }),
  project_user: portalContract('project_user', 'user_key', PROJECT_USER_PROFILE_FIELDS, { expectedFirebaseCollection: 'project_user', strictFields: true, dynamicFields: true, forbiddenFields: ['group_key', 'position_key', 'user_password_hash', 'password', 'plaintext_password'], requirePending: true, authIdentityField: 'firebase_uid', rejectDisabled: true, ackPreserveColumns: [], requiredFields: ['user_key', 'firebase_uid', 'firebase_collection', 'mysql_sync_status'], requiredNonEmptyFields: [] }),
  project_user_login_history: portalContract('project_user_login_history', 'user_login_history_key', PROJECT_USER_LOGIN_HISTORY_FIELDS, { expectedFirebaseCollection: 'project_user_login_history', strictFields: true, requirePending: true, ackPreserveColumns: [], requiredFields: ['user_login_history_key', 'project_key', 'user_action', 'user_action_status', 'user_action_at', 'firebase_collection', ...PORTAL_LIFECYCLE_FIELDS.map((field) => field.name)], requiredNonEmptyFields: ['project_key', 'user_action', 'user_action_status'] }),
})

export function collectionContract(name) {
  const contract = COLLECTIONS[name]
  if (!contract) throw new Error('collection_not_allowlisted')
  return contract
}

export function validIdentifier(name) {
  return typeof name === 'string' && /^[a-z][a-z0-9_]{0,62}$/.test(name)
}

export function quoteIdentifier(name) {
  if (!validIdentifier(name)) throw new Error('identifier_invalid')
  return `\`${name}\``
}
