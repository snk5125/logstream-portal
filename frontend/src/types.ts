export interface User {
  id: string
  display_name: string
  team: string
  role: 'consumer' | 'admin'
  account_scope?: string | null
}

export interface SubRef {
  stream_id: number
  stream_name: string
  status: string
}

export interface Source {
  fqn: string
  name: string
  log_type: string
  sensitivity: 'standard' | 'sensitive'
  est_volume_per_min: number
  description: string
  columns: { name: string; type: string }[]
  subscriptions: SubRef[]
  origin?: string
}

export interface Workload {
  name: string
  schema: string
  environment: string
  sources: Source[]
}

export interface Account {
  account_id: string
  account_alias: string
  workloads: Workload[]
}

export interface Catalog {
  as_of: string
  stale: boolean
  accounts: Account[]
}

export interface StreamSource {
  id: number
  source_fqn: string
  workload: string
  source_name: string
  status: 'active' | 'pending_approval' | 'rejected'
}

export interface Stream {
  id: number
  name: string
  type: 'kinesis' | 'sqs'
  status: 'provisioning' | 'live' | 'error' | 'deleted'
  last_error?: string | null
  sources: StreamSource[]
  flow?: { recent_records: number } | null
  read_role_arn?: string | null
  consumer_account_id?: string | null
}

export interface Approval {
  id: number
  stream_id: number
  stream_name: string
  source_fqn: string
  requested_by: string
  requested_at: string
}
