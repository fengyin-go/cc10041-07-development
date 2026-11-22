/** 采样工作台请求封装：可信身份通过网关头传递，绝不写进请求体。 */
import { request } from './client'

export type TrustedIdentity = {
  domain: string
  principal: string
}

export type ScopePartition = {
  partition_id: number
  name: string
  status: string
  sample_count: number
}

export type ScopeDomain = {
  domain: string
  table: string
  visible: boolean
  ownership_status: string
  owner: string | null
  claim_id: number | null
  partitions: ScopePartition[]
  note: string
}

export type LegacyScope = {
  config_id: number
  config_name: string
  target_table: string
  sample_ratio: string
  sample_method: string
  has_stored_domain: boolean
  trusted: { trusted: boolean; domain: string | null; principal: string | null }
  authorized: boolean
  domains: ScopeDomain[]
  same_name_domains: string[]
  reason: string
  manifest_version: number
}

export type DeliveredRow = {
  row_id: string
  values: Record<string, string | number | boolean>
  domain: string
  table: string
  partition: string
  source: string
}

export type Delivery = {
  request_id: number
  domain: string | null
  target_table: string
  delivered: boolean
  status: string
  visible: boolean
  row_count: number | null
  row_ids: string[]
  rows: DeliveredRow[]
  lineage: { row_id: string; source: string; partition: string }[]
  can_trace_sources: boolean
  reason: string
  manifest_version: number
}

export type SamplingRequest = {
  request_id: number
  config_id: number | null
  config_name: string
  target_table: string
  sample_ratio: number
  domain: string | null
  status: string
  delivered: boolean
  reason: string
  row_count: number | null
  row_ids: string[]
  rows_visible: boolean
}

function identityHeaders(identity: TrustedIdentity): Record<string, string> {
  return {
    'X-Trusted-Domain': identity.domain,
    'X-Trusted-Principal': identity.principal,
  }
}

async function read<T>(response: Response, fallback: string): Promise<T> {
  const payload = await response.json().catch(() => null)
  if (!response.ok) {
    const detail = payload && typeof payload === 'object' ? payload.detail : null
    const text =
      typeof detail === 'string'
        ? detail
        : detail && typeof detail === 'object' && 'message' in detail
          ? String((detail as { message?: string }).message)
          : fallback
    throw new Error(text)
  }
  return payload as T
}

export async function loadLegacyScope(
  identity: TrustedIdentity,
  configId: number,
): Promise<LegacyScope> {
  const response = await request(`/api/sampling/legacy-scope?config_id=${configId}`, {
    headers: identityHeaders(identity),
  })
  return read<LegacyScope>(response, '旧配置范围读取失败')
}

export async function loadManifest(identity: TrustedIdentity) {
  const response = await request('/api/sampling/manifest', {
    headers: identityHeaders(identity),
  })
  return read<{ manifest_version: number; authorized: boolean; claims: any[] }>(
    response,
    '归属清单读取失败',
  )
}

export async function confirmClaim(identity: TrustedIdentity, claimId: number) {
  const response = await request(`/api/sampling/claims/${claimId}/confirm`, {
    method: 'POST',
    headers: identityHeaders(identity),
    body: JSON.stringify({}),
  })
  return read<{ ok: boolean; message: string }>(response, '确认归属失败')
}

export async function approvePartition(identity: TrustedIdentity, partitionId: number) {
  const response = await request(`/api/sampling/partitions/${partitionId}/approve`, {
    method: 'POST',
    headers: identityHeaders(identity),
    body: JSON.stringify({}),
  })
  return read<{ ok: boolean; message: string }>(response, '核准分区失败')
}

export async function revokePartition(identity: TrustedIdentity, partitionId: number) {
  const response = await request(`/api/sampling/partitions/${partitionId}/revoke`, {
    method: 'POST',
    headers: identityHeaders(identity),
    body: JSON.stringify({ note: '工作台撤回' }),
  })
  return read<{ ok: boolean; message: string }>(response, '撤回分区失败')
}

export async function queueSampling(
  identity: TrustedIdentity,
  configId: number,
  sampleRatio: number,
): Promise<{ ok: boolean; message: string; request: SamplingRequest | null }> {
  const response = await request('/api/sampling/requests', {
    method: 'POST',
    headers: identityHeaders(identity),
    body: JSON.stringify({ config_id: configId, sample_ratio: sampleRatio }),
  })
  return read(response, '采样排队失败')
}

export async function executeSampling(identity: TrustedIdentity, requestId: number) {
  const response = await request(`/api/sampling/requests/${requestId}/execute`, {
    method: 'POST',
    headers: identityHeaders(identity),
    body: JSON.stringify({}),
  })
  return read<{ ok: boolean; message: string }>(response, '执行采样失败')
}

export async function loadDelivery(
  identity: TrustedIdentity,
  requestId: number,
): Promise<Delivery> {
  const response = await request(`/api/sampling/requests/${requestId}/delivery`, {
    headers: identityHeaders(identity),
  })
  return read<Delivery>(response, '交付结果读取失败')
}

export async function listRequests(identity: TrustedIdentity): Promise<SamplingRequest[]> {
  const response = await request('/api/sampling/requests', {
    headers: identityHeaders(identity),
  })
  const payload = await read<{ items: SamplingRequest[] }>(response, '采样请求读取失败')
  return payload.items
}
