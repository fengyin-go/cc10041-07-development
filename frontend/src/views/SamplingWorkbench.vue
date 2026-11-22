<template>
  <section class="sampling-panel" data-testid="sampling-workbench" :data-module="activeMeta.key">
    <header class="sampling-head">
      <div>
        <h3>采样工作台</h3>
        <p class="page-desc">
          归属清单按「表名 + 所属业务域」核对。表名只是名字，读取范围只认可信调用身份。
        </p>
      </div>
      <button class="btn" type="button" data-testid="sampling-refresh" @click="refresh">刷新核对</button>
    </header>

    <form class="sampling-identity" data-testid="sampling-identity" @submit.prevent="refresh">
      <label>
        <span>可信业务域</span>
        <select v-model="identity.domain" data-testid="trusted-domain">
          <option v-for="domain in domainOptions" :key="domain" :value="domain">{{ domain }}</option>
        </select>
      </label>
      <label>
        <span>可信身份</span>
        <input v-model="identity.principal" data-testid="trusted-principal" />
      </label>
      <label>
        <span>旧采样配置</span>
        <input v-model.number="configId" type="number" min="1" data-testid="sampling-config-id" />
      </label>
      <label>
        <span>采样比例</span>
        <input v-model.number="sampleRatio" type="number" min="0" max="1" step="0.1" data-testid="sampling-ratio" />
      </label>
      <button class="btn" type="submit" data-testid="sampling-load-scope">查看两域范围</button>
    </form>

    <p v-if="message" class="sampling-message" :class="{ error: isError }" data-testid="sampling-message">
      {{ message }}
    </p>

    <article v-if="scope" class="sampling-card" data-testid="scope-card">
      <h4>旧配置「{{ scope.config_name }}」的范围</h4>
      <p class="sampling-hint" data-testid="scope-summary">
        目标表 <strong>{{ scope.target_table }}</strong>
        · 配置未存业务域：{{ scope.has_stored_domain ? '否' : '是' }}
        · 归属清单版本 {{ scope.manifest_version }}
      </p>
      <table class="data-table">
        <thead>
          <tr>
            <th>业务域</th>
            <th>归属状态</th>
            <th>当前可信身份可见</th>
            <th>分区 / 状态 / 样本行</th>
            <th>动作</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="domain in scope.domains"
            :key="domain.domain"
            :data-testid="`scope-domain-${domain.domain}`"
            :data-visible="domain.visible"
          >
            <td>{{ domain.domain }}</td>
            <td>{{ domain.ownership_status }}</td>
            <td>{{ domain.visible ? '是' : '否（他域只给归属状态）' }}</td>
            <td>
              <template v-if="domain.visible">
                <div
                  v-for="part in domain.partitions"
                  :key="part.partition_id"
                  :data-testid="`partition-${part.partition_id}`"
                >
                  {{ part.name }} · {{ part.status }} · {{ part.sample_count }} 行
                </div>
              </template>
              <span v-else class="sampling-muted">{{ domain.note }}</span>
            </td>
            <td class="row-actions">
              <template v-if="domain.visible && domain.claim_id">
                <button
                  v-if="domain.ownership_status !== '已确认'"
                  class="link"
                  type="button"
                  :data-testid="`confirm-claim-${domain.claim_id}`"
                  @click="confirmDomain(domain)"
                >
                  确认归属
                </button>
                <template v-for="part in domain.partitions" :key="`act-${part.partition_id}`">
                  <button
                    v-if="part.status !== '已核准'"
                    class="link"
                    type="button"
                    :data-testid="`approve-partition-${part.partition_id}`"
                    @click="approve(part)"
                  >
                    核准 {{ part.name }}
                  </button>
                  <button
                    v-if="part.status === '已核准'"
                    class="link"
                    type="button"
                    :data-testid="`revoke-partition-${part.partition_id}`"
                    @click="revoke(part)"
                  >
                    撤回 {{ part.name }}
                  </button>
                </template>
              </template>
            </td>
          </tr>
        </tbody>
      </table>
      <p v-if="scope.reason" class="sampling-muted" data-testid="scope-reason">{{ scope.reason }}</p>
    </article>

    <article class="sampling-card">
      <h4>执行采样</h4>
      <p class="sampling-hint">
        排队只记录可信域身份；真正取数在交付前重新读取归属清单，撤回的分区不会读。
      </p>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="queue-sampling" @click="queue">
          排队一笔采样
        </button>
        <label class="sampling-inline">
          <span>请求号</span>
          <input v-model.number="latestRequestId" type="number" min="1" data-testid="sampling-request-id" />
        </label>
        <button
          class="btn"
          type="button"
          data-testid="execute-sampling"
          :disabled="latestRequestId === null"
          @click="execute"
        >
          执行排队采样
        </button>
        <button
          class="btn"
          type="button"
          data-testid="check-delivery"
          :disabled="latestRequestId === null"
          @click="checkDelivery"
        >
          查询交付
        </button>
      </div>
      <p v-if="latestRequestId !== null" class="sampling-muted" data-testid="latest-request">
        当前采样请求 #{{ latestRequestId }}
      </p>
    </article>

    <article v-if="delivery" class="sampling-card" data-testid="delivery-card">
      <h4>交付结果 · 请求 #{{ delivery.request_id }}</h4>
      <p data-testid="delivery-summary">
        是否交付：<strong>{{ delivery.delivered ? '是' : '否' }}</strong>
        · 状态：{{ delivery.status }}
        · 交付行数：{{ delivery.row_count ?? '未公开' }}
        · 可追溯来源：{{ delivery.can_trace_sources ? '是' : '否' }}
      </p>
      <p class="sampling-muted" data-testid="delivery-reason">{{ delivery.reason }}</p>
      <table v-if="delivery.rows.length" class="data-table" data-testid="delivery-rows">
        <thead>
          <tr>
            <th>样本行</th>
            <th>业务域</th>
            <th>分区</th>
            <th>真实来源</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in delivery.rows" :key="row.row_id" :data-testid="`delivered-row-${row.row_id}`">
            <td>{{ row.row_id }}</td>
            <td>{{ row.domain }}</td>
            <td>{{ row.partition }}</td>
            <td><code>{{ row.source }}</code></td>
          </tr>
        </tbody>
      </table>
      <p v-else class="sampling-muted" data-testid="delivery-empty">
        本次没有返回任何样本行，核对依据仍保留在请求记录里。
      </p>
    </article>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'

import type { ModuleMeta } from '@/data/types'
import type { Delivery, LegacyScope, ScopeDomain, ScopePartition } from '@/api/sampling-service'
import {
  approvePartition,
  confirmClaim,
  executeSampling,
  listRequests,
  loadDelivery,
  loadLegacyScope,
  queueSampling,
  revokePartition,
} from '@/api/sampling-service'

const props = defineProps<{ meta: ModuleMeta }>()
const activeMeta = props.meta

const domainOptions = ['销售', '履约']
const identity = reactive({ domain: '销售', principal: '销售维护人' })
const configId = ref(1)
const sampleRatio = ref(1)
const scope = ref<LegacyScope | null>(null)
const delivery = ref<Delivery | null>(null)
const latestRequestId = ref<number | null>(null)
const message = ref('')
const isError = ref(false)

function setMessage(text: string, error = false) {
  message.value = text
  isError.value = error
}

async function refresh() {
  setMessage('')
  try {
    scope.value = await loadLegacyScope(identity, configId.value)
    if (!scope.value.authorized) {
      setMessage(scope.value.reason || '归属判定未明确，已保留核对依据', true)
    }
  } catch (error) {
    setMessage(error instanceof Error ? error.message : '范围读取失败', true)
  }
}

async function confirmDomain(domain: ScopeDomain) {
  if (!domain.claim_id) return
  try {
    const result = await confirmClaim(identity, domain.claim_id)
    setMessage(result.message)
    await refresh()
  } catch (error) {
    setMessage(error instanceof Error ? error.message : '确认归属失败', true)
  }
}

async function approve(part: ScopePartition) {
  try {
    const result = await approvePartition(identity, part.partition_id)
    setMessage(result.message)
    await refresh()
  } catch (error) {
    setMessage(error instanceof Error ? error.message : '核准分区失败', true)
  }
}

async function revoke(part: ScopePartition) {
  try {
    const result = await revokePartition(identity, part.partition_id)
    setMessage(result.message)
    await refresh()
  } catch (error) {
    setMessage(error instanceof Error ? error.message : '撤回分区失败', true)
  }
}

async function queue() {
  try {
    const result = await queueSampling(identity, configId.value, sampleRatio.value)
    latestRequestId.value = result.request ? result.request.request_id : null
    delivery.value = null
    setMessage(result.message, !result.ok)
  } catch (error) {
    setMessage(error instanceof Error ? error.message : '采样排队失败', true)
  }
}

async function execute() {
  if (latestRequestId.value === null) return
  try {
    const result = await executeSampling(identity, latestRequestId.value)
    setMessage(result.message, !result.ok)
    await checkDelivery()
  } catch (error) {
    setMessage(error instanceof Error ? error.message : '执行采样失败', true)
  }
}

async function checkDelivery() {
  if (latestRequestId.value === null) return
  try {
    delivery.value = await loadDelivery(identity, latestRequestId.value)
    if (!delivery.value.visible) {
      setMessage(delivery.value.reason, true)
    }
  } catch (error) {
    setMessage(error instanceof Error ? error.message : '交付结果读取失败', true)
  }
}

onMounted(async () => {
  await refresh()
  try {
    const requests = await listRequests(identity)
    if (requests.length) latestRequestId.value = requests[requests.length - 1].request_id
  } catch {
    // 请求列表读取失败不影响范围核对。
  }
})
</script>

<style scoped>
.sampling-panel { margin-top: 24px; border-top: 2px solid var(--border); padding-top: 16px; }
.sampling-head { display: flex; justify-content: space-between; align-items: flex-start; }
.sampling-identity { display: flex; flex-wrap: wrap; gap: 10px; align-items: flex-end; margin: 12px 0; }
.sampling-identity label { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--muted); }
.sampling-identity input, .sampling-identity select { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; }
.sampling-card { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px 14px; margin-bottom: 12px; }
.sampling-card h4 { margin: 0 0 8px; }
.sampling-hint { font-size: 12px; color: var(--muted); margin: 0 0 8px; }
.sampling-muted { font-size: 12px; color: var(--muted); }
.sampling-message { font-size: 13px; color: #1f6feb; }
.sampling-message.error { color: #b42318; }
.sampling-inline { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--muted); }
.sampling-inline input { width: 100px; padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; }
</style>
