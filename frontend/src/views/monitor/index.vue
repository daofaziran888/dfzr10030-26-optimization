<template>
  <section class="page" data-module="monitor">
    <header class="page-head">
      <div><h2>采集监控</h2><p class="page-desc">监控分析分批执行：冻结样本集合后按固定单批容量拆分，分组完成即落库，可停止、续跑并按批次编号回查。</p></div>
    </header>
    <div class="analysis-summary">
      <article v-for="m in summaryMetrics" :key="m.label" class="analysis-card" data-testid="kpi-card">
        <span style="font-size:12px;color:var(--muted)">{{ m.label }}</span>
        <strong style="font-size:22px;display:block">{{ m.value }}</strong>
        <span :class="m.trend > 0 ? 'trend-up' : 'trend-down'" style="font-size:12px">{{ m.trend > 0 ? '↑' : '↓' }}{{ Math.abs(m.trend) }}%</span>
      </article>
    </div>

    <form class="analysis-form" @submit.prevent="submitBatch">
      <label><span>面板编号</span>
        <select v-model="form['面板编号']" data-testid="batch-panel">
          <option value="MONI-0001">MONI-0001</option>
          <option value="MONI-0002">MONI-0002</option>
          <option value="MONI-0003">MON-0003（验收面板）</option>
        </select>
      </label>
      <label><span>样本数量</span><input v-model="form['样本范围']" data-testid="batch-sample-count" placeholder="如 12" /></label>
      <label><span>单批容量</span><input v-model="form['单批容量']" data-testid="batch-size" placeholder="固定每批样本数，如 5" /></label>
      <label><span>并发事件策略</span>
        <select v-model="form['冲突策略']" data-testid="batch-policy">
          <option value="freeze">冻结当前样本集合，新事件进入下一批次</option>
          <option value="new_batch">未处理分组改用新样本集合，已完成分组保留</option>
        </select>
      </label>
      <button class="btn primary" type="submit" data-testid="run-analysis">提交分析批次</button>
      <button class="btn" type="button" data-testid="refresh-batches" @click="loadBatches">刷新批次清单</button>
    </form>

    <p v-if="message" class="batch-message" :data-testid="'batch-message'">{{ message }}</p>

    <div v-if="current" class="batch-panel" data-testid="batch-detail">
      <div class="batch-head">
        <div>
          <strong>批次 {{ current.batch_no }}</strong>
          <span class="batch-tag" :data-status="current.status">{{ current.status }}</span>
          <span class="batch-tag">样本版本 v{{ current.sample_version }}</span>
          <span class="batch-tag">策略：{{ current.conflict_policy === 'freeze' ? '冻结' : '新样本集合' }}</span>
        </div>
        <div class="batch-actions">
          <button class="btn primary" data-testid="execute-batch" @click="executeChunk">执行下一分组</button>
          <button class="btn" data-testid="execute-all" @click="executeAll">执行全部分组</button>
          <button class="btn" data-testid="stop-batch" @click="stopBatch">停止批次</button>
        </div>
      </div>
      <p class="batch-progress" data-testid="batch-progress">
        分组进度 {{ current.progress.已完成分组 }}/{{ current.progress.总分组数 }}
        （待处理 {{ current.progress.待处理分组 }} · 已停止 {{ current.progress.已停止分组 }}）
        ｜样本 {{ current.sample_count }} 个，单批容量 {{ current.batch_size }}
      </p>

      <h4>监控分析批次清单与指标快照</h4>
      <table class="data-table" data-testid="chunk-table">
        <thead><tr><th>分组编号</th><th>分组标签</th><th>样本区间</th><th>样本数</th><th>样本版本</th><th>状态</th><th>成功率</th><th>平均延迟</th><th>完成时间</th></tr></thead>
        <tbody>
          <tr v-for="c in current.chunks" :key="c.chunk_id" :data-testid="'chunk-row-' + c.chunk_no">
            <td>{{ c.chunk_id }}</td>
            <td>{{ c.分组标签.join('、') }}</td>
            <td>{{ c.样本区间[0] }}-{{ c.样本区间[1] }}</td>
            <td>{{ c.样本数 }}</td>
            <td>v{{ c.sample_version }}</td>
            <td><span class="batch-tag" :data-status="c.status">{{ c.status }}</span></td>
            <td>{{ c.result ? c.result.aggregated.成功率 : '—' }}</td>
            <td>{{ c.result ? c.result.平均延迟 : '—' }}</td>
            <td>{{ c.finished_at || '—' }}</td>
          </tr>
        </tbody>
      </table>

      <h4>监控指标快照（批次提交时冻结）</h4>
      <table class="data-table" data-testid="snapshot-table">
        <thead><tr><th v-for="k in snapshotKeys" :key="k">{{ k }}</th></tr></thead>
        <tbody><tr><td v-for="k in snapshotKeys" :key="k">{{ current.metric_snapshot[k] }}</td></tr></tbody>
      </table>

      <div v-if="groupResults.length" class="analysis-chart" data-testid="batch-chart">
        <div v-for="bar in chartData" :key="bar.label" class="analysis-bar" :style="{ height: barHeight(bar.value) + 'px' }"><span>{{ bar.label }}</span></div>
      </div>
      <table class="data-table" v-if="groupResults.length" data-testid="result-table">
        <thead><tr><th v-for="col in resultColumns" :key="col">{{ col }}</th></tr></thead>
        <tbody>
          <tr v-for="row in groupResults" :key="row.sample_id + row.id">
            <td v-for="col in resultColumns" :key="col">{{ row[col] ?? '—' }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <h4>历史批次（同一面板可按批次编号回查，历史状态保留）</h4>
    <table class="data-table" data-testid="batch-list">
      <thead><tr><th>批次编号</th><th>面板</th><th>状态</th><th>样本版本</th><th>分组进度</th><th>提交时间</th><th>操作</th></tr></thead>
      <tbody>
        <tr v-for="b in batches" :key="b.batch_no">
          <td>{{ b.batch_no }}</td>
          <td>{{ b.panel_code }}</td>
          <td><span class="batch-tag" :data-status="b.status">{{ b.status }}</span></td>
          <td>v{{ b.sample_version }}</td>
          <td>{{ b.progress.已完成分组 }}/{{ b.progress.总分组数 }}</td>
          <td>{{ b.submitted_at }}</td>
          <td><button class="btn ghost" data-testid="reenter-batch" @click="reenter(b.batch_no)">重新进入</button></td>
        </tr>
      </tbody>
    </table>

    <form class="analysis-form" @submit.prevent="recordEvent">
      <label><span>新增采集事件（并发模拟）</span><input v-model="eventForm['事件数量']" data-testid="event-count" placeholder="新增样本数，如 3" /></label>
      <input type="hidden" v-model="eventForm['面板编号']" />
      <button class="btn primary" type="submit" data-testid="record-event">提交采集事件</button>
    </form>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { request } from '@/api/client'

const ENDPOINT = '/api/monitor'
interface Chunk { chunk_id: string; chunk_no: number; 分组标签: string[]; 样本区间: number[]; 样本数: number; sample_version: number; status: string; finished_at: string | null; result: any }
interface Batch { batch_no: string; panel_code: string; status: string; conflict_policy: string; batch_size: number; sample_count: number; sample_version: number; submitted_at: string; progress: Record<string, number>; chunks: Chunk[]; metric_snapshot: Record<string, any> }

const summaryMetrics = ref<{ label: string; value: number; trend: number }[]>([])
const batches = ref<Batch[]>([])
const current = ref<Batch | null>(null)
const message = ref('')
const form = ref<Record<string, string>>({ 面板编号: 'MONI-0003', 样本范围: '12', 单批容量: '5', 冲突策略: 'freeze' })
const eventForm = ref<Record<string, string>>({ 面板编号: 'MONI-0003', 事件数量: '3' })
const resultColumns = ['sample_id', '分组', '指标值', '对比基准', '差异', '成功率', '平均延迟']
const snapshotKeys = ['面板编号', '采集源总数', '今日采集量', '成功率', '平均延迟', '失败重试', '异常告警', '更新时间', 'snapshot_at']

const groupResults = computed(() => {
  if (!current.value) return []
  return current.value.chunks.filter(c => c.result).flatMap(c => c.result.items)
})
const chartData = computed(() => current.value?.chunks.filter(c => c.result).map(c => ({
  label: c.chunk_id.slice(-3), value: c.result!.aggregated.指标值,
})) || [])
function barHeight(v: number) { const max = Math.max(...chartData.value.map(d => d.value), 0.0001); return Math.max(10, (v / max) * 140) }

async function fetchSummary() {
  try { const res = await request(`${ENDPOINT}/summary`); summaryMetrics.value = (await res.json()).metrics || [] } catch { /* 看板指标失败不阻塞批次功能 */ }
}
async function loadBatches() {
  const res = await request(`${ENDPOINT}/batches?panel=${encodeURIComponent(form.value['面板编号'])}`)
  batches.value = (await res.json()).items || []
  if (current.value) await reenter(current.value.batch_no)
}
async function reenter(batchNo: string) {
  const res = await request(`${ENDPOINT}/batches/${batchNo}`)
  current.value = await res.json()
  message.value = `已按批次编号 ${batchNo} 回查，监控页与报告页重新进入看到相同结果`
}
async function submitBatch() {
  eventForm.value['面板编号'] = form.value['面板编号']
  const res = await request(`${ENDPOINT}/batches`, { method: 'POST', body: JSON.stringify({ values: form.value }) })
  const data = await res.json()
  if (!res.ok) { message.value = data.detail || '提交失败'; return }
  message.value = `${data.reason}：${data.batch.batch_no}`
  current.value = data.batch
  // 提交后自动跑完所有分组：每个分组仍单独落库，已完成分组不会重复计算
  const execRes = await request(`${ENDPOINT}/batches/${data.batch.batch_no}/execute`, { method: 'POST', body: JSON.stringify({ values: {} }) })
  const execData = await execRes.json()
  message.value += `；本次执行 ${execData.processed} 个分组，复用已完成分组 ${data.batch.progress.已完成分组} 个`
  await loadBatches()
}
async function executeChunk() {
  if (!current.value) return
  const res = await request(`${ENDPOINT}/batches/${current.value.batch_no}/execute`, { method: 'POST', body: JSON.stringify({ values: { limit: 1 } }) })
  const data = await res.json()
  message.value = data.reason || `本次处理 ${data.processed} 个分组`
  await loadBatches()
}
async function executeAll() {
  if (!current.value) return
  const res = await request(`${ENDPOINT}/batches/${current.value.batch_no}/execute`, { method: 'POST', body: JSON.stringify({ values: {} }) })
  const data = await res.json()
  message.value = `已处理 ${data.processed} 个分组，已完成分组不重复计算`
  await loadBatches()
}
async function stopBatch() {
  if (!current.value) return
  const res = await request(`${ENDPOINT}/batches/${current.value.batch_no}/stop`, { method: 'POST', body: JSON.stringify({ values: {} }) })
  const data = await res.json()
  message.value = data.reason
  await loadBatches()
}
async function recordEvent() {
  eventForm.value['面板编号'] = form.value['面板编号']
  const res = await request(`${ENDPOINT}/events`, { method: 'POST', body: JSON.stringify({ values: eventForm.value }) })
  const data = await res.json()
  if (!res.ok) { message.value = data.detail || '事件提交失败'; return }
  message.value = `采集事件 ${data.event_id}：${data.处理方式}`
  await loadBatches()
}
onMounted(async () => { await fetchSummary(); await loadBatches() })
</script>
<style scoped>
.batch-panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 14px; margin: 12px 0; }
.batch-head { display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 8px; }
.batch-actions { display: flex; gap: 8px; }
.batch-tag { display: inline-block; margin-left: 6px; padding: 2px 8px; border-radius: 10px; font-size: 12px; background: #eef2ff; color: #3730a3; }
.batch-tag[data-status='已完成'] { background: #dcfce7; color: #166534; }
.batch-tag[data-status='执行中'] { background: #fef9c3; color: #854d0e; }
.batch-tag[data-status='已停止'] { background: #fee2e2; color: #991b1b; }
.batch-progress { color: var(--muted); font-size: 13px; }
.batch-message { color: #1d4ed8; font-size: 13px; }
h4 { margin: 14px 0 6px; }
</style>
