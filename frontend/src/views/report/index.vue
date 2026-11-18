<template>
  <section class="page" data-module="report">
    <header class="page-head">
      <div><h2>采集报告</h2><p class="page-desc">读取监控分析批次清单，按批次汇总成功率与平均延迟并生成待复核结果；同一批次编号在监控页与报告页回查结果一致。</p></div>
    </header>
    <div class="analysis-summary">
      <article v-for="m in summaryMetrics" :key="m.label" class="analysis-card" data-testid="kpi-card">
        <span style="font-size:12px;color:var(--muted)">{{ m.label }}</span>
        <strong style="font-size:22px;display:block">{{ m.value }}</strong>
        <span :class="m.trend > 0 ? 'trend-up' : 'trend-down'" style="font-size:12px">{{ m.trend > 0 ? '↑' : '↓' }}{{ Math.abs(m.trend) }}%</span>
      </article>
    </div>

    <form class="analysis-form" @submit.prevent="runAutoReview">
      <label><span>面板编号</span>
        <select v-model="panel" data-testid="review-panel">
          <option value="MONI-0001">MONI-0001</option>
          <option value="MONI-0002">MONI-0002</option>
          <option value="MONI-0003">MON-0003（验收面板）</option>
        </select>
      </label>
      <button class="btn primary" type="submit" data-testid="run-analysis">运行分析（汇总最新批次）</button>
      <button class="btn" type="button" data-testid="refresh-batches" @click="loadAll">刷新批次与复核清单</button>
    </form>
    <p v-if="message" class="batch-message" data-testid="review-message">{{ message }}</p>

    <h4>监控分析批次清单（来自采集监控模块）</h4>
    <table class="data-table" data-testid="monitor-batch-table">
      <thead><tr><th>批次编号</th><th>面板</th><th>批次状态</th><th>样本版本</th><th>分组进度</th><th>提交时间</th><th>操作</th></tr></thead>
      <tbody>
        <tr v-for="b in monitorBatches" :key="b.batch_no">
          <td>{{ b.batch_no }}</td>
          <td>{{ b.panel_code }}</td>
          <td><span class="batch-tag" :data-status="b.status">{{ b.status }}</span></td>
          <td>v{{ b.sample_version }}</td>
          <td>{{ b.progress.已完成分组 }}/{{ b.progress.总分组数 }}</td>
          <td>{{ b.submitted_at }}</td>
          <td>
            <button class="btn primary" data-testid="summarize-batch" :disabled="b.progress.已完成分组 === 0"
                    @click="summarize(b.batch_no)">生成待复核结果</button>
            <button class="btn ghost" data-testid="view-batch" @click="viewBatch(b.batch_no)">查看批次清单</button>
          </td>
        </tr>
      </tbody>
    </table>
    <p v-if="!monitorBatches.length" style="color:var(--muted)">该面板暂无监控分析批次，请先在采集监控页提交。</p>

    <div v-if="currentBatch" class="batch-panel" data-testid="batch-detail">
      <h4>批次 {{ currentBatch.batch_no }} 的分组清单（与监控页同一编号同一结果）</h4>
      <table class="data-table">
        <thead><tr><th>分组编号</th><th>样本区间</th><th>样本版本</th><th>状态</th><th>成功率</th><th>平均延迟</th></tr></thead>
        <tbody>
          <tr v-for="c in currentBatch.chunks" :key="c.chunk_id">
            <td>{{ c.chunk_id }}</td>
            <td>{{ c.样本区间[0] }}-{{ c.样本区间[1] }}</td>
            <td>v{{ c.sample_version }}</td>
            <td><span class="batch-tag" :data-status="c.status">{{ c.status }}</span></td>
            <td>{{ c.result ? c.result.aggregated.成功率 : '—' }}</td>
            <td>{{ c.result ? c.result.aggregated.平均延迟 : '—' }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <h4>待复核结果（按批次汇总成功率与平均延迟）</h4>
    <table class="data-table" data-testid="review-table">
      <thead><tr><th>复核编号</th><th>批次编号</th><th>复核状态</th><th>已完成分组</th><th>总样本数</th><th>成功率</th><th>平均延迟</th><th>平均指标值</th><th>生成时间</th><th>操作</th></tr></thead>
      <tbody>
        <tr v-for="r in reviews" :key="r.复核编号">
          <td>{{ r.复核编号 }}</td>
          <td>{{ r.批次编号 }}</td>
          <td><span class="batch-tag">{{ r.复核状态 }}</span></td>
          <td>{{ r.已完成分组 }}/{{ r.总分组数 }}</td>
          <td>{{ r.总样本数 }}</td>
          <td>{{ r.成功率 }}</td>
          <td>{{ r.平均延迟 }}</td>
          <td>{{ r.平均指标值 }}</td>
          <td>{{ r.生成时间 }}</td>
          <td><button class="btn ghost" data-testid="open-review" @click="openReview(r.批次编号)">查看明细</button></td>
        </tr>
      </tbody>
    </table>

    <div v-if="currentReview" class="batch-panel" data-testid="review-detail">
      <h4>复核明细 {{ currentReview.复核编号 }} ｜ 批次 {{ currentReview.批次编号 }}</h4>
      <table class="data-table">
        <thead><tr><th>分组</th><th>来源批次</th><th>样本数</th><th>成功率</th><th>平均延迟</th><th>指标值</th></tr></thead>
        <tbody>
          <tr v-for="row in currentReview.明细" :key="row.chunk_id">
            <td>{{ row.分组 }}</td>
            <td>{{ row.来源批次 }}</td>
            <td>{{ row.样本数 }}</td>
            <td>{{ row.成功率 }}</td>
            <td>{{ row.平均延迟 }}</td>
            <td>{{ row.指标值 }}</td>
          </tr>
        </tbody>
      </table>
      <div class="analysis-chart" data-testid="review-chart">
        <div v-for="bar in reviewChart" :key="bar.label" class="analysis-bar" :style="{ height: barHeight(bar.value) + 'px' }"><span>{{ bar.label }}</span></div>
      </div>
    </div>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { request } from '@/api/client'

const ENDPOINT = '/api/report'
const MONITOR = '/api/monitor'
const panel = ref('MONI-0003')
const summaryMetrics = ref<{ label: string; value: number; trend: number }[]>([])
const monitorBatches = ref<any[]>([])
const reviews = ref<any[]>([])
const currentBatch = ref<any | null>(null)
const currentReview = ref<any | null>(null)
const message = ref('')

const reviewChart = computed(() => (currentReview.value?.明细 || []).map((row: any) => ({
  label: String(row.分组).slice(0, 6), value: Number(row.成功率) || 0,
})))
function barHeight(v: number) { const max = Math.max(...reviewChart.value.map((d: { value: number }) => d.value), 0.0001); return Math.max(10, (v / max) * 140) }

async function fetchSummary() {
  try { const res = await request(`${ENDPOINT}/summary`); summaryMetrics.value = (await res.json()).metrics || [] } catch { /* 不阻塞复核功能 */ }
}
async function loadAll() {
  const [bRes, rRes] = await Promise.all([
    request(`${MONITOR}/batches?panel=${encodeURIComponent(panel.value)}`),
    request(`${ENDPOINT}/batch-reviews?panel=${encodeURIComponent(panel.value)}`),
  ])
  monitorBatches.value = (await bRes.json()).items || []
  reviews.value = (await rRes.json()).items || []
  message.value = `已读取 ${monitorBatches.value.length} 个监控批次、${reviews.value.length} 份复核结果`
}
async function runAutoReview() {
  await loadAll()
  const target = monitorBatches.value.find((b: any) => b.progress.已完成分组 > 0)
  if (!target) {
    message.value = '该面板暂无含已完成分组的监控批次，请先在采集监控页提交分析'
    currentReview.value = null
    return
  }
  await summarize(target.batch_no)
}
async function summarize(batchNo: string) {
  const res = await request(`${ENDPOINT}/batch-reviews`, { method: 'POST', body: JSON.stringify({ values: { batch_no: batchNo } }) })
  const data = await res.json()
  if (!res.ok) { message.value = data.detail || '复核生成失败'; return }
  message.value = `${data.reason}：${data.review.复核编号}`
  await loadAll()
  await openReview(batchNo)
}
async function openReview(batchNo: string) {
  const res = await request(`${ENDPOINT}/batch-reviews/${batchNo}`)
  if (res.ok) currentReview.value = await res.json()
}
async function viewBatch(batchNo: string) {
  const res = await request(`${MONITOR}/batches/${batchNo}`)
  currentBatch.value = await res.json()
  message.value = `批次 ${batchNo} 清单与监控页结果一致`
}
onMounted(async () => { await fetchSummary(); await loadAll() })
</script>
<style scoped>
.batch-panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 14px; margin: 12px 0; }
.batch-tag { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 12px; background: #eef2ff; color: #3730a3; }
.batch-tag[data-status='已完成'] { background: #dcfce7; color: #166534; }
.batch-tag[data-status='执行中'] { background: #fef9c3; color: #854d0e; }
.batch-tag[data-status='已停止'] { background: #fee2e2; color: #991b1b; }
.batch-message { color: #1d4ed8; font-size: 13px; }
h4 { margin: 14px 0 6px; }
</style>
