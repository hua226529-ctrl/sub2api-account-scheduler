<script setup lang="ts">
import type { ScorePolicyVersion } from "../../types";

defineProps<{ policies: ScorePolicyVersion[]; optimizerMode: string }>();
defineEmits<{ activate: [policy: ScorePolicyVersion]; reject: [policy: ScorePolicyVersion]; rollback: [policy: ScorePolicyVersion] }>();

const statusLabels: Record<string, string> = {
  draft: "草稿", simulated: "已模拟", pending_approval: "待批准", active: "当前活动",
  rejected: "已拒绝", rolled_back: "已回滚", superseded: "已取代"
};
</script>

<template>
  <section class="policy-lifecycle-panel">
    <div class="section-heading"><div><h2>策略生命周期</h2><p>提案先验证和模拟，批准后才会激活。</p></div><span>Optimizer {{ optimizerMode }}</span></div>
    <div class="policy-version-list">
      <article v-for="policy in policies.slice(0, 12)" :key="policy.id">
        <header><span>{{ policy.scope_type }} {{ policy.scope_id || '默认' }} · v{{ policy.version }}</span><strong>{{ statusLabels[policy.status] ?? policy.status }}</strong></header>
        <div class="policy-meta"><span :class="['risk-level', policy.risk_level]">风险 {{ policy.risk_level ?? 'unknown' }}</span><span>影响 {{ policy.affected_account_ids?.length ?? 0 }} 个账号</span><span>样本 {{ policy.simulation?.sample_count ?? 0 }}</span></div>
        <p>{{ policy.reason || policy.outcome_summary || '未填写原因' }}</p>
        <details v-if="policy.diff"><summary>字段级差异与模拟</summary><pre>{{ JSON.stringify(policy.diff, null, 2) }}</pre><small>{{ policy.simulation?.summary || (policy.simulation?.passed ? '模拟通过' : '模拟未通过') }}</small></details>
        <footer>
          <button v-if="['simulated', 'pending_approval'].includes(policy.status)" class="primary-button inline" @click="$emit('activate', policy)">批准并激活</button>
          <button v-if="['draft', 'simulated', 'pending_approval'].includes(policy.status)" class="secondary-button" @click="$emit('reject', policy)">拒绝</button>
          <button v-if="policy.status === 'active' && policy.previous_active_version_id" class="secondary-button" @click="$emit('rollback', policy)">回滚</button>
        </footer>
      </article>
      <p v-if="!policies.length" class="agent-empty">暂无策略版本</p>
    </div>
  </section>
</template>

<style scoped>
.policy-version-list { display: grid; gap: 10px; }
.policy-version-list article { display: grid; gap: 8px; padding: 12px; border: 1px solid var(--border, #d8dde3); border-radius: 6px; }
.policy-version-list header, .policy-meta, footer { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
.policy-version-list header strong { margin-left: auto; }
.policy-meta { color: #5a6670; font-size: 12px; }
.risk-level.high, .risk-level.critical { color: #9b2f2f; }
.risk-level.medium { color: #815400; }
.policy-version-list p { margin: 0; }
details pre { max-height: 180px; overflow: auto; padding: 8px; background: #f4f6f7; font-size: 12px; }
</style>
