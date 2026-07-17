<script setup lang="ts">
import type { AgentChatReceipt } from "../../types";

defineProps<{ receipt: AgentChatReceipt; pending: boolean }>();
defineEmits<{ confirm: [] }>();

const intentLabels: Record<string, string> = {
  query: "查询", analysis: "只读分析", direct_action: "直接动作",
  policy_change: "策略提案", scheduled_action: "定时命令", ambiguous: "需要澄清"
};
</script>

<template>
  <div class="agent-intent-receipt">
    <div>
      <span :class="['intent-badge', receipt.intent.intent_type]">{{ intentLabels[receipt.intent.intent_type] }}</span>
      <strong>{{ receipt.intent.user_facing_summary }}</strong>
    </div>
    <small>
      {{ receipt.intent.read_only ? '只读' : receipt.intent.operation }}
      <template v-if="receipt.intent.resource_ids?.length"> · 影响 {{ receipt.intent.resource_ids.length }} 个资源</template>
      · 风险 {{ receipt.intent.risk_level }}
    </small>
    <p v-if="receipt.intent.clarification">{{ receipt.intent.clarification }}</p>
    <p v-if="receipt.confirmation_expires_at">确认有效期至 {{ new Date(receipt.confirmation_expires_at).toLocaleString('zh-CN') }}</p>
	<button v-if="receipt.confirmation_token" class="danger-button" @click="$emit('confirm')">确认执行预览动作</button>
  </div>
</template>

<style scoped>
.agent-intent-receipt { display: grid; gap: 8px; padding: 12px; border: 1px solid var(--border, #d8dde3); border-radius: 6px; background: #f7f9fa; }
.agent-intent-receipt > div { display: flex; gap: 8px; align-items: center; }
.agent-intent-receipt p, .agent-intent-receipt small { margin: 0; color: #5a6670; }
.intent-badge { padding: 3px 7px; border-radius: 4px; background: #dfe8ec; font-size: 12px; }
.intent-badge.direct_action, .intent-badge.scheduled_action { background: #fee8c8; color: #7a4600; }
.intent-badge.ambiguous { background: #f6dcdc; color: #8b2424; }
.danger-button { justify-self: start; }
</style>
