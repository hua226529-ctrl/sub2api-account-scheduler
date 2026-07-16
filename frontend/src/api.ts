import type { AgentCapability, AgentChatReceipt, AgentCommandReceipt, AgentFreezeMode, AgentFreezeState, AgentGoal, AgentGoalList, AgentMemory, AgentMessage, AgentOverview, AgentProvider, AgentProviderInput, AgentRun, AgentRuntimeEvent, AgentScheduledTask, AgentSettings, AgentStep, Diagnostics, EventItem, FailoverTier, GroupFailoverPolicy, GroupTierTransition, Policy, Settings, Snapshot, UpstreamFailoverPolicyInput, UpstreamInput, UpstreamPreview, UpstreamSource } from "./types";

let csrfToken = "";

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  headers.set("Accept", "application/json");
  if (init.body) headers.set("Content-Type", "application/json");
  if (csrfToken && init.method && init.method !== "GET") headers.set("X-CSRF-Token", csrfToken);
  const response = await fetch(`api/${path}`, { ...init, headers, credentials: "same-origin" });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({ error: `请求失败：${response.status}` }));
    throw new Error(payload.error || `请求失败：${response.status}`);
  }
  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
}

export async function restoreSession(): Promise<boolean> {
  try {
    const result = await request<{ csrf_token: string }>("session");
    csrfToken = result.csrf_token;
    return true;
  } catch {
    csrfToken = "";
    return false;
  }
}

export async function login(apiKey: string): Promise<void> {
  const result = await request<{ csrf_token: string }>("session", { method: "POST", body: JSON.stringify({ api_key: apiKey }) });
  csrfToken = result.csrf_token;
}

export async function logout(): Promise<void> {
  await request<void>("session", { method: "DELETE" });
  csrfToken = "";
}

export async function getOverview(): Promise<Snapshot> {
  const snapshot = await request<Snapshot>("overview");
  return {
    ...snapshot,
    bindings: snapshot.bindings ?? [],
    unmatched_monitors: snapshot.unmatched_monitors ?? [],
    conflicts: snapshot.conflicts ?? []
  };
}
export const getDiagnostics = () => request<Diagnostics>("diagnostics");
export async function getEvents(limit = 150): Promise<{ items: EventItem[] }> {
  const result = await request<{ items: EventItem[] }>(`events?limit=${limit}`);
  return { items: result.items ?? [] };
}
export const triggerReconcile = () => request("reconcile", { method: "POST", body: JSON.stringify({ confirm: true }) });

export function updateSettings(settings: Settings) {
  return request<Settings>("settings", { method: "PUT", body: JSON.stringify({ ...settings, confirm: true }) });
}

export function updatePolicy(accountID: number, policy: Omit<Policy, "account_id">) {
  return request<Policy>(`policies/${accountID}`, { method: "PUT", body: JSON.stringify({ ...policy, confirm: true }) });
}

export function accountAction(accountID: number, action: "pause" | "resume" | "clear-override" | "clear-flap") {
  return request(`actions/${accountID}/${action}`, { method: "POST", body: JSON.stringify({ confirm: true }) });
}

export async function getUpstreams(): Promise<{ items: UpstreamSource[] }> {
  const result = await request<{ items: UpstreamSource[] }>("upstreams");
  return { items: result.items ?? [] };
}

export function validateUpstream(input: UpstreamInput) {
  return request<UpstreamPreview>("upstreams/validate", { method: "POST", body: JSON.stringify(input) });
}

export function createUpstream(input: UpstreamInput) {
  return request<UpstreamSource>("upstreams", { method: "POST", body: JSON.stringify({ ...input, confirm: true }) });
}

export function updateUpstream(id: number, input: UpstreamInput) {
  return request<UpstreamSource>(`upstreams/${id}`, { method: "PUT", body: JSON.stringify({ ...input, confirm: true }) });
}

export function deleteUpstream(id: number) {
  return request<void>(`upstreams/${id}`, { method: "DELETE", body: JSON.stringify({ confirm: true }) });
}

export function refreshUpstream(id: number) {
  return request<UpstreamSource>(`upstreams/${id}/refresh`, { method: "POST", body: JSON.stringify({ confirm: true }) });
}

export function switchUpstreamKeyGroup(id: number, keyID: string, groupID: string) {
  return request<UpstreamSource>(`upstreams/${id}/keys/${encodeURIComponent(keyID)}/group`, {
    method: "POST",
    body: JSON.stringify({ group_id: groupID, confirm: true })
  });
}

export function saveUpstreamFailoverPolicy(id: number, input: UpstreamFailoverPolicyInput) {
  return request<GroupFailoverPolicy>(`upstreams/${id}/failover/policies/${encodeURIComponent(input.key_id)}`, {
    method: "PUT",
    body: JSON.stringify({ ...input, confirm: true })
  });
}

export async function getUpstreamFailoverTransitions(id: number, keyID: string): Promise<{ items: GroupTierTransition[] }> {
  const result = await request<{ items: GroupTierTransition[] }>(`upstreams/${id}/failover/transitions?key_id=${encodeURIComponent(keyID)}`);
  return { items: result.items ?? [] };
}

export function confirmUpstreamFailoverPolicy(id: number, keyID: string, version: number) {
  return request<GroupFailoverPolicy>(`upstreams/${id}/failover/policies/${encodeURIComponent(keyID)}/confirm`, {
    method: "POST",
    body: JSON.stringify({ version, confirm: true })
  });
}

export function switchUpstreamKeyTier(id: number, keyID: string, tier: FailoverTier) {
  return request<GroupFailoverPolicy>(`upstreams/${id}/keys/${encodeURIComponent(keyID)}/tier`, {
    method: "POST",
    body: JSON.stringify({ tier, confirm: true })
  });
}

export const getAgentOverview = () => request<AgentOverview>("agent/overview");

export function updateAgentSettings(settings: AgentSettings) {
  return request<AgentSettings>("agent/settings", { method: "PUT", body: JSON.stringify({ ...settings, confirm: true }) });
}

export function validateAgentProvider(input: AgentProviderInput) {
  return request<AgentProvider>("agent/providers/validate", { method: "POST", body: JSON.stringify(input) });
}

export function saveAgentProvider(input: AgentProviderInput) {
  return request<AgentProvider>(`agent/providers/${input.slot}`, { method: "PUT", body: JSON.stringify({ ...input, confirm: true }) });
}

export function runAgent() {
  return request<AgentRun | AgentCommandReceipt>("agent/run", { method: "POST", body: JSON.stringify({ confirm: true }) });
}

export function chatAgent(message: string, conversationID = 0) {
  return request<AgentChatReceipt>("agent/chat", {
    method: "POST", body: JSON.stringify({ conversation_id: conversationID, message })
  });
}

export function getAgentMessages(conversationID: number) {
  return request<{ items: AgentMessage[] }>(`agent/conversations/${conversationID}/messages`);
}

export function activateAgentPolicy(id: number) {
  return request<{ activated: boolean }>(`agent/policies/${id}/activate`, {
    method: "POST", body: JSON.stringify({ confirm: true })
  });
}

function listItems<T>(payload: { items?: T[] } | T[] | undefined): T[] {
  if (Array.isArray(payload)) return payload;
  return payload?.items ?? [];
}

export async function getAgentCapabilities(): Promise<{ items: AgentCapability[] }> {
  const result = await request<{ items?: AgentCapability[] } | AgentCapability[]>("agent/capabilities");
  return { items: listItems(result) };
}

export async function getAgentGoals(limit = 30): Promise<AgentGoalList> {
  const result = await request<{ items?: AgentGoal[]; steps?: AgentStep[] } | AgentGoal[]>(`agent/goals?limit=${limit}&include=steps`);
  const items = listItems(result);
  const topLevelSteps = Array.isArray(result) ? [] : result.steps ?? [];
  const embeddedSteps = items.flatMap((goal) => goal.steps ?? []);
  return { items, steps: topLevelSteps.length ? topLevelSteps : embeddedSteps };
}

export async function getAgentRuntimeEvents(limit = 100, afterID = 0): Promise<{ items: AgentRuntimeEvent[] }> {
  const suffix = afterID > 0 ? `&after_id=${afterID}` : "";
  const result = await request<{ items?: AgentRuntimeEvent[] } | AgentRuntimeEvent[]>(`agent/events?limit=${limit}${suffix}`);
  return { items: listItems(result) };
}

export async function getAgentTasks(limit = 50): Promise<{ items: AgentScheduledTask[] }> {
  const result = await request<{ items?: AgentScheduledTask[] } | AgentScheduledTask[]>(`agent/tasks?limit=${limit}`);
  return { items: listItems(result) };
}

export async function getAgentMemories(limit = 50): Promise<{ items: AgentMemory[] }> {
  const result = await request<{ items?: AgentMemory[] } | AgentMemory[]>(`agent/memories?limit=${limit}`);
  return { items: listItems(result) };
}

export async function getAgentFreezeState(): Promise<AgentFreezeState> {
  const result = await request<AgentFreezeState | { item?: AgentFreezeState; items?: AgentFreezeState[] }>("agent/freeze");
  if ("mode" in result) return result;
  return result.item ?? result.items?.[0] ?? {
    scope_type: "global", scope_id: "", mode: "active", reason: "", actor: "system"
  };
}

export function setAgentFreezeState(mode: AgentFreezeMode, reason: string, expiresAt?: string) {
  return request<AgentFreezeState>("agent/freeze", {
    method: "PUT",
    body: JSON.stringify({
      scope_type: "global", scope_id: "", mode, reason,
      ...(expiresAt ? { expires_at: expiresAt } : {}), confirm: true
    })
  });
}

export function openAgentStream(onEvent: (event: AgentRuntimeEvent) => void, onError?: () => void): EventSource | null {
  if (typeof EventSource === "undefined") return null;
  const stream = new EventSource("api/agent/stream", { withCredentials: true });
  const receive = (raw: MessageEvent<string>) => {
    try { onEvent(JSON.parse(raw.data) as AgentRuntimeEvent); }
    catch { /* Ignore keepalive frames and malformed optional stream events. */ }
  };
  stream.onmessage = receive;
  for (const eventName of ["agent_event", "goal", "step", "task", "run", "freeze"]) {
    stream.addEventListener(eventName, receive as EventListener);
  }
  stream.onerror = () => onError?.();
  return stream;
}
