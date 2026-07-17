# Agent Capability Contract

Capabilities are a closed registry with a centralized `ExecutionPolicy`. Policy metadata defines risk, read-only status, autonomous support, exact grant, confirmation, fresh snapshot, evidence, scheduling support, scope and TTL bounds.

Read capabilities can run without write grants. Account capabilities always delegate to AccountControl. Group capabilities always delegate to `TransitionGroupTier`. Policy updates create proposals; activation is a separate fenced operation. Scheduled capabilities persist a typed command and revalidate at execution time.

The Agent cannot execute arbitrary SQL, shell, HTTP or files. A model response cannot widen the locally validated chat intent, resource set, desired state, TTL, authority or grant.
