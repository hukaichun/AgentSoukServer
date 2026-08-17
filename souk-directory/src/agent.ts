import DOMPurify from "dompurify";
import { marked } from "marked";
import { AgentRosterEntry, escapeHtml, fetchAgents, linkWithSouk, renderSoukBar, streamSse } from "./app.js";

// A thread names the agent it belongs to by the pair, same as everything
// else now. `agent_name` arriving on the node is what lets the call-chain
// strip label itself without a lookup — but the stall still needs one,
// because two stalls may each keep an agent of the same name and a route
// that says "translator → scribe" would not say *which* translator.
interface ThreadTreeNode {
  thread_id: string;
  provider_key: string;
  agent_name: string;
  // Only children carry this — the root is the thread you already had, not
  // a call anyone made. It is what separates a fan from a relay: siblings
  // issued in one model turn are timestamped sub-millisecond apart, a
  // sequential second call seconds apart.
  created_at?: string;
  children: ThreadTreeNode[];
}

marked.setOptions({ breaks: true, gfm: true });

// Agent-produced prose (assistant replies, sub-agent replies, reasoning
// text, tool results) is near-always markdown — the models are prompted
// (and generally trained) to format that way. Rendered through DOMPurify
// rather than trusted raw: this text ultimately originates from whatever
// model a possibly-untrusted third-party provider is running, so treat it
// like any other untrusted HTML source before it hits innerHTML.
function renderMarkdown(el: HTMLElement, raw: string): void {
  el.innerHTML = DOMPurify.sanitize(marked.parse(raw, { async: false }) as string);
  el.classList.add("markdown");
}

// The pair is the address: ?provider=<fingerprint>&name=<name>.
const params = new URLSearchParams(window.location.search);
const providerRef = params.get("provider");
const agentName = params.get("name");
let threadId: string | null = null;
let selfName = "assistant";
let currentAssistantEl: HTMLElement | null = null;
let currentAssistantRaw = "";
let typingEl: HTMLElement | null = null;
// provider_key -> storefront label, from the roster fetched once at page
// load. The call-chain strip gets agent names from the tree itself now,
// but not stall names — and without the stall a route through a duplicated
// agent name is unreadable.
const stallNameByKey = new Map<string, string>();
// Keyed by the sub-agent's A2A task id — one accumulating bubble per
// delegated call, so a streamed multi-hop response reads like the main
// assistant's own streaming text instead of one raw JSON blob per event.
const subAgentEls = new Map<string, HTMLElement>();
const subAgentRaw = new Map<string, string>();
// Keyed by toolCallId — one placeholder bubble per in-flight tool call,
// filled in with the tool's result once it comes back. Surfacing this is
// what keeps a multi-second tool round trip from reading as unexplained
// silence (see the souk<->provider latency work this pairs with).
const toolCallEls = new Map<string, HTMLElement>();
// Keyed by the reasoning message_id AG-UI's REASONING_* events share
// across REASONING_START/MESSAGE_CONTENT/ENCRYPTED_VALUE/END for one
// "thought". Not every model exposes readable reasoning text — some only
// ever send an opaque REASONING_ENCRYPTED_VALUE — so the bubble falls
// back to a plain "thinking" placeholder when no content ever arrives.
const reasoningEls = new Map<string, { el: HTMLElement; raw: string }>();

function appendEntry(tag: string, text: string, cssClass: string): HTMLElement {
  const log = document.getElementById("chat-log")!;
  const div = document.createElement("div");
  div.className = `entry ${cssClass}`;
  div.innerHTML = `<span class="tag">${escapeHtml(tag)}</span><div class="bubble"></div>`;
  const bubble = div.querySelector(".bubble") as HTMLElement;
  bubble.textContent = text;
  log.appendChild(div);
  log.scrollTop = log.scrollHeight;
  return bubble;
}

function showTyping(): void {
  if (typingEl) return;
  const bubble = appendEntry(selfName, "", "assistant");
  bubble.innerHTML = `<span class="typing"><span></span><span></span><span></span></span>`;
  typingEl = bubble.parentElement as HTMLElement;
}

function clearTyping(): void {
  typingEl?.remove();
  typingEl = null;
}

async function loadAgentInfo(soukUrl: string): Promise<AgentRosterEntry | null> {
  const backLink = document.getElementById("back-link") as HTMLAnchorElement;
  backLink.href = linkWithSouk("index.html", soukUrl);

  const agents = await fetchAgents(soukUrl);
  for (const a of agents) {
    if (a.provider_name) stallNameByKey.set(a.provider_key, a.provider_name);
  }
  // `provider` in the URL is the fingerprint, but accept the full key too:
  // it costs one comparison and means a hand-written link works either way,
  // exactly as the gateway's own {provider} path segment accepts both.
  const agent = agents.find(
    (a) => a.name === agentName && (a.fingerprint === providerRef || a.provider_key === providerRef)
  );
  if (!agent) {
    document.getElementById("agent-title")!.textContent = "Agent not found";
    document.getElementById("agent-desc")!.textContent =
      "It may be offline past the staleness window, or de-listed.";
    (document.getElementById("chat-form") as HTMLElement).style.display = "none";
    return null;
  }
  selfName = agent.name;
  document.getElementById("agent-title")!.textContent = agent.name;
  document.getElementById("status-chip")!.innerHTML = `
    <span class="status-chip ${agent.online ? "online" : "offline"}">
      <span class="dot"></span>${agent.online ? "online" : "offline"}
    </span>
  `;
  // The address, shown the way it is written: stall then name.
  document.getElementById("agent-id")!.textContent =
    `${agent.provider_name || "unnamed provider"} · ${agent.fingerprint}/${agent.name}`;
  document.getElementById("agent-desc")!.textContent = agent.description || "";
  if (!agent.online) {
    document.getElementById("offline-banner")!.innerHTML =
      `<div class="offline-banner">Currently offline — your message will wait ` +
      `briefly for it to come back, then fail if it doesn't.</div>`;
  }
  return agent;
}

function handleAguiEvent(event: any): void {
  if (event.type === "RUN_STARTED") {
    // The standard, in-band place to learn the resolved thread_id — souk
    // substitutes its own real one if the id sent above wasn't
    // recognized (see souk-no-forced-protocol-deviation); no custom
    // response header for this anymore.
    if (event.threadId) threadId = event.threadId;
    return;
  }
  if (event.type === "RUN_ERROR") {
    clearTyping();
    appendEntry("error", event.message || "The agent failed to respond.", "error");
    return;
  }
  if (event.type === "TEXT_MESSAGE_CONTENT" || event.type === "TEXT_MESSAGE_CHUNK") {
    const delta = event.delta || event.content || "";
    if (!delta) return;
    clearTyping();
    if (!currentAssistantEl) {
      currentAssistantEl = appendEntry(selfName, "", "assistant");
      currentAssistantRaw = "";
    }
    currentAssistantRaw += delta;
    renderMarkdown(currentAssistantEl, currentAssistantRaw);
    return;
  }
  if (event.type === "TOOL_CALL_START") {
    clearTyping();
    const el = appendEntry(`tool · ${event.toolCallName || "?"}`, "…", "tool");
    toolCallEls.set(event.toolCallId, el);
    return;
  }
  if (event.type === "TOOL_CALL_RESULT") {
    const el = toolCallEls.get(event.toolCallId);
    toolCallEls.delete(event.toolCallId);
    if (!el) return;
    const content = typeof event.content === "string" ? event.content : JSON.stringify(event.content);
    renderMarkdown(el, content || "(no result)");
    return;
  }
  if (event.type === "REASONING_START") {
    clearTyping();
    const el = appendEntry("thinking", "…", "reasoning");
    reasoningEls.set(event.messageId, { el, raw: "" });
    return;
  }
  if (event.type === "REASONING_MESSAGE_CONTENT") {
    const entry = reasoningEls.get(event.messageId);
    if (!entry) return;
    entry.raw += event.delta || "";
    renderMarkdown(entry.el, entry.raw);
    return;
  }
  if (event.type === "REASONING_ENCRYPTED_VALUE") {
    // entityId is the same message_id REASONING_START used — see
    // pydantic_ai.ui.ag_ui's _thinking_0_13.py. Some models never expose
    // readable reasoning text, only this opaque blob; if nothing rendered
    // yet, at least say a thought happened rather than leaving "…" stuck.
    const entry = reasoningEls.get(event.entityId);
    if (entry && !entry.raw) {
      entry.el.textContent = "(reasoning hidden by the model)";
    }
    return;
  }
  if (event.type === "REASONING_END") {
    reasoningEls.delete(event.messageId);
    return;
  }
  if (event.type === "RUN_FINISHED") {
    currentAssistantEl = null;
    currentAssistantRaw = "";
    toolCallEls.clear();
    reasoningEls.clear();
    clearTyping();
    return;
  }
  if (event.type === "CUSTOM" && event.name === "sub_agent_progress") {
    // Surfaces multi-hop delegation live, when the provider forwards it
    // (see providers/pydantic-ai-agent's sub_agent_tool.py) — souk itself
    // doesn't guarantee this for every provider, see the project plan's
    // A9 notes.
    //
    // The raw payload is an A2A task update (status ticks, an internal
    // agui_event passthrough, then streamed artifact text parts) — most
    // of that is plumbing, not something a human should have to read.
    // Render one accumulating bubble per sub-agent call instead of a raw
    // JSON dump per event.
    clearTyping();
    const value = event.value || {};
    const subAgent = value.sub_agent || "sub-agent";
    // A2A v1.0 wraps every streamed item in a StreamResponse whose single
    // key says what it is, rather than a bare update carrying a
    // discriminator field.
    const statusUpdate = value.statusUpdate || {};
    const artifactUpdate = value.artifactUpdate || {};
    const taskId = statusUpdate.taskId || artifactUpdate.taskId || subAgent;
    const state = statusUpdate.status?.state;

    if (state === "TASK_STATE_COMPLETED" || state === "TASK_STATE_FAILED") {
      subAgentEls.delete(taskId);
      subAgentRaw.delete(taskId);
      return;
    }

    // `Part` is a oneof, so a text part is just `{text}` — nothing else to
    // match on.
    const textParts = (artifactUpdate.artifact?.parts || []).filter((p: any) => p.text);
    if (textParts.length === 0) {
      // A status-only tick (e.g. the initial "working") — show a
      // placeholder once so the delegation is visible before any text
      // streams in, but don't spam a bubble per tick.
      if (!subAgentEls.has(taskId)) {
        subAgentEls.set(taskId, appendEntry(subAgent, "…", "sub"));
      }
      return;
    }

    let el = subAgentEls.get(taskId);
    if (!el || !subAgentRaw.get(taskId)) {
      el = el || appendEntry(subAgent, "", "sub");
      subAgentEls.set(taskId, el);
      subAgentRaw.set(taskId, "");
    }
    for (const part of textParts) {
      subAgentRaw.set(taskId, (subAgentRaw.get(taskId) || "") + part.text);
    }
    renderMarkdown(el, subAgentRaw.get(taskId) || "");
  }
}

// Every root-to-leaf path through the delegation tree — one row per walk
// that actually happened.
//
// This replaces a depth-first flatten into a single line, which did not
// merely lose the branching: it *asserted calls that were never made*.
// Measured on the demo market — one run of souk-guide fanning out to
// translator and summarizer, with summarizer then calling translator
// itself — flattened to
//
//     souk-guide -> translator -> summarizer -> translator
//
// whose middle arrow says translator called summarizer. It did not; they
// were siblings, started 0.5ms apart. A four-stop relay and a two-way fan
// with one deep branch are different events, and the flatten rendered them
// identically, inventing an edge to do it. Paths cannot: every arrow drawn
// here is an edge that exists in the tree.
function routesToLeaves(node: ThreadTreeNode): ThreadTreeNode[][] {
  if (node.children.length === 0) return [[node]];
  return node.children.flatMap((child) =>
    routesToLeaves(child).map((path) => [node, ...path])
  );
}

// Siblings that began within this window are treated as having been issued
// together rather than one after another. The gap that matters is orders of
// magnitude, not milliseconds: concurrent tool calls from one model turn
// land sub-millisecond apart, while a genuinely sequential second call
// waits for the first to come back — seconds. Anything in between is rare
// enough that calling it concurrent costs nothing.
const SIMULTANEOUS_MS = 250;

function startedTogether(node: ThreadTreeNode): boolean {
  const times = node.children
    .map((c) => (c.created_at ? Date.parse(c.created_at) : NaN))
    .filter((t) => !Number.isNaN(t));
  if (times.length < 2) return false;
  return Math.max(...times) - Math.min(...times) <= SIMULTANEOUS_MS;
}

async function renderCallChain(soukUrl: string, forThreadId: string): Promise<void> {
  const container = document.getElementById("call-chain")!;
  try {
    const resp = await fetch(`${soukUrl}/threads/${encodeURIComponent(forThreadId)}/tree`);
    if (!resp.ok) return;
    const root: ThreadTreeNode = await resp.json();
    // Only worth showing once there's an actual delegation — a plain
    // agent with no sub-calls would otherwise render a pointless
    // single-stop route on every reply.
    if (root.children.length === 0) {
      container.innerHTML = "";
      container.classList.remove("has-chain");
      return;
    }
    const routes = routesToLeaves(root);
    const fannedOut = startedTogether(root);
    const renderRoute = (stops: ThreadTreeNode[]): string =>
      stops
      .map((node, i) => {
        const name = escapeHtml(node.agent_name);
        // Name the stall only where the walk actually crosses into it.
        // Rendering it on every hop was the first thing that looked wrong
        // on a real chain: a three-hop delegation inside one stall printed
        // the same storefront three times and wrapped the strip onto two
        // lines, saying nothing on any of them. The root is covered by the
        // page header, and an unchanged stall means nobody went anywhere —
        // so the label appears exactly where a visitor would have had to
        // walk, and disappears when the work never left the counter.
        const stall = stallNameByKey.get(node.provider_key);
        const crossed = i > 0 && node.provider_key !== stops[i - 1].provider_key;
        const at = crossed && stall ? `<span class="stop-stall">${escapeHtml(stall)}</span>` : "";
        const link = i > 0 ? `<span class="link"></span>` : "";
        return `${link}<span class="stop ${i === 0 ? "root" : ""}" title="${escapeHtml(
          node.provider_key
        )}"><span class="dot"></span>${name}${at}</span>`;
      })
        .join("");
    const label =
      routes.length === 1
        ? "call chain for this reply"
        : fannedOut
          ? `call chains for this reply · ${routes.length} sent at once`
          : `call chains for this reply · ${routes.length}`;
    container.classList.add("has-chain");
    container.innerHTML =
      `<div class="chain-label">${label}</div>` +
      routes.map((stops) => `<div class="route">${renderRoute(stops)}</div>`).join("");
  } catch {
    // Best-effort UI sugar — a failed fetch here shouldn't disrupt the
    // chat itself, which already succeeded by the time this runs.
  }
}

async function sendMessage(soukUrl: string, text: string): Promise<void> {
  const sendBtn = document.getElementById("chat-send") as HTMLButtonElement;
  sendBtn.disabled = true;
  appendEntry("you", text, "user");
  currentAssistantEl = null;
  currentAssistantRaw = "";
  subAgentEls.clear();
  subAgentRaw.clear();
  toolCallEls.clear();
  reasoningEls.clear();
  document.getElementById("call-chain")!.innerHTML = "";
  document.getElementById("call-chain")!.classList.remove("has-chain");
  showTyping();

  // POST /threads is optional now (souk mints one automatically for an
  // unrecognized threadId — see souk-no-forced-protocol-deviation), but
  // the directory still calls it explicitly here so it can show the
  // thread_id before the first message is even sent. The message itself
  // still needs an `id` — real AG-UI's own Message schema requires one,
  // no default — but souk discards whatever value this is and assigns
  // its own real one regardless (see
  // repo.append_thread_messages), so any locally-unique placeholder
  // satisfies the schema without meaning anything beyond that.
  try {
    if (!threadId) {
      const created = await fetch(`${soukUrl}/threads/${encodeURIComponent(providerRef!)}/${encodeURIComponent(agentName!)}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({}),
      });
      threadId = (await created.json()).thread_id;
    }

    const body: Record<string, unknown> = {
      threadId,
      runId: crypto.randomUUID(),
      state: null,
      messages: [{ id: crypto.randomUUID(), role: "user", content: text }],
      tools: [],
      context: [],
      forwardedProps: null,
    };

    const resp = await fetch(`${soukUrl}/agui/${encodeURIComponent(providerRef!)}/${encodeURIComponent(agentName!)}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    const contentType = resp.headers.get("content-type") || "";
    if (contentType.includes("application/json")) {
      // Duplicate-call snapshot branch (an active run already exists on
      // this thread) — no new stream to read, just a state snapshot.
      // Its own top-level `thread_id` field is the resolved one (see
      // repo.get_thread_snapshot) — no header needed here either.
      const snapshot = await resp.json();
      threadId = snapshot.thread_id || threadId;
      clearTyping();
      appendEntry("system", "(already in flight — waiting for it to finish)", "system");
      return;
    }
    await streamSse(resp, handleAguiEvent);
    if (threadId) {
      await renderCallChain(soukUrl, threadId);
    }
  } catch (err) {
    clearTyping();
    const message = err instanceof Error ? err.message : String(err);
    appendEntry("error", `request failed: ${message}`, "error");
  } finally {
    clearTyping();
    sendBtn.disabled = false;
  }
}

async function init(soukUrl: string): Promise<void> {
  if (!providerRef || !agentName) {
    document.getElementById("agent-title")!.textContent =
      "Need both ?provider= and ?name= — an agent is the pair";
    return;
  }
  await loadAgentInfo(soukUrl);
  document.getElementById("chat-form")!.addEventListener("submit", (e) => {
    e.preventDefault();
    const input = document.getElementById("chat-input") as HTMLInputElement;
    const text = input.value.trim();
    if (!text) return;
    input.value = "";
    sendMessage(soukUrl, text);
  });
}

const initial = renderSoukBar(document.getElementById("souk-bar")!, init);
init(initial);
