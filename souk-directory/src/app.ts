// Shared helpers for souk-directory — a pure static browser client of a
// souk's public HTTP API (GET /agents, POST /agui/{provider}/{name}, A2A
// agent cards). No backend of its own.
//
// The souk base URL is a runtime parameter, never baked into the build
// (see the project plan: this is the only hook worth building now for
// eventual multi-souk federation, without building any aggregation logic
// that doesn't exist yet) — read from ?souk=, remembered in localStorage,
// editable at any time from the top bar on every page. When neither is
// set, defaults to http://localhost:8000 — this repo's own
// docker-compose.yml maps souk to that port, so the common "just cloned
// this and ran docker compose up" case needs zero manual entry; the
// input box (still pre-filled and editable) is what makes anything else
// possible.

import { EventSourceParserStream } from "eventsource-parser/stream";

const SOUK_URL_KEY = "souk-directory:soukUrl";
const DEFAULT_SOUK_URL = "http://localhost:8000";

// An agent *is* `(provider_key, name)` — souk mints no id for anyone to
// hold. `fingerprint` is that same identity in 16 hex, which is what goes
// in a URL; `provider_key` is the thing to compare, since the fingerprint
// is derived and never authoritative.
export interface AgentRosterEntry {
  provider_key: string;
  fingerprint: string;
  name: string;
  description: string;
  skills: unknown[];
  joined_at: string;
  last_seen_at: string;
  online: boolean;
  provider_name: string | null;
}

// Provider identity is purely the provider_key — provider_name is an
// optional label on top of it, not a replacement. Groups a flat agent list
// into "who registered these." The fingerprint rides along because every
// link this page builds addresses a stall by it.
export interface ProviderGroup {
  providerKey: string;
  fingerprint: string;
  providerName: string | null;
  agents: AgentRosterEntry[];
}

export function shortKey(providerKey: string): string {
  return providerKey.length <= 14 ? providerKey : `${providerKey.slice(0, 8)}…${providerKey.slice(-6)}`;
}

export function groupByProvider(agents: AgentRosterEntry[]): ProviderGroup[] {
  const groups = new Map<string, ProviderGroup>();
  for (const agent of agents) {
    // Keyed on provider_key, not fingerprint: the key is the identity and
    // the fingerprint is a 16-hex digest of it, so grouping on the digest
    // would be grouping on something derived.
    let group = groups.get(agent.provider_key);
    if (!group) {
      group = {
        providerKey: agent.provider_key,
        fingerprint: agent.fingerprint,
        providerName: agent.provider_name,
        agents: [],
      };
      groups.set(agent.provider_key, group);
    }
    group.agents.push(agent);
  }
  // Named storefronts first (alphabetically), anonymous ones after,
  // ordered by key so the grouping is at least stable across reloads.
  return [...groups.values()].sort((a, b) => {
    if (!!a.providerName !== !!b.providerName) return a.providerName ? -1 : 1;
    const an = a.providerName || a.providerKey;
    const bn = b.providerName || b.providerKey;
    return an.localeCompare(bn);
  });
}

export function getSoukUrl(): string {
  const params = new URLSearchParams(window.location.search);
  const fromQuery = params.get("souk");
  if (fromQuery) {
    localStorage.setItem(SOUK_URL_KEY, fromQuery);
    return fromQuery.replace(/\/$/, "");
  }
  const stored = localStorage.getItem(SOUK_URL_KEY);
  return (stored || DEFAULT_SOUK_URL).replace(/\/$/, "");
}

export function setSoukUrl(url: string): string {
  const clean = url.trim().replace(/\/$/, "");
  localStorage.setItem(SOUK_URL_KEY, clean);
  return clean;
}

export function linkWithSouk(path: string, soukUrl: string): string {
  const url = new URL(path, window.location.href);
  url.searchParams.set("souk", soukUrl);
  return url.toString();
}

// Renders the "which souk am I browsing" bar present on every page — the
// souk URL is always resolved (falls back to DEFAULT_SOUK_URL), so
// `onChange` fires once immediately with that value and again whenever
// the user edits it.
export function renderSoukBar(containerEl: HTMLElement, onChange: (soukUrl: string) => void): string {
  const current = getSoukUrl();
  containerEl.innerHTML = `
    <form id="souk-bar-form" class="souk-bar">
      <label for="souk-bar-input">souk</label>
      <input id="souk-bar-input" type="text" placeholder="${DEFAULT_SOUK_URL}"
             value="${escapeHtml(current)}" />
      <button type="submit">Connect</button>
    </form>
  `;
  const form = containerEl.querySelector<HTMLFormElement>("#souk-bar-form")!;
  form.addEventListener("submit", (e) => {
    e.preventDefault();
    const input = containerEl.querySelector<HTMLInputElement>("#souk-bar-input")!;
    const url = setSoukUrl(input.value);
    onChange(url);
  });
  return current;
}

export function escapeHtml(str: string): string {
  const div = document.createElement("div");
  div.textContent = str;
  return div.innerHTML;
}

export async function fetchAgents(soukUrl: string): Promise<AgentRosterEntry[]> {
  const resp = await fetch(`${soukUrl}/agents`);
  if (!resp.ok) {
    throw new Error(`GET /agents failed: ${resp.status}`);
  }
  const body: { agents: AgentRosterEntry[] } = await resp.json();
  return body.agents;
}

// Parses a POST'd EventSource-shaped stream (the browser's native
// EventSource can't POST, and souk's /agui/{provider}/{name} requires a
// POST body, so we still drive the fetch ourselves) via eventsource-parser
// instead of hand-rolling SSE framing — a hand-rolled version of this
// previously shipped broken (assumed bare `\n\n` between events; sse_
// starlette actually emits `\r\n\r\n`, so it silently parsed zero events
// off a perfectly well-formed stream). SSE framing has enough edge cases
// (CRLF vs LF, multi-line `data:` fields, comment lines, `id:`/`retry:`)
// that it isn't worth re-deriving by hand a second time.
// Calls `onEvent` with the parsed JSON payload of each event as it arrives.
//
// Read with an explicit reader loop, not `for await (… of stream)`:
// async iteration of ReadableStream is a late addition to the spec that
// WebKit has not implemented, so the for-await form worked in
// Chromium/Gecko and threw `undefined is not a function` on every
// Safari/iOS browser — which killed the whole conversation page there,
// while roster/index (plain resp.json()) kept working. Nothing caught it
// because tsconfig's lib said "DOM.AsyncIterable", which is exactly the
// promise WebKit doesn't keep; that lib entry is gone now, so
// reintroducing for-await over a ReadableStream fails `npm run
// typecheck` instead of failing in the user's hand. `getReader()` is
// original-spec and universal.
export async function streamSse(response: Response, onEvent: (event: any) => void): Promise<void> {
  const reader = response
    .body!.pipeThrough(new TextDecoderStream())
    .pipeThrough(new EventSourceParserStream())
    .getReader();
  while (true) {
    const { done, value: event } = await reader.read();
    if (done) return;
    try {
      onEvent(JSON.parse(event.data));
    } catch (err) {
      console.error("souk-directory: failed to parse SSE payload", event.data, err);
    }
  }
}
