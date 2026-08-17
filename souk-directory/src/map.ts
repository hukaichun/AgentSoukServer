// The market view: the same souk as a place.
//
// EXPLORATORY. This exists to answer one question the list cannot settle by
// argument — does giving a stall a *location* say anything the list does
// not already say? docs/ai-town-as-frontend.md claims three things a
// sequence cannot hold: fan-out drawn as fan-out, distance, and motion
// while a run is still in flight. This page is the cheapest honest test of
// all three, and it needs no engine, no server and no new infrastructure:
// stalls are placed by hashing an identity, and everything else is already
// on souk's public HTTP API.
//
// Scope, deliberately: only conversations this page starts itself. souk has
// no way to enumerate threads or runs — a viewer can follow only ids it
// already holds — so "open the map and watch the market work" is not
// buildable today by anyone, with or without a game engine. That is a souk
// gap (server-mode.md scopes run observation out on purpose), not a
// rendering one, and pretending otherwise here would be inventing traffic.

import DOMPurify from "dompurify";
import { marked } from "marked";
import {
  AgentRosterEntry,
  escapeHtml,
  fetchAgents,
  getSoukUrl,
  groupByProvider,
  linkWithSouk,
  renderSoukBar,
  streamSse,
} from "./app.js";
import { placeStalls } from "./layout.js";

marked.setOptions({ breaks: true, gfm: true });

const COLS = 4;
const ROWS = 3;
const CELL_W = 250;
const CELL_H = 206;
const STALL_W = 200;
const STALL_H = 150;

interface Stall {
  providerKey: string;
  fingerprint: string;
  name: string | null;
  agents: AgentRosterEntry[];
  x: number;
  y: number;
}

interface ThreadTreeNode {
  thread_id: string;
  provider_key: string;
  agent_name: string;
  created_at?: string;
  children: ThreadTreeNode[];
}

let stalls: Stall[] = [];
let current: AgentRosterEntry | null = null;
let threadId: string | null = null;
// One walker per delegated call, keyed by the A2A task id — the same key
// the chat log uses, and the only thing on the wire that distinguishes two
// calls issued in the same model turn.
const walkers = new Map<string, SVGGElement>();

const svg = () => document.getElementById("map") as unknown as SVGSVGElement;
const side = () => document.getElementById("side")!;
const note = (msg: string) => {
  document.getElementById("map-note")!.textContent = msg;
};

function stallCentre(s: Stall): { x: number; y: number } {
  return { x: s.x + STALL_W / 2, y: s.y + STALL_H / 2 };
}

function findStall(providerKey: string): Stall | undefined {
  return stalls.find((s) => s.providerKey === providerKey);
}

// Resolving a stall from a name alone, for the one case that still needs
// it. `sub_agent_progress` now carries `provider`, but it is null for a
// sub-agent configured with an explicit a2a_url — an agent on some other
// souk, which has no row in this roster. Then the name is all there is, and
// a name is not an address: if two stalls staff the same one, this returns
// nothing rather than picking. Guessing would draw a walk to a stall the
// work never went to, which is the failure this whole addressing scheme
// exists to prevent.
function stallByAgentName(name: string): Stall | undefined {
  const hits = stalls.filter((s) => s.agents.some((a) => a.name === name));
  return hits.length === 1 ? hits[0] : undefined;
}

function renderMap(): void {
  const el = svg();
  const placed = placeStalls(
    stalls.map((s) => ({ fingerprint: s.fingerprint, stall: s })),
    COLS,
    ROWS
  );
  const originX = (1000 - COLS * CELL_W) / 2 + (CELL_W - STALL_W) / 2;
  const originY = (620 - ROWS * CELL_H) / 2 + (CELL_H - STALL_H) / 2;

  for (const p of placed) {
    p.item.stall.x = originX + p.col * CELL_W;
    p.item.stall.y = originY + p.row * CELL_H;
  }

  el.innerHTML =
    `<g id="edges"></g>` +
    stalls
      .map((s) => {
        const title = s.name ? escapeHtml(s.name) : "unnamed provider";
        const agents = s.agents
          .map(
            (a, i) => `
        <g class="person ${a.online ? "" : "away"}"
           data-provider="${escapeHtml(a.fingerprint)}" data-name="${escapeHtml(a.name)}"
           transform="translate(${14}, ${52 + i * 26})">
          <circle class="person-dot" cx="6" cy="-4" r="4"></circle>
          <text class="person-name" x="18" y="0">${escapeHtml(a.name)}</text>
        </g>`
          )
          .join("");
        return `
      <g class="stall-g" data-key="${escapeHtml(s.providerKey)}" transform="translate(${s.x}, ${s.y})">
        <rect class="stall-box" width="${STALL_W}" height="${STALL_H}" rx="6"></rect>
        <text class="stall-sign" x="14" y="24">${title}</text>
        <text class="stall-fp" x="14" y="40">${escapeHtml(s.fingerprint)}</text>
        ${agents}
      </g>`;
      })
      .join("") +
    `<g id="walkers"></g>`;

  el.querySelectorAll<SVGGElement>(".person").forEach((g) => {
    g.addEventListener("click", () => {
      const fp = g.dataset.provider!;
      const name = g.dataset.name!;
      const agent = stalls.flatMap((s) => s.agents).find(
        (a) => a.fingerprint === fp && a.name === name
      );
      if (agent) openChat(agent);
    });
  });
}

// ---- walkers: motion while the run is still in flight

function walkerFor(taskId: string, from: Stall, to: Stall): void {
  if (walkers.has(taskId)) return;
  // The rule the strip arrived at first: work that never left the stall is
  // not a journey. Two people behind one counter passing something between
  // them should not draw a walk across the market.
  if (from.providerKey === to.providerKey) return;

  const g = document.createElementNS("http://www.w3.org/2000/svg", "g");
  g.setAttribute("class", "walker");
  const a = stallCentre(from);
  const b = stallCentre(to);
  g.setAttribute("transform", `translate(${a.x}, ${a.y})`);
  g.innerHTML = `<circle r="6"></circle>`;
  document.getElementById("walkers")!.appendChild(g);
  walkers.set(taskId, g);
  // Next frame, so the browser has the start position before the
  // transition to the end one.
  requestAnimationFrame(() => {
    g.setAttribute("transform", `translate(${b.x}, ${b.y})`);
  });
}

function retireWalker(taskId: string, home: Stall | undefined): void {
  const g = walkers.get(taskId);
  if (!g) return;
  walkers.delete(taskId);
  if (home) {
    const a = stallCentre(home);
    g.setAttribute("transform", `translate(${a.x}, ${a.y})`);
    setTimeout(() => g.remove(), 900);
  } else {
    g.remove();
  }
}

// ---- the finished shape, drawn from the tree rather than guessed live

function drawTree(root: ThreadTreeNode): void {
  const edges = document.getElementById("edges")!;
  const lines: string[] = [];
  const walk = (node: ThreadTreeNode) => {
    const from = findStall(node.provider_key);
    for (const child of node.children) {
      const to = findStall(child.provider_key);
      // A hop within one stall is real but has no distance to draw. It is
      // marked on the stall rather than as a line, so the map never shows
      // a journey that did not happen.
      if (from && to && from.providerKey !== to.providerKey) {
        const a = stallCentre(from);
        const b = stallCentre(to);
        lines.push(`<line class="edge" x1="${a.x}" y1="${a.y}" x2="${b.x}" y2="${b.y}"></line>`);
      } else if (to) {
        lines.push(
          `<circle class="edge-inplace" cx="${stallCentre(to).x}" cy="${stallCentre(to).y}" r="16"></circle>`
        );
      }
      walk(child);
    }
  };
  walk(root);
  edges.innerHTML = lines.join("");
}

function describeShape(root: ThreadTreeNode): string {
  const times = root.children
    .map((c) => (c.created_at ? Date.parse(c.created_at) : NaN))
    .filter((t) => !Number.isNaN(t));
  const together = times.length >= 2 && Math.max(...times) - Math.min(...times) <= 250;
  const depth = (n: ThreadTreeNode): number =>
    n.children.length === 0 ? 0 : 1 + Math.max(...n.children.map(depth));
  if (root.children.length === 0) return "no delegation — the answer never left this stall";
  const shape = together
    ? `${root.children.length} sent at once`
    : `${root.children.length} in sequence`;
  return `${shape}, ${depth(root)} deep`;
}

// ---- chat: the only traffic this page can honestly show

function openChat(agent: AgentRosterEntry): void {
  current = agent;
  threadId = null;
  const stall = findStall(agent.provider_key);
  side().innerHTML = `
    <div class="side-head">
      <div class="side-name">${escapeHtml(agent.name)}</div>
      <div class="side-stall">${escapeHtml(stall?.name || "unnamed provider")} · ${escapeHtml(
        agent.fingerprint
      )}</div>
      <a class="back-link" href="${escapeHtml(
        linkWithSouk(
          `agent.html?provider=${encodeURIComponent(agent.fingerprint)}&name=${encodeURIComponent(agent.name)}`,
          getSoukUrl()
        )
      )}">open as a page &rarr;</a>
    </div>
    <div id="side-log" class="side-log"></div>
    <form id="side-form" class="side-form">
      <input id="side-input" type="text" placeholder="Message ${escapeHtml(agent.name)}…" />
      <button type="submit">Send</button>
    </form>`;
  document.getElementById("side-form")!.addEventListener("submit", (e) => {
    e.preventDefault();
    const input = document.getElementById("side-input") as HTMLInputElement;
    const text = input.value.trim();
    if (!text) return;
    input.value = "";
    send(getSoukUrl(), text);
  });
}

function logEntry(tag: string, cssClass: string): HTMLElement {
  const log = document.getElementById("side-log")!;
  const div = document.createElement("div");
  div.className = `entry ${cssClass}`;
  div.innerHTML = `<span class="tag">${escapeHtml(tag)}</span><div class="bubble"></div>`;
  log.appendChild(div);
  log.scrollTop = log.scrollHeight;
  return div.querySelector(".bubble") as HTMLElement;
}

async function send(soukUrl: string, text: string): Promise<void> {
  if (!current) return;
  const agent = current;
  logEntry("you", "user").textContent = text;
  document.getElementById("edges")!.innerHTML = "";
  note("");

  let reply: HTMLElement | null = null;
  let raw = "";
  const home = findStall(agent.provider_key);

  try {
    if (!threadId) {
      const created = await fetch(
        `${soukUrl}/threads/${encodeURIComponent(agent.fingerprint)}/${encodeURIComponent(agent.name)}`,
        { method: "POST", headers: { "content-type": "application/json" }, body: "{}" }
      );
      threadId = (await created.json()).thread_id;
    }
    const resp = await fetch(
      `${soukUrl}/agui/${encodeURIComponent(agent.fingerprint)}/${encodeURIComponent(agent.name)}`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          threadId,
          runId: crypto.randomUUID(),
          state: null,
          messages: [{ id: crypto.randomUUID(), role: "user", content: text }],
          tools: [],
          context: [],
          forwardedProps: null,
        }),
      }
    );
    if (!resp.body) throw new Error("no stream");

    await streamSse(resp, (event: any) => {
      if (event.type === "RUN_STARTED" && event.threadId) threadId = event.threadId;
      if (event.type === "TEXT_MESSAGE_CONTENT" || event.type === "TEXT_MESSAGE_CHUNK") {
        const delta = event.delta || event.content || "";
        if (!delta) return;
        if (!reply) {
          reply = logEntry(agent.name, "assistant");
          raw = "";
        }
        raw += delta;
        reply.innerHTML = DOMPurify.sanitize(marked.parse(raw, { async: false }) as string);
        return;
      }
      if (event.type === "CUSTOM" && event.name === "sub_agent_progress") {
        onDelegation(event.value || {}, home);
        return;
      }
    });
  } catch (err) {
    logEntry("error", "error").textContent = err instanceof Error ? err.message : String(err);
    return;
  }

  // Everything still walking is retired before the true shape is drawn:
  // the live guesses were about *who is busy*, the tree is about what
  // actually happened, and only one of them is evidence.
  for (const id of [...walkers.keys()]) retireWalker(id, home);

  if (!threadId) return;
  try {
    const resp = await fetch(`${soukUrl}/threads/${encodeURIComponent(threadId)}/tree`);
    if (!resp.ok) return;
    const root: ThreadTreeNode = await resp.json();
    drawTree(root);
    note(describeShape(root));
  } catch {
    // The reply already arrived; the picture is a bonus, not the answer.
  }
}

function onDelegation(value: any, home: Stall | undefined): void {
  const statusUpdate = value.statusUpdate || {};
  const artifactUpdate = value.artifactUpdate || {};
  const taskId = statusUpdate.taskId || artifactUpdate.taskId || value.sub_agent;
  const state = statusUpdate.status?.state;

  // `provider` is the resolved callee's fingerprint and is the right answer
  // whenever it is there. `agent_name` is who was called; `sub_agent` is
  // only the tool's name and may differ, so it is a label and never an
  // identity.
  const to =
    (value.provider && stalls.find((s) => s.fingerprint === value.provider)) ||
    (value.provider_key && findStall(value.provider_key)) ||
    stallByAgentName(value.agent_name || value.sub_agent);

  if (state === "TASK_STATE_COMPLETED" || state === "TASK_STATE_FAILED") {
    retireWalker(taskId, home);
    return;
  }
  if (!to) {
    // Named, unplaceable: an agent on another souk, or a name two stalls
    // share. Said out loud rather than drawn somewhere plausible.
    note(`a call went to "${value.agent_name || value.sub_agent}", which this map can't place`);
    return;
  }
  if (home) walkerFor(taskId, home, to);
}

// ---- boot

async function load(soukUrl: string): Promise<void> {
  try {
    const agents = await fetchAgents(soukUrl);
    stalls = groupByProvider(agents).map((g) => ({
      providerKey: g.providerKey,
      fingerprint: g.fingerprint,
      name: g.providerName,
      agents: g.agents,
      x: 0,
      y: 0,
    }));
    if (stalls.length === 0) {
      note("This souk has no stalls yet.");
      svg().innerHTML = "";
      return;
    }
    if (stalls.length > COLS * ROWS) {
      note(`${stalls.length} stalls, ${COLS * ROWS} squares — the grid is too small for this market.`);
    }
    renderMap();
  } catch (err) {
    note(`Couldn't reach ${soukUrl}: ${err instanceof Error ? err.message : String(err)}`);
  }
}

const initial = renderSoukBar(document.getElementById("souk-bar")!, load);
(document.getElementById("list-link") as HTMLAnchorElement).href = linkWithSouk(
  "index.html",
  initial
);
load(initial);
