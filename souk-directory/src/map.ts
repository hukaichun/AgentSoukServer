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
import {
  Sheets,
  StallBox,
  drawGround,
  drawStall,
  drawWalker,
  loadSheets,
} from "./scene.js";

marked.setOptions({ breaks: true, gfm: true });

const SCALE = 2;
const COLS = 3;
const ROWS = 2;
const CELL_W = 320;
const CELL_H = 300;
const STALL_W = 192;
const STALL_H = 128;

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
interface Walk { ax: number; ay: number; bx: number; by: number; t0: number }
const walkers = new Map<string, Walk>();
// Edges of the finished tree, kept as provider-key pairs so the draw loop
// can look up wherever those stalls are standing now.
let routeEdges: { from: string; to: string }[] = [];

const canvas = () => document.getElementById("map") as HTMLCanvasElement;
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

// ---- the scene

let sheets: Sheets | null = null;
let frame = 0;

// Positions every stall from its fingerprint. Pure placement — nothing is
// drawn here, so the draw loop can run at whatever rate it likes without
// re-deciding where the market is.
function layout(): void {
  const placed = placeStalls(
    stalls.map((s) => ({ fingerprint: s.fingerprint, stall: s })),
    COLS,
    ROWS
  );
  for (const p of placed) {
    p.item.stall.x = p.col * CELL_W + (CELL_W - STALL_W) / 2;
    p.item.stall.y = p.row * CELL_H + (CELL_H - STALL_H) / 2;
  }
}

// Hit-testing rather than DOM nodes: on a canvas the stall is a rectangle
// and the keeper is a point, so "who did I click" is arithmetic.
function agentAt(px: number, py: number): AgentRosterEntry | null {
  for (const s of stalls) {
    if (px < s.x - 8 || px > s.x + STALL_W + 8) continue;
    if (py < s.y - 40 || py > s.y + STALL_H + 16) continue;
    const step = STALL_W / (s.agents.length + 1);
    let best: AgentRosterEntry | null = null;
    let bestD = Infinity;
    s.agents.forEach((a, i) => {
      const d = Math.abs(s.x + step * (i + 1) - px);
      if (d < bestD) {
        bestD = d;
        best = a;
      }
    });
    return best;
  }
  return null;
}

function stallBox(s: Stall): StallBox {
  const step = STALL_W / (s.agents.length + 1);
  return {
    x: s.x,
    y: s.y,
    w: STALL_W,
    h: STALL_H,
    sign: s.name || "unnamed provider",
    tone: parseInt(s.fingerprint.slice(0, 2), 16) % 3,
    open: s.agents.some((a) => a.online),
    keepers: s.agents.map((a, i) => ({
      name: a.name,
      online: a.online,
      x: s.x + step * (i + 1),
      y: s.y + STALL_H - 20,
    })),
  };
}

// Everything is redrawn every frame. At this size that is far cheaper than
// tracking what changed, and it means the walkers, the stalls and the
// finished route can never disagree about where anything is.
function draw(): void {
  const c = canvas();
  const g = c.getContext("2d")!;
  if (!sheets) return;
  g.imageSmoothingEnabled = false;
  drawGround(g, sheets, c.width, c.height, SCALE);

  // The finished shape first, so it lies under the stalls rather than
  // across their signs.
  for (const e of routeEdges) {
    const from = findStall(e.from);
    const to = findStall(e.to);
    if (!from || !to) continue;
    const a = stallCentre(from);
    const b = stallCentre(to);
    g.save();
    g.strokeStyle = "rgba(214, 168, 74, 0.85)";
    g.lineWidth = 3;
    g.setLineDash([7, 6]);
    g.beginPath();
    g.moveTo(a.x, a.y + STALL_H / 2);
    g.lineTo(b.x, b.y + STALL_H / 2);
    g.stroke();
    g.restore();
  }

  for (const s of [...stalls].sort((a, b) => a.y - b.y)) drawStall(g, sheets, stallBox(s), SCALE);

  const now = performance.now();
  for (const w of walkers.values()) {
    const t = Math.min(1, (now - w.t0) / 1400);
    const eased = t * t * (3 - 2 * t);
    const x = w.ax + (w.bx - w.ax) * eased;
    const y = w.ay + (w.by - w.ay) * eased;
    drawWalker(g, sheets, x, y, w.bx >= w.ax ? 1 : -1, Math.floor(frame / 7), SCALE);
  }
  frame++;
  requestAnimationFrame(draw);
}

// ---- walkers: motion while the run is still in flight

function walkerFor(taskId: string, from: Stall, to: Stall): void {
  if (walkers.has(taskId)) return;
  // Work that never left the stall is not a journey. Two people behind one
  // counter passing something between them should not send anybody across
  // the market, and drawing it would be the map's version of the route
  // strip's invented edge.
  if (from.providerKey === to.providerKey) return;
  const a = stallCentre(from);
  const b = stallCentre(to);
  walkers.set(taskId, {
    ax: a.x,
    ay: a.y + STALL_H / 2,
    bx: b.x,
    by: b.y + STALL_H / 2,
    t0: performance.now(),
  });
}

function retireWalker(taskId: string, _home: Stall | undefined): void {
  const w = walkers.get(taskId);
  if (!w) return;
  // Walk back the way they came, then go.
  walkers.set(taskId, { ax: w.bx, ay: w.by, bx: w.ax, by: w.ay, t0: performance.now() });
  setTimeout(() => walkers.delete(taskId), 1500);
}

// ---- the finished shape, drawn from the tree rather than guessed live

function collectEdges(root: ThreadTreeNode): void {
  const out: { from: string; to: string }[] = [];
  const walk = (node: ThreadTreeNode) => {
    for (const child of node.children) {
      // Same-stall hops are real but have no distance, so they get no line.
      // The map says nothing rather than drawing a journey nobody took.
      if (child.provider_key !== node.provider_key) {
        out.push({ from: node.provider_key, to: child.provider_key });
      }
      walk(child);
    }
  };
  walk(root);
  routeEdges = out;
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
  routeEdges = [];
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
    collectEdges(root);
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
      return;
    }
    if (stalls.length > COLS * ROWS) {
      note(`${stalls.length} stalls, ${COLS * ROWS} squares — the grid is too small for this market.`);
    }
    layout();
    if (!sheets) {
      sheets = await loadSheets();
      const c = canvas();
      c.addEventListener("click", (e) => {
        const r = c.getBoundingClientRect();
        const a = agentAt(
          ((e.clientX - r.left) / r.width) * c.width,
          ((e.clientY - r.top) / r.height) * c.height
        );
        if (a) openChat(a);
      });
      requestAnimationFrame(draw);
    }
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
