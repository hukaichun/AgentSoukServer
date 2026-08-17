// Drawing a souk out of a forest.
//
// The art is ansimuz's CC0 Tiny RPG Forest pack (see assets/CREDITS.md) —
// which contains no market whatsoever. What it does contain is dirt, rugs,
// planks, fence posts and a person, and a stall is those five things
// arranged: a rug for the pitch, planks for the counter, posts holding a
// canopy, someone standing behind it. The canopy is the one thing drawn
// rather than blitted, because no pack has an awning in the right colours
// and a striped quad at 16px reads as canvas anyway.
//
// Tile coordinates below are in 16px cells, read off the tileset once with
// a labelled grid rather than guessed.

export const TILE = 16;

// prettier-ignore
const T = {
  dirt:    { x: 13, y: 18, w: 1, h: 1 },
  tuft:    [{ x: 20, y: 12, w: 1, h: 1 }, { x: 22, y: 14, w: 1, h: 1 },
            { x: 24, y: 16, w: 1, h: 1 }, { x: 21, y: 16, w: 1, h: 1 }],
  grass:   { x: 13, y: 30, w: 3, h: 1 },
  bush:    { x: 16, y: 20, w: 2, h: 2 },
  // A bordered rug, blitted whole as the stall's pitch.
  rug:     { x: 17, y: 24, w: 4, h: 4 },
  // Horizontal boards — the counter.
  boards:  { x: 31, y: 19, w: 2, h: 1 },
  // A single upright post.
  post:    { x: 33, y: 21, w: 1, h: 3 },
};

export interface Sheets {
  tiles: HTMLImageElement;
  keeper: HTMLImageElement;
  walker: HTMLImageElement;
}

export function loadSheets(): Promise<Sheets> {
  const one = (src: string) =>
    new Promise<HTMLImageElement>((resolve, reject) => {
      const img = new Image();
      img.onload = () => resolve(img);
      img.onerror = () => reject(new Error(`could not load ${src}`));
      img.src = src;
    });
  return Promise.all([
    one("assets/tileset.png"),
    one("assets/keeper.png"),
    one("assets/walker.png"),
  ]).then(([tiles, keeper, walker]) => ({ tiles, keeper, walker }));
}

function blit(
  g: CanvasRenderingContext2D,
  sheet: HTMLImageElement,
  t: { x: number; y: number; w: number; h: number },
  dx: number,
  dy: number,
  scale: number
): void {
  g.drawImage(
    sheet,
    t.x * TILE,
    t.y * TILE,
    t.w * TILE,
    t.h * TILE,
    Math.round(dx),
    Math.round(dy),
    t.w * TILE * scale,
    t.h * TILE * scale
  );
}

// A deterministic per-cell pseudo-random, so the scattered ground detail is
// the same every visit. A market that rearranges its own gravel each time
// you open it would undercut the one thing this view is claiming: that what
// you see is derived from what is there, not from when you looked.
function noise(x: number, y: number): number {
  let h = (x * 374761393 + y * 668265263) >>> 0;
  h = Math.imul(h ^ (h >>> 13), 1274126177) >>> 0;
  return ((h ^ (h >>> 16)) >>> 0) / 4294967296;
}

export function drawGround(
  g: CanvasRenderingContext2D,
  s: Sheets,
  w: number,
  h: number,
  scale: number
): void {
  const step = TILE * scale;
  for (let y = 0; y < h; y += step) {
    for (let x = 0; x < w; x += step) {
      blit(g, s.tiles, T.dirt, x, y, scale);
    }
  }
  // Scattered detail, thinly — enough that the ground is not a flat field,
  // sparse enough that it never competes with a stall for attention.
  for (let y = 0; y < h; y += step) {
    for (let x = 0; x < w; x += step) {
      const n = noise(x / step, y / step);
      if (n > 0.93) blit(g, s.tiles, T.tuft[Math.floor(n * 997) % T.tuft.length], x, y, scale);
      else if (n < 0.022) blit(g, s.tiles, T.bush, x, y, scale);
    }
  }
}

export interface StallBox {
  x: number;
  y: number;
  w: number;
  h: number;
  sign: string;
  tone: number;
  open: boolean;
  keepers: { name: string; online: boolean; x: number; y: number }[];
}

const AWNING_TONES = [
  ["#b4523c", "#efe3cc"],
  ["#2f6b64", "#efe3cc"],
  ["#c0902f", "#efe3cc"],
];

export function drawStall(
  g: CanvasRenderingContext2D,
  s: Sheets,
  b: StallBox,
  scale: number
): void {
  const px = TILE * scale;

  // Pitch: the rug, tiled to the stall footprint.
  for (let y = 0; y < b.h; y += px * 4) {
    for (let x = 0; x < b.w; x += px * 4) {
      g.save();
      g.beginPath();
      g.rect(b.x, b.y, b.w, b.h);
      g.clip();
      blit(g, s.tiles, T.rug, b.x + x, b.y + y, scale);
      g.restore();
    }
  }

  // Keepers stand on the rug, behind the counter.
  for (const k of b.keepers) {
    g.globalAlpha = k.online ? 1 : 0.4;
    g.drawImage(
      s.keeper,
      0,
      0,
      32,
      32,
      Math.round(k.x - 16 * scale),
      Math.round(k.y - 32 * scale),
      32 * scale,
      32 * scale
    );
    g.globalAlpha = 1;
  }

  // Counter: boards along the front edge.
  for (let x = 0; x < b.w; x += px * 2) {
    g.save();
    g.beginPath();
    g.rect(b.x, b.y, b.w, b.h);
    g.clip();
    blit(g, s.tiles, T.boards, b.x + x, b.y + b.h - px, scale);
    g.restore();
  }

  // Posts at the front corners, holding the canopy up.
  blit(g, s.tiles, T.post, b.x - px * 0.15, b.y - px * 0.35, scale);
  blit(g, s.tiles, T.post, b.x + b.w - px * 0.85, b.y - px * 0.35, scale);

  // Canopy. Drawn, not blitted — no pack has one, and stripes at this size
  // are four rectangles and a shadow.
  const [a, c] = AWNING_TONES[b.tone % AWNING_TONES.length];
  const ah = px * 0.62;
  const ay = b.y - ah;
  const stripes = Math.max(8, Math.round(b.w / (px * 0.34)));
  const sw = (b.w + px * 0.5) / stripes;
  g.globalAlpha = b.open ? 1 : 0.45;
  for (let i = 0; i < stripes; i++) {
    g.fillStyle = i % 2 ? c : a;
    g.fillRect(Math.round(b.x - px * 0.25 + i * sw), Math.round(ay), Math.ceil(sw), Math.round(ah));
  }
  g.fillStyle = "rgba(0,0,0,0.28)";
  g.fillRect(Math.round(b.x - px * 0.25), Math.round(ay + ah - 3), Math.round(b.w + px * 0.5), 3);
  g.globalAlpha = 1;

  // The sign hangs on the canopy.
  g.font = `600 ${Math.round(10 * scale)}px ui-monospace, monospace`;
  g.textAlign = "center";
  const tw = g.measureText(b.sign).width;
  const sy = ay - px * 0.32;
  // A hanging board, so the name sits on something instead of floating on
  // the stripes it is least legible against.
  g.fillStyle = "#5a3a22";
  g.fillRect(Math.round(b.x + b.w / 2 - tw / 2 - 7), Math.round(sy - 11 * scale), Math.round(tw + 14), Math.round(15 * scale));
  g.fillStyle = "#f4ecd8";
  g.fillText(b.sign, b.x + b.w / 2, sy);

  // Names under the counter, so a stall reads as people and not just a shop.
  g.font = `${Math.round(8 * scale)}px ui-monospace, monospace`;
  b.keepers.forEach((k, i) => {
    // Staggered rows. Two keepers at one counter have names wider than the
    // space between them, and overlapping text is worse than a second line.
    const ty = b.y + b.h + (9 + (i % 2) * 9) * scale;
    g.lineWidth = 3;
    g.strokeStyle = "rgba(0,0,0,0.55)";
    g.strokeText(k.name, k.x, ty);
    g.fillStyle = k.online ? "#f4ecd8" : "#b9ad96";
    g.fillText(k.name, k.x, ty);
  });
  g.textAlign = "start";
}

// A customer crossing the market. Four frames of a side-walk sheet, flipped
// when heading left, so the figure faces where it is going.
export function drawWalker(
  g: CanvasRenderingContext2D,
  s: Sheets,
  x: number,
  y: number,
  dir: number,
  frame: number,
  scale: number
): void {
  const frames = Math.max(1, Math.floor(s.walker.width / 32));
  const f = frame % frames;
  g.save();
  g.translate(Math.round(x), Math.round(y));
  if (dir < 0) g.scale(-1, 1);
  g.drawImage(s.walker, f * 32, 0, 32, 32, -16 * scale, -30 * scale, 32 * scale, 32 * scale);
  g.restore();
}
