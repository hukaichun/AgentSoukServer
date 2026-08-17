// Where a stall stands.
//
// The whole point of the market view is that a stall has a *place*, and the
// place has to be the same every time you open the map — otherwise the
// picture teaches you nothing you can carry between visits. So the position
// is derived from identity and from nothing else: no stored coordinates, no
// server state, no layout table anyone has to keep.
//
// `fingerprint` is sha256(provider_key)[:16], and souk guarantees it is
// unique across providers. That gives a stable seed for free. What it does
// *not* give is a unique grid cell — there are far fewer cells than
// fingerprints — so two stalls can want the same square, and something has
// to break the tie deterministically.

export interface Placed<T> {
  item: T;
  col: number;
  row: number;
}

// A deterministic 32-bit hash of the fingerprint hex. Not cryptographic and
// does not need to be: the fingerprint already did that work, and this only
// has to spread 16 hex chars over a small grid without clumping.
export function hashFingerprint(fingerprint: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < fingerprint.length; i++) {
    h ^= fingerprint.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h >>> 0;
}

/**
 * Places stalls on a `cols` x `rows` grid, keyed on fingerprint.
 *
 * Stability, stated exactly, because a weaker claim would be a lie: a stall's
 * square depends on its own fingerprint and on which *earlier-sorted* stalls
 * collided with it. Placement runs in fingerprint order rather than roster
 * order, so a re-poll that returns the same stalls always yields the same
 * map regardless of how the roster happened to be sorted. A stall arriving
 * or leaving can move a stall that collided with it — that is the price of
 * a grid with fewer cells than possible identities, and it is paid only on
 * collision, not on every change.
 *
 * Deliberately not solved by storing positions: the moment a coordinate is
 * persisted somewhere, the map needs a database, a migration, and an answer
 * for what happens when two deployments disagree. A pure function of the
 * identity has none of those problems.
 */
export function placeStalls<T extends { fingerprint: string }>(
  stalls: T[],
  cols: number,
  rows: number
): Placed<T>[] {
  const taken = new Set<number>();
  const cells = cols * rows;
  const sorted = [...stalls].sort((a, b) => a.fingerprint.localeCompare(b.fingerprint));

  return sorted.map((item) => {
    const h = hashFingerprint(item.fingerprint);
    let cell = h % cells;
    // Linear probing with a fingerprint-derived stride, so two stalls that
    // collide do not then walk the grid together and collide again.
    const stride = 1 + ((h >>> 8) % (cells - 1 || 1));
    for (let i = 0; i < cells && taken.has(cell); i++) {
      cell = (cell + stride) % cells;
    }
    taken.add(cell);
    return { item, col: cell % cols, row: Math.floor(cell / cols) };
  });
}
