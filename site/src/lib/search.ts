export const num = (v: unknown): number | undefined => {
  const n = Number(v);
  return v !== undefined && v !== '' && Number.isFinite(n) && n > 0 ? Math.floor(n) : undefined;
};
export const str = (v: unknown): string | undefined =>
  v === undefined || v === null || v === '' ? undefined : String(v);

/** Build a URL with a query string, skipping empty values. */
export function href(path: string, search: Record<string, string | number | undefined>): string {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(search))
    if (v !== undefined && v !== '' && !(k === 'page' && v === 1)) q.set(k, String(v));
  const s = q.toString();
  return s ? `${path}?${s}` : path;
}
