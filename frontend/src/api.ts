export async function api<T>(path: string, opts?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    headers: opts?.body ? { 'Content-Type': 'application/json' } : {},
    credentials: 'same-origin',
    ...opts,
  })
  if (!res.ok) {
    const detail = await res
      .json()
      .then((b: { detail?: string }) => b.detail)
      .catch(() => undefined)
    throw new Error(detail ?? `${res.status} ${res.statusText}`)
  }
  if (res.status === 204) return undefined as T
  return res.json() as Promise<T>
}
