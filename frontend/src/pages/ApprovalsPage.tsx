import { useEffect, useState } from 'react'
import { api } from '../api'
import type { Approval } from '../types'

export default function ApprovalsPage() {
  const [items, setItems] = useState<Approval[] | null>(null)
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)

  function load() {
    api<Approval[]>('/api/approvals').then(setItems).catch(e => setError(String(e.message ?? e)))
  }
  useEffect(load, [])

  async function decide(id: number, approved: boolean) {
    setBusy(true)
    try {
      await api(`/api/approvals/${id}`, { method: 'POST', body: JSON.stringify({ approved }) })
      load()
    } catch (e) {
      setError(String((e as Error).message ?? e))
    } finally {
      setBusy(false)
    }
  }

  if (!items) return error ? <p className="error">{error}</p> : <p className="muted">Loading…</p>

  return (
    <div>
      <h2>Pending approvals</h2>
      {error && <p className="error">{error}</p>}
      {items.length === 0 && <p className="muted">Queue is empty.</p>}
      {items.map(a => (
        <div className="card" key={a.id}>
          <b>{a.source_fqn}</b> → stream <b>{a.stream_name}</b>
          <div className="muted small">
            requested by {a.requested_by} at {a.requested_at}
          </div>
          <div className="row">
            <button className="primary" disabled={busy} onClick={() => decide(a.id, true)}>Approve</button>
            <button className="danger" disabled={busy} onClick={() => decide(a.id, false)}>Reject</button>
          </div>
        </div>
      ))}
    </div>
  )
}
