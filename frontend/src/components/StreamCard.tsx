import { useState } from 'react'
import { api } from '../api'
import type { Stream } from '../types'

const STATUS_CHIP: Record<string, string> = {
  active: 'chip ok',
  pending_approval: 'chip warn',
  rejected: 'chip err',
}

export default function StreamCard({ stream, onChanged, onAddSources, onPeek }: {
  stream: Stream
  onChanged: () => void
  onAddSources: () => void
  onPeek: () => void
}) {
  const [error, setError] = useState('')

  async function removeSource(fqn: string) {
    try {
      await api(`/api/streams/${stream.id}/sources/${encodeURIComponent(fqn)}`, { method: 'DELETE' })
      onChanged()
    } catch (e) {
      setError(String((e as Error).message ?? e))
    }
  }

  async function deleteStream() {
    if (!confirm(`Delete stream "${stream.name}" and its ${stream.type} resource?`)) return
    try {
      await api(`/api/streams/${stream.id}`, { method: 'DELETE' })
      onChanged()
    } catch (e) {
      setError(String((e as Error).message ?? e))
    }
  }

  async function retry() {
    try {
      await api(`/api/streams/${stream.id}/retry`, { method: 'POST' })
      onChanged()
    } catch (e) {
      setError(String((e as Error).message ?? e))
    }
  }

  async function downloadAccess() {
    try {
      const bundle = await api<unknown>(`/api/streams/${stream.id}/access-bundle`)
      const blob = new Blob([JSON.stringify(bundle, null, 2)], { type: 'application/json' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `${stream.name}-access.json`
      a.click()
      URL.revokeObjectURL(url)
    } catch (e) {
      setError(String((e as Error).message ?? e))
    }
  }

  return (
    <div className="card">
      <div className="card-head">
        <b>{stream.name}</b>
        <span className="chip">{stream.type}</span>
        <span className={stream.status === 'live' ? 'chip ok' : stream.status === 'error' ? 'chip err' : 'chip warn'}>{stream.status}</span>
        {stream.flow && (
          <span className="muted small">{stream.flow.recent_records} recent records</span>
        )}
      </div>
      {stream.last_error && (
        <p className="error small">
          {stream.last_error} <button onClick={retry}>Retry</button>
        </p>
      )}
      {error && <p className="error small">{error}</p>}
      <ul>
        {stream.sources.map(src => (
          <li key={src.id}>
            {src.workload}/{src.source_name}
            <span className={STATUS_CHIP[src.status] ?? 'chip'}>{src.status}</span>
            <button className="link" onClick={() => removeSource(src.source_fqn)}>remove</button>
          </li>
        ))}
      </ul>
      <div className="row">
        <button onClick={onAddSources}>+ Add sources</button>
        <button onClick={onPeek} disabled={stream.status !== 'live'}>Peek</button>
        <button
          onClick={downloadAccess}
          disabled={stream.status !== 'live' || !stream.read_role_arn}
        >
          Download access
        </button>
        <button className="danger" onClick={deleteStream}>Delete stream</button>
      </div>
    </div>
  )
}
