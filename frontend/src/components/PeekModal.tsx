import { useEffect, useState } from 'react'
import { api } from '../api'
import type { Stream } from '../types'

export default function PeekModal({ stream, onClose }: { stream: Stream; onClose: () => void }) {
  const [records, setRecords] = useState<unknown[] | null>(null)
  const [error, setError] = useState('')

  useEffect(() => {
    api<unknown[]>(`/api/streams/${stream.id}/peek`)
      .then(setRecords)
      .catch(e => setError(String((e as Error).message ?? e)))
  }, [stream.id])

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal wide" onClick={e => e.stopPropagation()}>
        <h3>Peek: {stream.name}</h3>
        {error && <p className="error">{error}</p>}
        {!records && !error && <p className="muted">Reading stream…</p>}
        {records && records.length === 0 && (
          <p className="muted">No records yet — logs flow in within a few seconds of activation.</p>
        )}
        {records && records.map((r, i) => (
          <pre key={i}>{JSON.stringify(r, null, 2)}</pre>
        ))}
        <button onClick={onClose}>Close</button>
      </div>
    </div>
  )
}
