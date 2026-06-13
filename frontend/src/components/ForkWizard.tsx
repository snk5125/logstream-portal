import { useState } from 'react'
import { api } from '../api'
import type { Source, Stream } from '../types'

export default function ForkWizard({ sources, streams, presetStreamId, onClose, onDone }: {
  sources: Source[]
  streams: Stream[]
  presetStreamId?: number
  onClose: () => void
  onDone: () => void
}) {
  const existing = streams.filter(s => s.status !== 'deleted')
  const [mode, setMode] = useState<'new' | 'existing'>(presetStreamId ? 'existing' : 'new')
  const [type, setType] = useState<'kinesis' | 'sqs'>('kinesis')
  const [name, setName] = useState('')
  const [streamId, setStreamId] = useState<number | undefined>(presetStreamId ?? existing[0]?.id)
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const sensitive = sources.filter(s => s.sensitivity === 'sensitive')

  async function submit() {
    setBusy(true)
    setError('')
    const source_fqns = sources.map(s => s.fqn)
    try {
      if (mode === 'new') {
        await api('/api/streams', {
          method: 'POST',
          body: JSON.stringify({ name, type, source_fqns }),
        })
      } else {
        await api(`/api/streams/${streamId}/sources`, {
          method: 'POST',
          body: JSON.stringify({ source_fqns }),
        })
      }
      onDone()
    } catch (e) {
      setError(String((e as Error).message ?? e))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="modal-backdrop">
      <div className="modal">
        <h3>Fork {sources.length} source{sources.length > 1 ? 's' : ''}</h3>
        <p className="muted small">{sources.map(s => s.name).join(', ')}</p>

        <label>
          <input type="radio" checked={mode === 'new'} onChange={() => setMode('new')} />
          {' '}New stream
        </label>
        {mode === 'new' && (
          <div className="indent">
            <select value={type} onChange={e => setType(e.target.value as 'kinesis' | 'sqs')}>
              <option value="kinesis">Kinesis</option>
              <option value="sqs">SQS</option>
            </select>
            <input placeholder="stream name" value={name} onChange={e => setName(e.target.value)} />
          </div>
        )}

        <label>
          <input
            type="radio"
            checked={mode === 'existing'}
            disabled={existing.length === 0}
            onChange={() => setMode('existing')}
          />
          {' '}Add to existing stream
        </label>
        {mode === 'existing' && (
          <div className="indent">
            <select value={streamId} onChange={e => setStreamId(Number(e.target.value))}>
              {existing.map(s => (
                <option key={s.id} value={s.id}>{s.name} ({s.type})</option>
              ))}
            </select>
          </div>
        )}

        {sensitive.length > 0 && (
          <div className="banner warn">
            <b>{sensitive.map(s => s.name).join(', ')}</b>{' '}
            {sensitive.length > 1 ? 'are' : 'is'} sensitive and will require admin approval.
            Standard sources activate immediately.
          </div>
        )}
        {error && <p className="error">{error}</p>}

        <div className="row">
          <button
            className="primary"
            disabled={busy || (mode === 'new' && !name) || (mode === 'existing' && !streamId)}
            onClick={submit}
          >
            Submit
          </button>
          <button onClick={onClose}>Cancel</button>
        </div>
      </div>
    </div>
  )
}
