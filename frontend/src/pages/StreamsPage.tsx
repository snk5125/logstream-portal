import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { api } from '../api'
import PeekModal from '../components/PeekModal'
import StreamCard from '../components/StreamCard'
import type { Stream } from '../types'

export default function StreamsPage() {
  const [streams, setStreams] = useState<Stream[] | null>(null)
  const [error, setError] = useState('')
  const [peeking, setPeeking] = useState<Stream | null>(null)
  const navigate = useNavigate()

  function load() {
    api<Stream[]>('/api/streams').then(setStreams).catch(e => setError(String(e.message ?? e)))
  }
  useEffect(load, [])

  if (error) return <p className="error">{error}</p>
  if (!streams) return <p className="muted">Loading…</p>

  return (
    <div>
      <h2>My Streams</h2>
      {streams.length === 0 && (
        <p className="muted">No streams yet — fork some sources from the Catalog.</p>
      )}
      {streams.map(s => (
        <StreamCard
          key={s.id}
          stream={s}
          onChanged={load}
          onAddSources={() => navigate(`/?dest=${s.id}`)}
          onPeek={() => setPeeking(s)}
        />
      ))}
      {peeking && <PeekModal stream={peeking} onClose={() => setPeeking(null)} />}
    </div>
  )
}
