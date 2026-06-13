import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { api } from '../api'
import type { User } from '../types'

export default function LoginPage({ onLogin }: { onLogin: (u: User) => void }) {
  const [personas, setPersonas] = useState<User[]>([])
  const [error, setError] = useState('')
  const navigate = useNavigate()

  useEffect(() => {
    api<User[]>('/api/personas').then(setPersonas).catch(e => setError(String(e.message ?? e)))
  }, [])

  async function pick(id: string) {
    const user = await api<User>('/api/session', {
      method: 'POST',
      body: JSON.stringify({ user_id: id }),
    })
    onLogin(user)
    navigate('/')
  }

  return (
    <div className="login">
      <h1>LogStream Portal</h1>
      <p className="muted">Pick a persona to continue (demo login)</p>
      {error && <p className="error">{error}</p>}
      {personas.map(p => (
        <button key={p.id} className="persona" onClick={() => pick(p.id).catch(e => setError(String(e.message ?? e)))}>
          <b>{p.display_name}</b> — {p.id}
          <span className={p.role === 'admin' ? 'chip warn' : 'chip'}>{p.role}</span>
        </button>
      ))}
    </div>
  )
}
