import { useEffect, useState } from 'react'
import { Link, Navigate, Route, Routes, useLocation, useNavigate } from 'react-router-dom'
import { api } from './api'
import type { User } from './types'
import ApprovalsPage from './pages/ApprovalsPage'
import CatalogPage from './pages/CatalogPage'
import LoginPage from './pages/LoginPage'
import StreamsPage from './pages/StreamsPage'

export default function App() {
  const [user, setUser] = useState<User | null>(null)
  const [loading, setLoading] = useState(true)
  const location = useLocation()
  const navigate = useNavigate()

  useEffect(() => {
    api<User>('/api/session')
      .then(setUser)
      .catch(() => setUser(null))
      .finally(() => setLoading(false))
  }, [])

  function signOut() {
    api('/api/session', { method: 'DELETE' })
      .then(() => {
        setUser(null)
        navigate('/login', { replace: true })
      })
      .catch(() => setUser(null))
  }

  if (loading) return <p className="muted">Loading…</p>
  if (!user && location.pathname !== '/login') return <Navigate to="/login" replace />

  return (
    <div className="app">
      {user && (
        <nav className="topnav">
          <span className="brand">LogStream Portal</span>
          <Link to="/">Catalog</Link>
          <Link to="/streams">My Streams</Link>
          {user.role === 'admin' && <Link to="/approvals">Approvals</Link>}
          <span className="spacer" />
          <span className="muted">{user.display_name} · {user.team}</span>
          {user.account_scope && (
            <span className="chip">scope: {user.account_scope}</span>
          )}
          <button onClick={signOut}>Sign out</button>
        </nav>
      )}
      <Routes>
        <Route path="/login" element={<LoginPage onLogin={setUser} />} />
        <Route path="/" element={<CatalogPage />} />
        <Route path="/streams" element={<StreamsPage />} />
        <Route path="/approvals" element={<ApprovalsPage />} />
      </Routes>
    </div>
  )
}
