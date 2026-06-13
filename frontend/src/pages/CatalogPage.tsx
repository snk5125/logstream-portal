import { useEffect, useState } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { api } from '../api'
import ForkWizard from '../components/ForkWizard'
import SourcesTable from '../components/SourcesTable'
import type { Catalog, Stream } from '../types'

export default function CatalogPage() {
  const [catalog, setCatalog] = useState<Catalog | null>(null)
  const [streams, setStreams] = useState<Stream[]>([])
  const [error, setError] = useState('')
  const [selected, setSelected] = useState({ account: 0, workload: 0 })
  const [checked, setChecked] = useState<Set<string>>(new Set())
  const [wizardOpen, setWizardOpen] = useState(false)
  const [params] = useSearchParams()
  const navigate = useNavigate()
  const destParam = Number(params.get('dest'))
  const destId = Number.isFinite(destParam) && destParam > 0 ? destParam : undefined

  function load() {
    api<Catalog>('/api/catalog').then(setCatalog).catch(e => setError(String(e.message ?? e)))
    api<Stream[]>('/api/streams').then(setStreams).catch(() => {})
  }
  useEffect(load, [])

  if (error) return <p className="error">{error}</p>
  if (!catalog) return <p className="muted">Loading catalog…</p>

  const workload = catalog.accounts[selected.account]?.workloads[selected.workload]
  const allSources = catalog.accounts.flatMap(a => a.workloads.flatMap(w => w.sources))
  const checkedSources = allSources.filter(s => checked.has(s.fqn))

  function toggle(fqn: string) {
    setChecked(prev => {
      const next = new Set(prev)
      if (next.has(fqn)) next.delete(fqn)
      else next.add(fqn)
      return next
    })
  }

  return (
    <div>
      {catalog.stale && (
        <div className="banner">
          Catalog as of {catalog.as_of} — Databricks unreachable, showing cached snapshot.
        </div>
      )}
      {destId !== undefined && (
        <div className="banner">
          Adding sources to stream #{destId} — select below and hit Fork.
        </div>
      )}
      <div className="catalog">
        <aside>
          {catalog.accounts.map((account, ai) => (
            <div key={account.account_id}>
              <div className="account-header">
                {account.account_alias} · {account.account_id}
              </div>
              {account.workloads.map((w, wi) => (
                <button
                  key={w.schema}
                  className={ai === selected.account && wi === selected.workload ? 'wl active' : 'wl'}
                  onClick={() => setSelected({ account: ai, workload: wi })}
                >
                  {w.name}
                </button>
              ))}
            </div>
          ))}
        </aside>
        <main>
          {workload && (
            <>
              <h2>
                {workload.name} <span className="muted small">({workload.environment})</span>
              </h2>
              <SourcesTable sources={workload.sources} checked={checked} onToggle={toggle} />
            </>
          )}
          <button className="primary" disabled={checked.size === 0} onClick={() => setWizardOpen(true)}>
            Fork {checked.size} selected →
          </button>
        </main>
      </div>
      {wizardOpen && (
        <ForkWizard
          sources={checkedSources}
          streams={streams}
          presetStreamId={destId}
          onClose={() => setWizardOpen(false)}
          onDone={() => navigate('/streams')}
        />
      )}
    </div>
  )
}
