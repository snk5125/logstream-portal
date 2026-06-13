import type { Source } from '../types'

export default function SourcesTable({ sources, checked, onToggle }: {
  sources: Source[]
  checked: Set<string>
  onToggle: (fqn: string) => void
}) {
  return (
    <table className="sources">
      <thead>
        <tr>
          <th />
          <th>Source</th>
          <th>Type</th>
          <th>Est. volume</th>
          <th>Sensitivity</th>
          <th>Subscribed</th>
        </tr>
      </thead>
      <tbody>
        {sources.map(s => (
          <tr key={s.fqn}>
            <td>
              <input
                type="checkbox"
                aria-label={`select ${s.name}`}
                checked={checked.has(s.fqn)}
                onChange={() => onToggle(s.fqn)}
              />
            </td>
            <td>
              <b>{s.name}</b>
              {s.origin === 'cribl' && (
                <span
                  className="chip"
                  title="Discovered live from Cribl — pending classification in Unity Catalog"
                  style={{ marginLeft: 6 }}
                >
                  discovered
                </span>
              )}
              <div className="muted small">{s.description}</div>
            </td>
            <td>{s.log_type}</td>
            <td>{s.est_volume_per_min}/min</td>
            <td>
              <span className={s.sensitivity === 'sensitive' ? 'chip warn' : 'chip'}>
                {s.sensitivity}
              </span>
            </td>
            <td>
              {s.subscriptions.length === 0
                ? '—'
                : s.subscriptions.map(x => `${x.stream_name} (${x.status})`).join(', ')}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
