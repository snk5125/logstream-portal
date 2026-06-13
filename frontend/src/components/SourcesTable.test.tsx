import { fireEvent, render, screen } from '@testing-library/react'
import type { Source } from '../types'
import SourcesTable from './SourcesTable'

const sources: Source[] = [
  {
    fqn: 'c.s.syslog', name: 'syslog', log_type: 'system', sensitivity: 'standard',
    est_volume_per_min: 900, description: 'host syslog', columns: [], subscriptions: [],
  },
  {
    fqn: 'c.s.auth_log', name: 'auth_log', log_type: 'system', sensitivity: 'sensitive',
    est_volume_per_min: 300, description: 'auth events', columns: [],
    subscriptions: [{ stream_id: 1, stream_name: 'ops', status: 'active' }],
  },
]

it('renders sources with sensitivity chips and subscription refs', () => {
  render(<SourcesTable sources={sources} checked={new Set()} onToggle={() => {}} />)
  expect(screen.getByText('syslog')).toBeInTheDocument()
  expect(screen.getByText('sensitive')).toBeInTheDocument()
  expect(screen.getByText('ops (active)')).toBeInTheDocument()
})

it('reports toggles through the callback', () => {
  const onToggle = vi.fn()
  render(<SourcesTable sources={sources} checked={new Set()} onToggle={onToggle} />)
  fireEvent.click(screen.getByLabelText('select syslog'))
  expect(onToggle).toHaveBeenCalledWith('c.s.syslog')
})
