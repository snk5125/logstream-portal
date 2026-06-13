import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import type { Source } from '../types'
import ForkWizard from './ForkWizard'

const sources: Source[] = [
  {
    fqn: 'c.s.syslog', name: 'syslog', log_type: 'system', sensitivity: 'standard',
    est_volume_per_min: 900, description: '', columns: [], subscriptions: [],
  },
  {
    fqn: 'c.s.auth_log', name: 'auth_log', log_type: 'system', sensitivity: 'sensitive',
    est_volume_per_min: 300, description: '', columns: [], subscriptions: [],
  },
]

afterEach(() => vi.unstubAllGlobals())

it('warns when the selection includes sensitive sources', () => {
  render(<ForkWizard sources={sources} streams={[]} onClose={() => {}} onDone={() => {}} />)
  expect(screen.getByText(/require admin approval/i)).toBeInTheDocument()
})

it('posts a new-stream payload and calls onDone', async () => {
  const fetchMock = vi.fn().mockResolvedValue({
    ok: true,
    status: 201,
    json: async () => ({}),
  })
  vi.stubGlobal('fetch', fetchMock)
  const onDone = vi.fn()
  render(<ForkWizard sources={sources} streams={[]} onClose={() => {}} onDone={onDone} />)
  fireEvent.change(screen.getByPlaceholderText('stream name'), { target: { value: 'team-a-logs' } })
  fireEvent.click(screen.getByText('Submit'))
  await waitFor(() => expect(onDone).toHaveBeenCalled())
  expect(fetchMock).toHaveBeenCalledWith('/api/streams', expect.objectContaining({ method: 'POST' }))
  const body = JSON.parse((fetchMock.mock.calls[0][1] as RequestInit).body as string)
  expect(body).toEqual({ name: 'team-a-logs', type: 'kinesis', source_fqns: ['c.s.syslog', 'c.s.auth_log'] })
})

it('posts to the existing stream when preset', async () => {
  const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({}) })
  vi.stubGlobal('fetch', fetchMock)
  const streams = [{
    id: 7, name: 'ops', type: 'kinesis' as const, status: 'live' as const, sources: [],
  }]
  render(
    <ForkWizard sources={[sources[0]]} streams={streams} presetStreamId={7}
      onClose={() => {}} onDone={() => {}} />,
  )
  fireEvent.click(screen.getByText('Submit'))
  await waitFor(() => expect(fetchMock).toHaveBeenCalled())
  expect(fetchMock.mock.calls[0][0]).toBe('/api/streams/7/sources')
})
