import { render, screen } from '@testing-library/react'
import type { Stream } from '../types'
import StreamCard from './StreamCard'

const base: Stream = {
  id: 1, name: 's1', type: 'kinesis', status: 'live', sources: [],
  read_role_arn: 'arn:aws:iam::337394138208:role/logstream/logstream-read-1-s1',
}

it('enables Download access for live streams with a role', () => {
  render(<StreamCard stream={base} onChanged={() => {}} onAddSources={() => {}} onPeek={() => {}} />)
  expect(screen.getByText('Download access')).toBeEnabled()
})

it('disables Download access when no role exists', () => {
  render(<StreamCard stream={{ ...base, read_role_arn: null }} onChanged={() => {}}
    onAddSources={() => {}} onPeek={() => {}} />)
  expect(screen.getByText('Download access')).toBeDisabled()
})
