//
// Copyright © 2026 Hardcore Engineering Inc.
//
// Licensed under the Eclipse Public License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may
// obtain a copy of the License at https://www.eclipse.org/legal/epl-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//
// See the License for the specific language governing permissions and
// limitations under the License.
//

import { ClientSocketReadyState, type ClientSocket } from '@hcengineering/client'
import { MeasureMetricsContext, type PersonUuid, type Tx, type WorkspaceUuid } from '@hcengineering/core'
import { connect } from '../connection'

class ControlledSocket implements ClientSocket {
  readyState = ClientSocketReadyState.OPEN
  onmessage: ((this: ClientSocket, ev: MessageEvent) => unknown) | null = null
  onclose: ((this: ClientSocket, ev: CloseEvent) => unknown) | null = null
  onopen: ((this: ClientSocket, ev: Event) => unknown) | null = null
  onerror: ((this: ClientSocket, ev: Event) => unknown) | null = null
  bufferedAmount = 0

  constructor (readonly url: string) {}

  send (): void {}

  close (): void {
    this.readyState = ClientSocketReadyState.CLOSED
  }

  receive (data: string | Blob): void {
    this.onmessage?.call(this, { data } as MessageEvent)
  }
}

interface AuditableConnection {
  scheduleOpen: (ctx: MeasureMetricsContext, force: boolean) => void
  close: () => Promise<void>
}

describe('Connection Blob dispatch', () => {
  it('drops a decoded message when its socket was replaced', async () => {
    const sockets: ControlledSocket[] = []
    const received: number[] = []
    const ctx = new MeasureMetricsContext('stale-blob', {})
    const client = connect(
      'ws://stale-blob',
      (...transactions: Tx[]) => {
        received.push(...transactions.map((tx) => (tx as unknown as { version: number }).version))
      },
      'test-workspace' as WorkspaceUuid,
      'test-user' as PersonUuid,
      {
        ctx,
        socketFactory: (url) => {
          const socket = new ControlledSocket(url)
          sockets.push(socket)
          return socket
        }
      }
    ) as unknown as AuditableConnection
    let finishDecode: ((value: ArrayBuffer) => void) | undefined
    const decoding = new Promise<ArrayBuffer>((resolve) => {
      finishDecode = resolve
    })
    const oldPayload = new Blob([])
    Object.defineProperty(oldPayload, 'arrayBuffer', { value: () => decoding })

    sockets[0].receive(oldPayload)
    client.scheduleOpen(ctx, true)
    sockets[1].receive(JSON.stringify({ result: { version: 2 } }))
    finishDecode?.(new TextEncoder().encode(JSON.stringify({ result: { version: 1 } })).buffer)
    await decoding
    await new Promise<void>((resolve) => setTimeout(resolve, 0))

    expect(received).toEqual([2])
    await client.close()
  })
})
