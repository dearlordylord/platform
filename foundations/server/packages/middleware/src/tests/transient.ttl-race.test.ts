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

import core, {
  DOMAIN_TRANSIENT,
  MeasureMetricsContext,
  TxFactory,
  type Class,
  type Doc,
  type MeasureContext,
  type Ref,
  type SessionData,
  type Space,
  type Tx
} from '@hcengineering/core'
import type { Middleware, PipelineContext } from '@hcengineering/server-core'
import { TransientMiddleware } from '../transient'

interface AuditableTransientMiddleware {
  ttlObjectMap: Map<Ref<Doc>, number>
}

describe('TransientMiddleware TTL cleanup', () => {
  it('does not clean a document renewed while the expiry load is pending', async () => {
    const ctx = new MeasureMetricsContext('transient-ttl-race', {})
    const documentClass = 'test:class:Transient' as Ref<Class<Doc>>
    const documentId = 'test:doc:renewed' as Ref<Doc>
    const document = {
      _id: documentId,
      _class: documentClass,
      space: 'test:space' as Ref<Space>,
      modifiedBy: core.account.System,
      modifiedOn: 1
    }
    let releaseLoad: (() => void) | undefined
    let loadStarted: (() => void) | undefined
    const loadStartedPromise = new Promise<void>((resolve) => {
      loadStarted = resolve
    })
    const loadBarrier = new Promise<void>((resolve) => {
      releaseLoad = resolve
    })
    const clean = jest.fn(async () => {})
    const adapter = {
      load: jest.fn(async () => {
        loadStarted?.()
        await loadBarrier
        return [document]
      }),
      clean
    }
    const context = {
      modelDb: { findAllSync: () => [{ _id: documentClass, ttl: 30 }] },
      hierarchy: { findDomain: () => DOMAIN_TRANSIENT },
      adapterManager: { getAdapter: () => adapter },
      broadcastEvent: jest.fn(async () => {})
    } as unknown as PipelineContext
    const middleware = await TransientMiddleware.create(ctx, context, undefined)
    const auditable = middleware as unknown as AuditableTransientMiddleware
    auditable.ttlObjectMap.set(documentId, 0)

    await middleware.checkTTL()
    await loadStartedPromise
    const renewal = new TxFactory(core.account.System).createTxUpdateDoc(
      documentClass,
      document.space,
      documentId,
      {}
    )
    await middleware.tx(ctx as unknown as MeasureContext<SessionData>, [renewal])
    releaseLoad?.()
    await new Promise<void>((resolve) => setImmediate(resolve))

    expect(clean).not.toHaveBeenCalled()
    expect(auditable.ttlObjectMap.get(documentId)).toBeGreaterThan(middleware.now)
    await middleware.close()
  })

  it('still cleans an expired document when its TTL entry is unchanged', async () => {
    const ctx = new MeasureMetricsContext('transient-ttl-expired', {})
    const documentClass = 'test:class:Transient' as Ref<Class<Doc>>
    const documentId = 'test:doc:expired' as Ref<Doc>
    let cleanFinished: (() => void) | undefined
    const cleanFinishedPromise = new Promise<void>((resolve) => {
      cleanFinished = resolve
    })
    const clean = jest.fn(async () => {
      cleanFinished?.()
    })
    const context = {
      modelDb: { findAllSync: () => [{ _id: documentClass, ttl: 30 }] },
      hierarchy: { findDomain: () => DOMAIN_TRANSIENT },
      adapterManager: {
        getAdapter: () => ({
          load: async () => [
            {
              _id: documentId,
              _class: documentClass,
              space: 'test:space' as Ref<Space>,
              modifiedBy: core.account.System,
              modifiedOn: 1
            }
          ],
          clean
        })
      },
      broadcastEvent: jest.fn(async () => {})
    } as unknown as PipelineContext
    const middleware = await TransientMiddleware.create(ctx, context, undefined)
    const auditable = middleware as unknown as AuditableTransientMiddleware
    auditable.ttlObjectMap.set(documentId, 0)

    await middleware.checkTTL()
    await cleanFinishedPromise

    expect(clean).toHaveBeenCalledWith(ctx, DOMAIN_TRANSIENT, [documentId])
    expect(auditable.ttlObjectMap.has(documentId)).toBe(false)
    await middleware.close()
  })

  it('does not queue unrelated transactions behind TTL storage work', async () => {
    const ctx = new MeasureMetricsContext('transient-ttl-contention', {})
    const documentClass = 'test:class:Transient' as Ref<Class<Doc>>
    const persistentClass = 'test:class:Persistent' as Ref<Class<Doc>>
    const documentId = 'test:doc:transient' as Ref<Doc>
    let releaseTransientTx: (() => void) | undefined
    let transientTxStarted: (() => void) | undefined
    const transientTxStartedPromise = new Promise<void>((resolve) => {
      transientTxStarted = resolve
    })
    const transientTxBarrier = new Promise<void>((resolve) => {
      releaseTransientTx = resolve
    })
    const next = {
      tx: jest.fn(async (_ctx: MeasureContext<SessionData>, txes: Tx[]) => {
        if ((txes[0] as { objectClass?: Ref<Class<Doc>> } | undefined)?.objectClass === documentClass) {
          transientTxStarted?.()
          await transientTxBarrier
        }
        return {}
      })
    } as unknown as Middleware
    const context = {
      modelDb: { findAllSync: () => [{ _id: documentClass, ttl: 30 }] },
      hierarchy: { findDomain: () => DOMAIN_TRANSIENT }
    } as unknown as PipelineContext
    const middleware = await TransientMiddleware.create(ctx, context, next)
    const factory = new TxFactory(core.account.System)
    const transientTx = factory.createTxUpdateDoc(documentClass, core.space.Workspace, documentId, {})
    const persistentTx = factory.createTxUpdateDoc(
      persistentClass,
      core.space.Workspace,
      'test:doc:persistent' as Ref<Doc>,
      {}
    )

    const pendingTransientTx = middleware.tx(ctx as unknown as MeasureContext<SessionData>, [transientTx])
    await transientTxStartedPromise
    await middleware.tx(ctx as unknown as MeasureContext<SessionData>, [persistentTx])

    expect(next.tx).toHaveBeenCalledTimes(2)
    releaseTransientTx?.()
    await pendingTransientTx
    await middleware.close()
  })
})
