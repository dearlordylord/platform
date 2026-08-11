//
// Copyright © 2026 Intabia Fusion
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
//
// A middleware to support TTL for transient objects.
import core, {
  DOMAIN_TRANSIENT,
  reduceCalls,
  TxFactory,
  TxProcessor,
  type Class,
  type Doc,
  type MeasureContext,
  type Ref,
  type SessionData,
  type Tx,
  type TxCUD
} from '@hcengineering/core'
import {
  BaseMiddleware,
  type DbAdapter,
  type Middleware,
  type PipelineContext,
  type TxMiddlewareResult
} from '@hcengineering/server-core'

/**
 * @public
 */
export class TransientMiddleware extends BaseMiddleware implements Middleware {
  private readonly ttlValues = new Map<Ref<Class<Doc>>, number>()
  private readonly ttlObjectMap = new Map<Ref<Doc>, number>()
  ttlChecker: any
  now = Date.now() / 1000
  dbProvider?: DbAdapter
  private ttlCriticalSection: Promise<void> = Promise.resolve()

  private constructor (
    readonly ctx: MeasureContext,
    context: PipelineContext,
    next?: Middleware
  ) {
    super(context, next)

    // Need to find all classes with TTL enabled
    const classes = context.modelDb.findAllSync(core.mixin.TransientTTL, {})
    for (const cl of classes) {
      this.ttlValues.set(cl._id as Ref<Class<Doc>>, cl.ttl)
    }

    this.dbProvider = context.adapterManager?.getAdapter?.(DOMAIN_TRANSIENT, true)
    if (this.dbProvider !== undefined) {
      this.ttlChecker = setInterval(() => {
        void this.checkTTL()
      }, 1000)
    }
  }

  static async create (
    ctx: MeasureContext,
    context: PipelineContext,
    next: Middleware | undefined
  ): Promise<TransientMiddleware> {
    return new TransientMiddleware(ctx, context, next)
  }

  private async withTtlCriticalSection<T>(operation: () => Promise<T>): Promise<T> {
    const previous = this.ttlCriticalSection
    let release: (() => void) | undefined
    const current = new Promise<void>((resolve) => {
      release = resolve
    })
    const tail = previous.then(() => current)
    this.ttlCriticalSection = tail
    await previous
    try {
      return await operation()
    } finally {
      release?.()
      if (this.ttlCriticalSection === tail) {
        this.ttlCriticalSection = Promise.resolve()
      }
    }
  }

  checkTTL = reduceCalls(async () => {
    const dbProvider = this.dbProvider
    if (dbProvider === undefined) {
      return
    }
    this.now = Date.now() / 1000 // Now in seconds

    const docsToRemove = new Map<Ref<Doc>, number>()
    for (const v of this.ttlObjectMap.entries()) {
      if (v[1] < this.now) {
        docsToRemove.set(v[0], v[1])
      }
    }

    if (docsToRemove.size > 0) {
      const candidateIds = [...docsToRemove.keys()]
      const f = new TxFactory(core.account.System)
      const docs = await dbProvider.load(this.ctx, DOMAIN_TRANSIENT, candidateIds)
      await this.withTtlCriticalSection(async () => {
        const now = Date.now() / 1000
        const confirmedIds = candidateIds.filter((id) => {
          const selectedExpiry = docsToRemove.get(id)
          return selectedExpiry !== undefined && selectedExpiry < now && this.ttlObjectMap.get(id) === selectedExpiry
        })
        if (confirmedIds.length === 0) {
          return
        }
        const confirmedIdSet = new Set(confirmedIds)
        for (const id of confirmedIds) {
          this.ttlObjectMap.delete(id)
        }
        await dbProvider.clean(this.ctx, DOMAIN_TRANSIENT, confirmedIds)
        await this.context.broadcastEvent?.(
          this.ctx,
          docs
            .filter((doc) => confirmedIdSet.has(doc._id))
            .map((doc) => f.createTxRemoveDoc(doc._class, doc.space, doc._id))
        )
      })
    }
  })

  override async close (): Promise<void> {
    if (this.ttlChecker !== undefined) {
      clearInterval(this.ttlChecker)
      this.ttlChecker = undefined
    }
    await super.close()
  }

  async tx (ctx: MeasureContext<SessionData>, txes: Tx[]): Promise<TxMiddlewareResult> {
    const ttlTxes = txes
      .filter((it) => TxProcessor.isExtendsCUD(it._class))
      .filter((tx) => {
        const cud = tx as TxCUD<Doc>
        return (
          this.ttlValues.has(cud.objectClass) && this.context.hierarchy.findDomain(cud.objectClass) === DOMAIN_TRANSIENT
        )
      }) as TxCUD<Doc>[]
    if (ttlTxes.length === 0) {
      return await this.provideTx(ctx, txes)
    }

    return await this.withTtlCriticalSection(async () => {
      for (const tx of ttlTxes) {
        const ttl = this.ttlValues.get(tx.objectClass)
        if (ttl !== undefined) {
          if (tx._class === core.class.TxRemoveDoc) {
            // ok we have operation against our TTL object.
            this.ttlObjectMap.delete(tx.objectId)
          } else {
            // ok we have operation against our TTL object.
            this.ttlObjectMap.set(tx.objectId, this.now + ttl + 1)
          }
        }
      }

      return await this.provideTx(ctx, txes)
    })
  }
}
