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

import core, { clone, createClient, toFindResult, TxFactory, TxOperations, type FindResult } from '@hcengineering/core'
import { LiveQuery } from '..'
import { connect } from './connection'
import { test, type AttachedComment } from './minmodel'
import { ResultArray } from '../results'
import type { Query } from '../types'

interface AuditableLiveQuery {
  queries: Map<unknown, Map<number, Query>>
  doRefresh: (query: Query) => Promise<void>
  withQueryMutation: <T>(query: Query, operation: () => Promise<T>) => Promise<T>
}

const getOnlyQuery = (liveQuery: LiveQuery): { audit: AuditableLiveQuery, query: Query } => {
  const audit = liveQuery as unknown as AuditableLiveQuery
  const query = Array.from(audit.queries.values())[0]?.values().next().value
  if (query === undefined) {
    throw new Error('Expected one registered LiveQuery')
  }
  return { audit, query }
}

describe('LiveQuery refresh ordering', () => {
  it('does not publish a stale refresh after a newer transaction', async () => {
    const storage = await createClient(connect)
    const liveQuery = new LiveQuery(storage)
    const operations = new TxOperations(storage, core.account.System)
    const space = await operations.createDoc(core.class.Space, core.space.Model, {
      name: 'refresh-race',
      description: '',
      private: false,
      members: [],
      archived: false
    })
    const comment = await operations.addCollection(
      test.class.TestComment,
      space,
      space,
      core.class.Space,
      'comments',
      { message: 'old' }
    )
    const initialSnapshot = await storage.findAll(test.class.TestComment, { _id: comment })
    const staleSnapshot = toFindResult(initialSnapshot.map((document) => clone(document)), initialSnapshot.total)
    const observed: string[] = []
    let initialPublished: (() => void) | undefined
    const initialPublishedPromise = new Promise<void>((resolve) => {
      initialPublished = resolve
    })
    const unsubscribe = liveQuery.query(test.class.TestComment, { _id: comment }, (result) => {
      observed.push(result[0]?.message ?? '<missing>')
      initialPublished?.()
    })
    await initialPublishedPromise
    let refreshStarted: (() => void) | undefined
    const refreshStartedPromise = new Promise<void>((resolve) => {
      refreshStarted = resolve
    })
    let releaseRefresh: ((result: FindResult<AttachedComment>) => void) | undefined
    const heldRefresh = new Promise<FindResult<AttachedComment>>((resolve) => {
      releaseRefresh = resolve
    })
    const findAll = jest.spyOn(storage, 'findAll').mockImplementationOnce(async () => {
      refreshStarted?.()
      return await heldRefresh
    })
    const refreshing = liveQuery.refreshConnect(false)
    await refreshStartedPromise
    const tx = new TxFactory(core.account.System).createTxUpdateDoc(
      test.class.TestComment,
      space,
      comment,
      { message: 'new' }
    )
    tx.modifiedOn = (initialSnapshot[0]?.modifiedOn ?? 0) + 1000

    await liveQuery.tx(tx)
    releaseRefresh?.(staleSnapshot)
    await refreshing
    await new Promise<void>((resolve) => setTimeout(resolve, 0))

    expect(observed).toEqual(['old', 'new'])
    findAll.mockRestore()
    unsubscribe()
    await liveQuery.close()
  })

  it('detects an ABA result mutation while refresh is pending', async () => {
    const storage = await createClient(connect)
    const liveQuery = new LiveQuery(storage)
    const operations = new TxOperations(storage, core.account.System)
    const space = await operations.createDoc(core.class.Space, core.space.Model, {
      name: 'refresh-aba',
      description: '',
      private: false,
      members: [],
      archived: false
    })
    const comment = await operations.addCollection(
      test.class.TestComment,
      space,
      space,
      core.class.Space,
      'comments',
      { message: 'old' }
    )
    const initialSnapshot = await storage.findAll(test.class.TestComment, { _id: comment })
    const staleSnapshot = toFindResult(initialSnapshot.map((document) => clone(document)), initialSnapshot.total)
    if (staleSnapshot[0] !== undefined) {
      staleSnapshot[0].message = 'stale-refresh'
    }
    const observed: string[] = []
    let initialPublished: (() => void) | undefined
    const initialPublishedPromise = new Promise<void>((resolve) => {
      initialPublished = resolve
    })
    const unsubscribe = liveQuery.query(test.class.TestComment, { _id: comment }, (result) => {
      observed.push(result[0]?.message ?? '<missing>')
      initialPublished?.()
    })
    await initialPublishedPromise
    const { audit, query } = getOnlyQuery(liveQuery)
    let refreshStarted: (() => void) | undefined
    const refreshStartedPromise = new Promise<void>((resolve) => {
      refreshStarted = resolve
    })
    let releaseRefresh: ((result: FindResult<AttachedComment>) => void) | undefined
    const heldRefresh = new Promise<FindResult<AttachedComment>>((resolve) => {
      releaseRefresh = resolve
    })
    const findAll = jest.spyOn(storage, 'findAll').mockImplementationOnce(async () => {
      refreshStarted?.()
      return await heldRefresh
    })
    const refreshing = audit.doRefresh(query)
    await refreshStartedPromise
    if (query.result instanceof Promise) {
      throw new Error('Expected the initial query to be settled')
    }
    const original = query.result.getClone<AttachedComment>()[0]
    if (original === undefined) {
      throw new Error('Expected the initial comment')
    }
    const temporary = clone(original)
    temporary.message = 'temporary'
    query.result.updateDoc(temporary)
    query.result.updateDoc(original)
    releaseRefresh?.(staleSnapshot)
    await refreshing

    expect(observed).toEqual(['old'])
    findAll.mockRestore()
    unsubscribe()
    await liveQuery.close()
  })

  it('does not overwrite a replacement while the initial result is pending', async () => {
    const storage = await createClient(connect)
    const liveQuery = new LiveQuery(storage)
    const operations = new TxOperations(storage, core.account.System)
    const space = await operations.createDoc(core.class.Space, core.space.Model, {
      name: 'refresh-pending',
      description: '',
      private: false,
      members: [],
      archived: false
    })
    const comment = await operations.addCollection(
      test.class.TestComment,
      space,
      space,
      core.class.Space,
      'comments',
      { message: 'initial' }
    )
    const initialSnapshot = await storage.findAll(test.class.TestComment, { _id: comment })
    let releaseInitial: ((result: FindResult<AttachedComment>) => void) | undefined
    const heldInitial = new Promise<FindResult<AttachedComment>>((resolve) => {
      releaseInitial = resolve
    })
    const findAll = jest.spyOn(storage, 'findAll').mockImplementationOnce(async () => await heldInitial)
    const unsubscribe = liveQuery.query(test.class.TestComment, { _id: comment }, () => {})
    const { audit, query } = getOnlyQuery(liveQuery)
    const refreshing = audit.doRefresh(query)
    const replacement = new ResultArray([], liveQuery.getHierarchy())
    query.result = replacement
    releaseInitial?.(initialSnapshot)
    await refreshing

    expect(query.result).toBe(replacement)
    findAll.mockRestore()
    unsubscribe()
    await liveQuery.close()
  })

  it('allows concurrent refreshes waiting for the same initial result', async () => {
    const storage = await createClient(connect)
    const liveQuery = new LiveQuery(storage)
    const operations = new TxOperations(storage, core.account.System)
    const space = await operations.createDoc(core.class.Space, core.space.Model, {
      name: 'refresh-shared-pending',
      description: '',
      private: false,
      members: [],
      archived: false
    })
    const comment = await operations.addCollection(
      test.class.TestComment,
      space,
      space,
      core.class.Space,
      'comments',
      { message: 'initial' }
    )
    const initialSnapshot = await storage.findAll(test.class.TestComment, { _id: comment })
    let releaseInitial: ((result: FindResult<AttachedComment>) => void) | undefined
    const heldInitial = new Promise<FindResult<AttachedComment>>((resolve) => {
      releaseInitial = resolve
    })
    const findAll = jest.spyOn(storage, 'findAll').mockImplementationOnce(async () => await heldInitial)
    const unsubscribe = liveQuery.query(test.class.TestComment, { _id: comment }, () => {})
    const { audit, query } = getOnlyQuery(liveQuery)
    const firstRefresh = audit.doRefresh(query)
    const secondRefresh = audit.doRefresh(query)
    releaseInitial?.(initialSnapshot)
    await Promise.all([firstRefresh, secondRefresh])

    expect(findAll).toHaveBeenCalledTimes(3)
    findAll.mockRestore()
    unsubscribe()
    await liveQuery.close()
  })

  it('defers refresh without changing mutation outcomes', async () => {
    const storage = await createClient(connect)
    const liveQuery = new LiveQuery(storage)
    const operations = new TxOperations(storage, core.account.System)
    const space = await operations.createDoc(core.class.Space, core.space.Model, {
      name: 'refresh-after-mutation',
      description: '',
      private: false,
      members: [],
      archived: false
    })
    const comment = await operations.addCollection(
      test.class.TestComment,
      space,
      space,
      core.class.Space,
      'comments',
      { message: 'initial' }
    )
    let initialPublished: (() => void) | undefined
    const initialPublishedPromise = new Promise<void>((resolve) => {
      initialPublished = resolve
    })
    const unsubscribe = liveQuery.query(test.class.TestComment, { _id: comment }, () => {
      initialPublished?.()
    })
    await initialPublishedPromise
    const { audit, query } = getOnlyQuery(liveQuery)
    let releaseMutation: (() => void) | undefined
    const mutationBarrier = new Promise<void>((resolve) => {
      releaseMutation = resolve
    })
    const findAll = jest.spyOn(storage, 'findAll')
    const mutation = audit.withQueryMutation(query, async () => await mutationBarrier)
    await Promise.resolve()

    await audit.doRefresh(query)
    expect(findAll).not.toHaveBeenCalled()

    releaseMutation?.()
    await mutation
    await new Promise<void>((resolve) => setTimeout(resolve, 0))
    expect(findAll).toHaveBeenCalledTimes(1)

    const refreshFailure = new Error('deferred refresh failed')
    findAll.mockRejectedValueOnce(refreshFailure)
    let releaseSuccessfulMutation: (() => void) | undefined
    const successfulMutationBarrier = new Promise<void>((resolve) => {
      releaseSuccessfulMutation = resolve
    })
    const successfulMutation = audit.withQueryMutation(query, async () => {
      await successfulMutationBarrier
      return 'mutation-result'
    })
    await Promise.resolve()
    await audit.doRefresh(query)
    releaseSuccessfulMutation?.()
    await expect(successfulMutation).resolves.toBe('mutation-result')
    await new Promise<void>((resolve) => setTimeout(resolve, 0))

    findAll.mockRejectedValueOnce(refreshFailure)
    const mutationFailure = new Error('mutation failed')
    let rejectMutation: ((error: Error) => void) | undefined
    const failedMutationBarrier = new Promise<void>((_resolve, reject) => {
      rejectMutation = reject
    })
    const failedMutation = audit.withQueryMutation(query, async () => await failedMutationBarrier)
    const failedMutationAssertion = expect(failedMutation).rejects.toBe(mutationFailure)
    await Promise.resolve()
    await audit.doRefresh(query)
    rejectMutation?.(mutationFailure)
    await failedMutationAssertion
    await new Promise<void>((resolve) => setTimeout(resolve, 0))

    expect(findAll).toHaveBeenCalledTimes(3)
    findAll.mockRestore()
    unsubscribe()
    await liveQuery.close()
  })
})
