/*
 * Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.postgres.api

import cats.Monad
import cats.data.EitherT
import cats.implicits._

import cats.effect.concurrent.{MVar, Ref}
import cats.effect.{Bracket, Clock, Concurrent}
import cats.effect.implicits._

import com.snowplowanalytics.iglu.core.SchemaKey

import com.snowplowanalytics.iglu.client.resolver.Resolver

import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaList

import com.snowplowanalytics.snowplow.badrows.FailureDetails.LoaderIgluError
import com.snowplowanalytics.snowplow.postgres.api.DB.StateCheck

/**
  * Mutable variable, protected by by lock.
  * [[checkAndRun]] is the only function that should be able to mutate this structure
  */
final class State[F[_]](lock: MVar[F, Unit], state: Ref[F, SchemaState]) {

  /**
    * Primary state-handling and the only state-mutation function.
    *
   * Most of the time `stateCheck` returns `StateCheck.Ok`, meaning that data can be
    * inserted without state or DB schema mutation and lock is not acquired, while
    * `action` gets executed.
    *
   * If new schemas are coming through and state and DB schema have to be changed
    * it acquires a lock, preventing other threads from mutating data first, then checks
    * if state is still outdated (in case other thread acquired the lock first) and
    * performs `mutate` and `action`, releasing the lock afterwards
    * If another thread already updated the state it just performs `action`
    *
   * @param stateCheck check if lock has to be acquired
    * @param action primary IO - DB insert statement
    * @param mutate IO that mutates the internal state and DB schema
    */
  def checkAndRun(stateCheck: SchemaState => StateCheck, action: F[Unit], mutate: (Set[SchemaKey], Set[SchemaKey]) => F[Unit])(implicit
    F: Bracket[F, Throwable]
  ): F[Unit] = {
    // Just insert OR mutate and insert
    def check(update: (Set[SchemaKey], Set[SchemaKey]) => F[Unit]) =
      state.get.map(stateCheck).flatMap {
        case StateCheck.Ok =>
          Monad[F].unit
        case StateCheck.Block(missingTables, outdatedTables) =>
          update(missingTables, outdatedTables)
      }

    check((_, _) => withLock(check(mutate))) *> action
  }

  /** Update [[SchemaState]] with new `SchemaList` */
  private[api] def put(schemaList: SchemaList): F[Unit] =
    state.update(_.put(schemaList))

  private def withLock[A](fa: F[A])(implicit F: Bracket[F, Throwable]): F[A] =
    lock.take.bracket(_ => fa)(_ => lock.put(()))

}

object State {
  def init[F[_]: Concurrent: Clock](keys: List[SchemaKey], resolver: Resolver[F]): EitherT[F, LoaderIgluError, State[F]] =
    for {
      lock <- EitherT.liftF[F, LoaderIgluError, MVar[F, Unit]](MVar[F].of(()))
      state <- SchemaState.init[F](keys, resolver)
    } yield new State[F](lock, state)
}
