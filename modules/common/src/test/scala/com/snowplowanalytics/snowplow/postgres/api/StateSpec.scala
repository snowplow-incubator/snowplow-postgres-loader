/*
 * Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.
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

import java.util.concurrent.TimeUnit

import concurrent.duration._

import cats.implicits._

import cats.effect.concurrent.Ref
import cats.effect.{Clock, IO, Timer}

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import com.snowplowanalytics.snowplow.postgres.Database.{CS, igluClient}
import com.snowplowanalytics.snowplow.postgres.api.DB.StateCheck
import com.snowplowanalytics.snowplow.postgres.api.StateSpec._

import org.scalacheck.{Gen, Prop}
import org.specs2.scalacheck.Parameters

class StateSpec extends Specification with ScalaCheck {
  "checkAndRun" should {
    "execute `mutate` when StateCheck is Block" >> {
      val key = SchemaKey("com.acme", "missing_table", "jsonschema", SchemaVer.Full(1, 0, 0))
      val alwaysEmpty: SchemaState => StateCheck =
        _ => StateCheck.Block(Set(key), Set.empty)

      val result = for {
        state <- initState
        db <- Ref.of[IO, List[Int]](List.empty)
        keys <- Ref.of[IO, Set[SchemaKey]](Set.empty)
        _ <- state.checkAndRun(alwaysEmpty, IO.sleep(100.millis) *> db.update(s => 1 :: s), (m, _) => keys.update(_ ++ m))
        res <- (db.get, keys.get).tupled
      } yield res

      result.unsafeRunSync() must beEqualTo((List(1), Set(key)))
    }

    "not execute `mutate` when StateCheck is Ok" >> {
      val alwaysOk: SchemaState => StateCheck =
        _ => StateCheck.Ok

      val result = for {
        state <- initState
        db <- Ref.of[IO, List[Int]](List.empty)
        keys <- Ref.of[IO, Set[SchemaKey]](Set.empty)
        _ <- state.checkAndRun(alwaysOk, IO.sleep(100.millis) *> db.update(s => 1 :: s), (m, _) => keys.update(_ ++ m))
        res <- (db.get, keys.get).tupled
      } yield res

      result.unsafeRunSync() must beEqualTo((List(1), Set()))
    }

    "execute locked calls one after another" >> {
      val key = SchemaKey("com.acme", "missing_table", "jsonschema", SchemaVer.Full(1, 0, 0))
      val alwaysEmpty: SchemaState => StateCheck =
        _ => StateCheck.Block(Set(key), Set.empty)

      Prop
        .forAll(durationsGen) { durations =>
          val checks = for {
            state <- initState
            db <- Ref.of[IO, Int](0)
            keys <- Ref.of[IO, Set[SchemaKey]](Set.empty)
            _ <- durations.parTraverse_(d => state.checkAndRun(alwaysEmpty, db.update(_ + 1), (m, _) => IO.sleep(d) *> keys.update(_ ++ m)))
            res <- (db.get, keys.get).tupled
          } yield res
          val result = measure(checks)

          result.unsafeRunSync() must beLike {
            case ((counter, keys), time) =>
              val totalDelays = durations.foldMap(_.toMillis)
              val allBlocking = time must beBetween(totalDelays, totalDelays * 2)
              allBlocking.and(counter must beEqualTo(durations.length)).and(keys must beEqualTo(Set(key)))
          }
        }
        .setParameters(Parameters(minTestsOk = 5, maxSize = 10))
    }

    "execute non-locked calls in parallel" >> { // Potentially flaky test
      val alwaysEmpty: SchemaState => StateCheck =
        _ => StateCheck.Ok

      Prop
        .forAll(durationsGen) { durations =>
          val checks = for {
            state <- initState
            db <- Ref.of[IO, Int](0)
            keys <- Ref.of[IO, Set[SchemaKey]](Set.empty)
            _ <- durations.parTraverse_(d => state.checkAndRun(alwaysEmpty, IO.sleep(d) *> db.update(_ + 1), (m, _) => keys.update(_ ++ m)))
            res <- (db.get, keys.get).tupled
          } yield res
          val result = measure(checks)

          result.unsafeRunSync() must beLike {
            case ((counter, keys), time) =>
              val maxDelay = durations.fold(5.millis)((a, b) => a.max(b)).toMillis
              val nonBlocking = time must lessThan(maxDelay * 2)
              nonBlocking.and(counter must beEqualTo(durations.length)).and(keys must beEqualTo(Set()))
          }
        }
        .setParameters(Parameters(minTestsOk = 5, maxSize = 10))
    }
  }
}

object StateSpec {
  implicit val C: Clock[IO] = Clock.create[IO]
  implicit val T: Timer[IO] = IO.timer(concurrent.ExecutionContext.global)

  val initState = State
    .init[IO](List.empty, igluClient.resolver)
    .value
    .flatMap(_.fold(_ => IO.raiseError[State[IO]](new IllegalStateException("Cannot start a test")), IO.pure))

  val durationsGen = for {
    size <- Gen.chooseNum(2, 20)
    delay = Gen.chooseNum(5, 300)
    durations <- Gen.listOfN(size, delay)
  } yield durations.map(_.millis)

  def measure[A](action: IO[A]): IO[(A, Long)] =
    for {
      start <- C.realTime(TimeUnit.MILLISECONDS)
      a <- action
      end <- C.realTime(TimeUnit.MILLISECONDS)
    } yield (a, end - start)
}
