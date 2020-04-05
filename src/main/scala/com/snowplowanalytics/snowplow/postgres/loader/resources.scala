package com.snowplowanalytics.snowplow.postgres.loader

import cats.implicits._

import cats.effect.concurrent.Ref
import cats.effect.{ContextShift, Async, Blocker, Clock, Resource, Sync}

import doobie.hikari._
import doobie.util.ExecutionContexts
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor

import io.circe.Json

import com.snowplowanalytics.iglu.client.Client

import com.snowplowanalytics.snowplow.postgres.loader.config.LoaderConfig
import com.snowplowanalytics.snowplow.postgres.loader.config.LoaderConfig.JdbcUri
import com.snowplowanalytics.snowplow.postgres.loader.storage.PgState

object resources {

  /** Initialise Blocking Thread Pool, Connection Pool and DB state */
  def initialize[F[_]: Async: Clock: ContextShift](postgres: LoaderConfig,
                                                   logger: LogHandler,
                                                   iglu: Client[F, Json]) =
    for {
      blocker <- Blocker[F]
      xa <- resources.getTransactor[F](postgres.getJdbc, postgres.username, postgres.password, blocker)

      initState = storage.PgState.init[F](xa, logger, iglu.resolver, postgres.schema).value.flatMap {
        case Left(error) =>
          val exception = new RuntimeException(s"Couldn't initalise the state $error")
          Sync[F].raiseError[Ref[F, PgState]](exception)
        case Right((issues, state)) =>
          issues.traverse(issue => Sync[F].delay(println(issue))).as(state)
      }
      state <- Resource.liftF(initState)
    } yield (blocker, xa, state)

  /** Get a HikariCP transactor */
  def getTransactor[F[_]: Async: ContextShift](jdbcUri: JdbcUri, user: String, password: String, be: Blocker): Resource[F, HikariTransactor[F]] =
    for {
      ce <- ExecutionContexts.fixedThreadPool[F](32)
      xa <- HikariTransactor.newHikariTransactor[F]("org.postgresql.Driver", jdbcUri.toString, user, password, ce, be)
    } yield xa

  /** Get default single-threaded transactor (use for tests only) */
  def getTransactorDefault[F[_]: Async: ContextShift](jdbcUri: JdbcUri, username: String, password: String): Transactor[F] =
    Transactor.fromDriverManager[F](
      "org.postgresql.Driver", jdbcUri.toString, username, password
    )
}
