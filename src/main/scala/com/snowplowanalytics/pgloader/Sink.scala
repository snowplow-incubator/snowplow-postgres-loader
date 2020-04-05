package com.snowplowanalytics.pgloader

import java.util.UUID

import cats.syntax.functor._

import doobie._
import doobie.implicits._

import cats.effect.{Async, ContextShift}
import com.snowplowanalytics.pgloader.Options.JdbcUri

object Sink {

  implicit val uuidPut: Put[UUID] =
    Put[String].contramap((x: UUID) => x.toString)

  def getTransaction[F[_]: Async: ContextShift](jdbcUri: JdbcUri, password: String): Transactor[F] =
    Transactor.fromDriverManager[F](
      "org.postgresql.Driver", jdbcUri.toString, jdbcUri.username, password
    )

  def insert(id: UUID): ConnectionIO[Unit] =
    sql"INSERT INTO events (id) VALUES ($id);".update.run.void

}
