package com.snowplowanalytics.snowplow.postgres

import java.net.URI
import java.util.UUID

import cats.data.EitherT
import cats.implicits._

import cats.effect.{Clock, ContextShift, IO}

import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAfterEach
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._

import io.circe.Json

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.Registry
import com.snowplowanalytics.iglu.client.resolver.registries.Registry.{Config, Http, HttpConnection}
import com.snowplowanalytics.iglu.client.validator.CirceValidator

import com.snowplowanalytics.snowplow.badrows.FailureDetails
import com.snowplowanalytics.snowplow.postgres.config.DBConfig.JdbcUri
import com.snowplowanalytics.snowplow.postgres.storage.definitions.EventsTableName

trait Database extends Specification with BeforeAfterEach {
  import Database._

  implicit val ioClock: Clock[IO] = Clock.create[IO]

  def before =
    (dropAll *> storage.utils.prepare[IO](Schema, xa)).unsafeRunSync()

  def after =
    dropAll.unsafeRunSync()

  sequential

}

object Database {

  val Schema = "public"

  implicit val CS: ContextShift[IO] = IO.contextShift(concurrent.ExecutionContext.global)

  val jdbcUri = JdbcUri("localhost", 5432, "snowplow", "allow")
  val registry = Http(Config("localhost registry", 1, Nil), HttpConnection(URI.create("http://localhost:8080/api/"), None))
  val igluClient = Client[IO, Json](Resolver(List(Registry.IgluCentral, registry), None), CirceValidator)
  val xa: Transactor[IO] = resources.getTransactorDefault[IO](jdbcUri, "postgres", "mysecretpassword")

  case class ColumnInfo(columnName: String,
                        columnDefault: Option[String],
                        isNullable: Boolean,
                        dataType: String,
                        characterMaximumLength: Option[Int]
  )

  def query: IO[List[UUID]] = {
    val tablefr = Fragment.const0(EventsTableName)
    fr"SELECT event_id FROM $tablefr".query[UUID].to[List].transact(xa)
  }

  def count(table: String): IO[Int] =
    (fr"SELECT count(*) FROM " ++ Fragment.const(table)).query[Int].unique.transact(xa)

  def describeTable(tableName: String) =
    sql"""SELECT column_name::VARCHAR,
                   column_default::VARCHAR,
                   is_nullable::BOOLEAN,
                   data_type::VARCHAR,
                   character_maximum_length::INTEGER
            FROM information_schema.columns
            WHERE table_name = $tableName"""
      .query[(String, Option[String], Boolean, String, Option[Int])]
      .map(ColumnInfo.tupled)
      .to[List]
      .transact(xa)

  def dropAll: IO[Unit] = {
    val schemaFr = Fragment.const(Schema)
    List(
      fr"DROP SCHEMA $schemaFr CASCADE;",
      fr"CREATE SCHEMA $schemaFr;",
      fr"GRANT ALL ON SCHEMA public TO postgres;",
      fr"GRANT ALL ON SCHEMA public TO $schemaFr;"
    ).map(_.update.run).traverse_(_.transact(xa).void)
  }

  implicit class ActionOps[A](io: IO[A]) {
    def action = EitherT.liftF[IO, FailureDetails.LoaderIgluError, A](io)
  }
}
