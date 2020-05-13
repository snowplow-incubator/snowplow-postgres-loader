package com.snowplowanalytics.snowplow.postgres.loader.streaming

import cats.effect.{IO, Clock}

import com.snowplowanalytics.iglu.client.Client

import org.specs2.mutable.Specification

class SinkSpec extends Specification {
  implicit val ioClock = Clock.create[IO]
  "insertData" should {
    "return Right with no entities (nothing to insert)" >> {
      val action = for {
        state <- PgState.init[IO]
        result <- sink.insertData[IO](SinkSpec.igluClient.resolver, state, List.empty).value
      } yield result

      action.unsafeRunSync() must beRight
    }
  }
}

object SinkSpec {
  val igluClient = Client.IgluCentral
}
