package com.snowplowanalytics.snowplow.postgres.loader.streaming

import java.util.Base64
import java.nio.charset.StandardCharsets

import cats.implicits._

import cats.effect.concurrent.Ref
import cats.effect.{ContextShift, Bracket, Clock, ConcurrentEffect, Sync}

import fs2.Stream
import fs2.aws.kinesis.{CommittableRecord, KinesisConsumerSettings}
import fs2.aws.kinesis.consumer.readFromKinesisStream

import io.circe.Json

import com.snowplowanalytics.iglu.client.Client

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.ParsingError.NotTSV
import com.snowplowanalytics.snowplow.badrows.{Processor, BadRow, Payload, Failure}
import doobie.implicits._
import doobie.util.transactor.Transactor

import com.snowplowanalytics.snowplow.postgres.loader.shredding.transform

object source {

  val processor = Processor("snowplow-postgres-loader", "0.1.0")

  def singleEvent[F[_]]: Stream[F, String] =
    Stream
      .emit("snowplowweb\tweb\t2018-12-18 15:07:17.970\t2016-03-29 07:28:18.611\t2016-03-29 07:28:18.634\tpage_view\t11cdec7b-4cbd-4aa4-a4c9-3874ab9663d4\t\tsnplow6\tjs-2.6.0\tssc-0.6.0-kinesis\tspark-1.16.0-common-0.35.0\t34df2c48bc170c87befb441732a94196\t372d1f2983860eefd262b58e6592dfbc\t80546dc70f4a91f1283c4b6247e31bcf\t26e6412a2421eb923d9d40258ca9ca69\t1\t3a12e8b8e3e91a4d092b833d583c7e30\tDK\t82\tOdder\t8300\t55.9731\t10.153\tCentral Jutland\tTDC Danmark\tTDC Danmark\t\t\thttp://snowplowanalytics.com/documentation/recipes/catalog-analytics/market-basket-analysis-identifying-products-that-sell-well-together.html\tMarket basket analysis - identifying products and content that go well together – Snowplow\thttp://snowplowanalytics.com/analytics/catalog-analytics/market-basket-analysis-identifying-products-that-sell-well-together.html\thttp\tsnowplowanalytics.com\t80\t/documentation/recipes/catalog-analytics/market-basket-analysis-identifying-products-that-sell-well-together.html\t\t\thttp\tsnowplowanalytics.com\t80\t/analytics/catalog-analytics/market-basket-analysis-identifying-products-that-sell-well-together.html\t\t\tinternal\t\t\t\t\t\t\t\t{\"schema\":\"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0\",\"data\":[{\"schema\":\"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0\",\"data\":{\"id\":\"05862d26-0dde-4d7a-a494-fc9aae283d23\"}},{\"schema\":\"iglu:org.schema/WebPage/jsonschema/1-0-0\",\"data\":{\"genre\":\"documentation\",\"inLanguage\":\"en-US\"}},{\"schema\":\"iglu:org.w3/PerformanceTiming/jsonschema/1-0-0\",\"data\":{\"navigationStart\":1459236496534,\"unloadEventStart\":1459236496838,\"unloadEventEnd\":1459236496838,\"redirectStart\":0,\"redirectEnd\":0,\"fetchStart\":1459236496534,\"domainLookupStart\":1459236496534,\"domainLookupEnd\":1459236496534,\"connectStart\":1459236496534,\"connectEnd\":1459236496534,\"secureConnectionStart\":0,\"requestStart\":1459236496580,\"responseStart\":1459236496834,\"responseEnd\":1459236496844,\"domLoading\":1459236496853,\"domInteractive\":1459236497780,\"domContentLoadedEventStart\":1459236497780,\"domContentLoadedEventEnd\":1459236498038,\"domComplete\":0,\"loadEventStart\":0,\"loadEventEnd\":0,\"chromeFirstPaint\":1459236498203}}]}\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tMozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36\tChrome 49\tChrome\t49.0.2623.87\tBrowser\tWEBKIT\ten-US\t1\t1\t0\t0\t0\t0\t0\t0\t0\t1\t24\t1920\t1075\tWindows 7\tWindows\tMicrosoft Corporation\tEurope/Berlin\tComputer\t0\t1920\t1200\tUTF-8\t1903\t11214\t\t\t\t\t\t\t\tEurope/Copenhagen\t\t\t\t2016-03-29 07:28:18.636\t\t\t{\"schema\":\"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1\",\"data\":[{\"schema\":\"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0\",\"data\":{\"useragentFamily\":\"Chrome\",\"useragentMajor\":\"49\",\"useragentMinor\":\"0\",\"useragentPatch\":\"2623\",\"useragentVersion\":\"Chrome 49.0.2623\",\"osFamily\":\"Windows\",\"osMajor\":\"7\",\"osMinor\":null,\"osPatch\":null,\"osPatchMinor\":null,\"osVersion\":\"Windows 7\",\"deviceFamily\":\"Other\"}}]}\t88c23330-ac4d-4c82-8a18-aa83c1e0c163\t2016-03-29 07:28:18.609\tcom.snowplowanalytics.snowplow\tpage_view\tjsonschema\t1-0-0\tcab5ba164038f31d8e10befc4eb199df\t")
      .covary[F]

  def parseString(s: String) =
    Event.parse(s).toEither.leftMap { error =>
      BadRow.LoaderParsingError(processor, error, Payload.RawPayload(s))
    }

  def getEvents[F[_]: ConcurrentEffect: ContextShift](settings: KinesisConsumerSettings): Stream[F, Either[BadRow, Event]] =
    readFromKinesisStream[F](settings).map(processRecord)
//    singleEvent[F].map(parseString)

  def badSink[F[_]: Sync](bads: Stream[F, BadRow]): Stream[F, Unit] =
    bads.evalMap(row => Sync[F].delay(println(row.compact)))

  def eventsSink[F[_]: Sync: Clock](xa: Transactor[F], state: Ref[F, PgState], client: Client[F, Json])
                                   (events: Stream[F, Event])
                                   (implicit B: Bracket[F, Throwable]): Stream[F, Unit] =
    events.evalMap { event =>
      val result = for {
        entities <- transform.shredEvent[F](client, event)
        insert <- sink.insertData(client.resolver, state, entities).leftMap { errors =>
          BadRow.LoaderIgluError(processor, Failure.LoaderIgluErrors(errors), Payload.LoaderPayload(event)): BadRow
        }
      } yield insert

      result.value.flatMap {
        case Right(insert) => insert.transact(xa)
        case Left(badRow) => ???
      }
    }

  /** Pasrse Kinesis record into a valid Snowplow `Event` or parsing error `BadRow` */
  def processRecord(record: CommittableRecord): Either[BadRow, Event] = {
    val string = try {
      StandardCharsets.UTF_8.decode(record.record.data()).toString.asRight[BadRow]
    } catch {
      case _: IllegalArgumentException =>
        val payload = StandardCharsets.UTF_8.decode(Base64.getEncoder.encode(record.record.data())).toString
        BadRow.LoaderParsingError(processor, NotTSV, Payload.RawPayload(payload)).asLeft
    }

    string.flatMap { s =>
      Event.parse(s).toEither.leftMap { error =>
        BadRow.LoaderParsingError(processor, error, Payload.RawPayload(s))
      }
    }
  }
}
