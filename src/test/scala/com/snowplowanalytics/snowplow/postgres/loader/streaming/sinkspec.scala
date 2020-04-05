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
package com.snowplowanalytics.snowplow.postgres.loader.streaming

import java.util.UUID

import cats.effect.concurrent.Ref
import cats.effect.IO

import fs2.Stream
import fs2.concurrent.Queue

import io.circe.Json
import io.circe.literal._

import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.postgres.loader.Database
import com.snowplowanalytics.snowplow.postgres.loader.storage.PgState
import com.snowplowanalytics.snowplow.postgres.loader.streaming.source.{Data, BadData}

class sinkspec extends Database {
  import Database._

  "insertData" should {
    "return Right with no entities (nothing to insert)" >> {
      val action = for {
        state <- Ref.of[IO, PgState](PgState(Map()))
        result <- sink.insertData[IO](igluClient.resolver, logger, Schema, state, List.empty, true).value
      } yield result

      action.unsafeRunSync() must beRight
    }
  }

  "goodSink" should {
    "sink a single good event" >> {
      val line = "snowplowweb\tweb\t2018-12-18 15:07:17.970\t2016-03-29 07:28:18.611\t2016-03-29 07:28:18.634\tpage_view\t11cdec7b-4cbd-4aa4-a4c9-3874ab9663d4\t\tsnplow6\tjs-2.6.0\tssc-0.6.0-kinesis\tspark-1.16.0-common-0.35.0\t34df2c48bc170c87befb441732a94196\t372d1f2983860eefd262b58e6592dfbc\t80546dc70f4a91f1283c4b6247e31bcf\t26e6412a2421eb923d9d40258ca9ca69\t1\t3a12e8b8e3e91a4d092b833d583c7e30\tDK\t82\tOdder\t8300\t42.0001\t42.003\tCentral Jutland\tTDC Danmark\tTDC Danmark\t\t\thttp://snowplowanalytics.com/documentation/recipes/catalog-analytics/market-basket-analysis-identifying-products-that-sell-well-together.html\tMarket basket analysis - identifying products and content that go well together – Snowplow\thttp://snowplowanalytics.com/analytics/catalog-analytics/market-basket-analysis-identifying-products-that-sell-well-together.html\thttp\tsnowplowanalytics.com\t80\t/documentation/recipes/catalog-analytics/market-basket-analysis-identifying-products-that-sell-well-together.html\t\t\thttp\tsnowplowanalytics.com\t80\t/analytics/catalog-analytics/market-basket-analysis-identifying-products-that-sell-well-together.html\t\t\tinternal\t\t\t\t\t\t\t\t{\"schema\":\"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0\",\"data\":[{\"schema\":\"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0\",\"data\":{\"id\":\"05862d26-0dde-4d7a-a494-fc9aae283d23\"}},{\"schema\":\"iglu:org.schema/WebPage/jsonschema/1-0-0\",\"data\":{\"genre\":\"documentation\",\"inLanguage\":\"en-US\"}},{\"schema\":\"iglu:org.w3/PerformanceTiming/jsonschema/1-0-0\",\"data\":{\"navigationStart\":1459236496534,\"unloadEventStart\":1459236496838,\"unloadEventEnd\":1459236496838,\"redirectStart\":0,\"redirectEnd\":0,\"fetchStart\":1459236496534,\"domainLookupStart\":1459236496534,\"domainLookupEnd\":1459236496534,\"connectStart\":1459236496534,\"connectEnd\":1459236496534,\"secureConnectionStart\":0,\"requestStart\":1459236496580,\"responseStart\":1459236496834,\"responseEnd\":1459236496844,\"domLoading\":1459236496853,\"domInteractive\":1459236497780,\"domContentLoadedEventStart\":1459236497780,\"domContentLoadedEventEnd\":1459236498038,\"domComplete\":0,\"loadEventStart\":0,\"loadEventEnd\":0,\"chromeFirstPaint\":1459236498203}}]}\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tMozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36\tChrome 49\tChrome\t49.0.2623.87\tBrowser\tWEBKIT\ten-US\t1\t1\t0\t0\t0\t0\t0\t0\t0\t1\t24\t1920\t1075\tWindows 7\tWindows\tMicrosoft Corporation\tEurope/Berlin\tComputer\t0\t1920\t1200\tUTF-8\t1903\t11214\t\t\t\t\t\t\t\tEurope/Copenhagen\t\t\t\t2016-03-29 07:28:18.636\t\t\t{\"schema\":\"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1\",\"data\":[{\"schema\":\"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0\",\"data\":{\"useragentFamily\":\"Chrome\",\"useragentMajor\":\"49\",\"useragentMinor\":\"0\",\"useragentPatch\":\"2623\",\"useragentVersion\":\"Chrome 49.0.2623\",\"osFamily\":\"Windows\",\"osMajor\":\"7\",\"osMinor\":null,\"osPatch\":null,\"osPatchMinor\":null,\"osVersion\":\"Windows 7\",\"deviceFamily\":\"Other\"}}]}\t88c23330-ac4d-4c82-8a18-aa83c1e0c163\t2016-03-29 07:28:18.609\tcom.snowplowanalytics.snowplow\tpage_view\tjsonschema\t1-0-0\tcab5ba164038f31d8e10befc4eb199df\t"
      val event = Event.parse(line).getOrElse(throw new RuntimeException("Event is invalid"))
      val stream = Stream.emit[IO, Data](Data.Snowplow(event))

      val action = for {
        (issues, state) <- PgState.init[IO](xa, logger, igluClient.resolver, Schema)
        queue <- Queue.bounded[IO, BadData](1).action
        _ <- stream.through(sink.goodSink(xa, logger, Schema, state, queue, igluClient)).compile.drain.action
        eventIds <- query.action
        uaParserCtxs <- count("com_snowplowanalytics_snowplow_ua_parser_context_1").action
      } yield (issues, eventIds, uaParserCtxs)

      val result = action.value.unsafeRunSync()
      val ExpectedEventId = UUID.fromString("11cdec7b-4cbd-4aa4-a4c9-3874ab9663d4")
      result must beRight.like {
        case (Nil, List(ExpectedEventId), 1) => ok
        case (issues, ids, ctxs) => ko(s"Unexpected result. Issues: $issues; Event ids: $ids; Contexts: $ctxs")
      }
    }

    "sink a single self-describing JSON" >> {
      val row = json"""{"schema":"iglu:com.getvero/bounced/jsonschema/1-0-0","data":{"bounce_type":"one"}}"""
      val json = SelfDescribingData.parse(row).getOrElse(throw new RuntimeException("Invalid SelfDescribingData"))
      val stream = Stream.emit[IO, Data](Data.SelfDescribing(json))

      val action = for {
        (issues, state) <- PgState.init[IO](xa, logger, igluClient.resolver, Schema)
        queue <- Queue.bounded[IO, BadData](1).action
        _ <- stream.through(sink.goodSink(xa, logger, Schema, state, queue, igluClient)).compile.drain.action
        eventIds <- query.action
        rows <- count("com_getvero_bounced_1").action
      } yield (issues, eventIds, rows)

      val result = action.value.unsafeRunSync()
      result must beRight.like {
        case (Nil, Nil, 1) => ok
        case (issues, ids, ctxs) => ko(s"Unexpected result. Issues: ${issues.mkString(", ")}; Event ids: ${ids.mkString(", ")}; Contexts: $ctxs")
      }
    }

    "sink a several self-describing JSONs with migrations" >> {
      val rows = List(
        json"""{"schema":"iglu:me.chuwy/pg-test/jsonschema/1-0-0","data":{"requiredString":"one","requiredUnion":false,"nested":{"a": 1}}}""",
        json"""{"schema":"iglu:me.chuwy/pg-test/jsonschema/1-0-1","data":{"requiredString":"two", "requiredUnion": false, "nested": {"a": 2}, "someArray": [2,"two",{}]}}""",
        json"""{"schema":"iglu:me.chuwy/pg-test/jsonschema/1-0-2","data":{"requiredString":"three","requiredUnion":"three","nested":{"a": 3},"bigInt": 3}}"""
      ).map(SelfDescribingData.parse[Json]).map(_.getOrElse(throw new RuntimeException("Invalid SelfDescribingData"))).map(Data.SelfDescribing.apply)

      val stream = Stream.emits[IO, Data](rows)

      val ExpectedColumnInfo = List(
        ColumnInfo("required_string", None, false,  "character varying",            Some(4096)),
        ColumnInfo("required_union",  None, false,  "jsonb",                        None),
        ColumnInfo("id",              None, true,   "uuid",                         None),
        ColumnInfo("nested.a",        None, true,   "double precision",             None),
        ColumnInfo("nested.b",        None, true,   "character varying",            Some(4096)),
        ColumnInfo("some_array",      None, true,   "jsonb",                        None),
        ColumnInfo("nested.c",        None, true,   "bigint",                       None),
        ColumnInfo("some_date",       None, true,   "timestamp without time zone",  None),
        ColumnInfo("big_int",         None, true,   "bigint",                       None),
      )

      val action = for {
        (issues, state) <- PgState.init[IO](xa, logger, igluClient.resolver, Schema)
        queue <- Queue.bounded[IO, BadData](1).action
        _ <- stream.through(sink.goodSink(xa, logger, Schema, state, queue, igluClient)).compile.drain.action
        rows <- count("me_chuwy_pg_test_1").action
        table <- describeTable("me_chuwy_pg_test_1").action
      } yield (issues, rows, table)

      val result = action.value.unsafeRunSync()
      result must beRight.like {
        case (Nil, 3, ExpectedColumnInfo) => ok
        case (issues, ctxs, cols) => ko(s"Unexpected result. Issues: $issues; Number of rows: $ctxs; Columns ${cols}")
      }
    }
  }
}
