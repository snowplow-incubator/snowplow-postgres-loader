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
package com.snowplowanalytics.snowplow.postgres.streaming

import com.snowplowanalytics.iglu.core.SelfDescribingData

import io.circe.Json

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

object data {

  /** Kind of data flowing through the Loader */
  sealed trait Data extends Product with Serializable {
    def snowplow: Boolean =
      this match {
        case _: Data.Snowplow       => true
        case _: Data.SelfDescribing => false
      }
  }

  object Data {
    case class Snowplow(data: Event) extends Data
    case class SelfDescribing(data: SelfDescribingData[Json]) extends Data
  }
}
