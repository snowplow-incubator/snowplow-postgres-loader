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
package com.snowplowanalytics.snowplow.postgres.loader.config

import java.util.Base64

import cats.syntax.either._
import cats.data.ValidatedNel

import io.circe.Json
import io.circe.parser.{ parse => jsonParse }

import com.monovore.decline.Argument

/** Base64-encoded JSON */
case class Base64Encoded(json: Json) extends AnyVal

object Base64Encoded {
  def parse(string: String): Either[String, Base64Encoded] =
    Either
      .catchOnly[IllegalArgumentException](Base64.getDecoder.decode(string))
      .map(bytes => new String(bytes))
      .flatMap(str => jsonParse(str))
      .leftMap(e => s"Cannot parse ${string} as Base64-encoded JSON: ${e.getMessage}")
      .map(json => Base64Encoded(json))

  implicit def base64EncodedDeclineArg: Argument[Base64Encoded] =
    new Argument[Base64Encoded] {
      def read(string:  String): ValidatedNel[String, Base64Encoded] =
        Base64Encoded.parse(string).toValidatedNel

      def defaultMetavar: String = "base64"
    }

}


