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
package com.snowplowanalytics.snowplow.postgres.shredding

import cats.Monad
import cats.data.EitherT

import cats.effect.Clock

import io.circe.Json

import com.snowplowanalytics.iglu.client.{ClientError, Resolver}
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaList, SchemaMap, SelfDescribingSchema}

import com.snowplowanalytics.iglu.schemaddl.{IgluSchema, Properties}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.{Type => SType}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.iglu.schemaddl.migrations.{FlatSchema, SchemaList => DdlSchemaList}

import com.snowplowanalytics.snowplow.badrows.FailureDetails

/** Generic schema functionality, related to JSON schema (Iglu) transformations */
object schema {

  def fetch[F[_]: Monad: RegistryLookup: Clock](
    resolver: Resolver[F]
  )(key: SchemaKey): EitherT[F, FailureDetails.LoaderIgluError, IgluSchema] =
    for {
      json <- EitherT(resolver.lookupSchema(key)).leftMap(error =>
        FailureDetails.LoaderIgluError.IgluError(key, error): FailureDetails.LoaderIgluError
      )
      schema <- EitherT.fromEither[F](Schema.parse(json).toRight(buildFailure(json, key)))
    } yield SelfDescribingSchema(SchemaMap(key), schema)

  def buildFailure(json: Json, key: SchemaKey): FailureDetails.LoaderIgluError =
    FailureDetails
      .LoaderIgluError
      .InvalidSchema(key, s"JSON ${json.noSpaces} cannot be parsed as JSON Schema"): FailureDetails.LoaderIgluError

  def getSchemaList[F[_]: Monad: RegistryLookup: Clock](
    resolver: Resolver[F]
  )(vendor: String, name: String, model: Int): EitherT[F, FailureDetails.LoaderIgluError, DdlSchemaList] = {

    val criterion = SchemaCriterion(vendor, name, "jsonschema", Some(model), None, None)
    val schemaList = resolver.listSchemas(vendor, name, model)
    for {
      schemaList <- EitherT[F, ClientError.ResolutionError, SchemaList](schemaList).leftMap(error =>
        FailureDetails.LoaderIgluError.SchemaListNotFound(criterion, error)
      )
      ordered <- DdlSchemaList.fromSchemaList(schemaList, fetch(resolver))
    } yield ordered
  }

  def getOrdered[F[_]: Monad: RegistryLookup: Clock](
    resolver: Resolver[F]
  )(vendor: String, name: String, model: Int): EitherT[F, FailureDetails.LoaderIgluError, Properties] =
    getSchemaList[F](resolver)(vendor, name, model).map(FlatSchema.extractProperties)

  def canBeNull(schema: Schema): Boolean =
    schema.enum.exists(_.value.exists(_.isNull)) || (schema.`type` match {
      case Some(SType.Union(types)) => types.contains(SType.Null)
      case Some(t)                  => t == SType.Null
      case None                     => false
    })
}
