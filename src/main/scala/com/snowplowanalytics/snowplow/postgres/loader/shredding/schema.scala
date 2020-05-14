package com.snowplowanalytics.snowplow.postgres.loader.shredding

import cats.Monad
import cats.data.EitherT
import cats.syntax.option._

import cats.effect.Clock

import io.circe.Json

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.client.{ClientError, Resolver}
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaList, SchemaCriterion, SelfDescribingSchema, SchemaKey, SchemaMap}

import com.snowplowanalytics.iglu.schemaddl.{IgluSchema, Properties}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.iglu.schemaddl.migrations.{FlatSchema, SchemaList => DdlSchemaList}

import com.snowplowanalytics.snowplow.badrows.FailureDetails

/** Generic schema functionality, related to JSON schema (Iglu) transformations */
object schema {

  def fetch[F[_]: Monad: RegistryLookup: Clock](resolver: Resolver[F])
                                               (key: SchemaKey): EitherT[F, FailureDetails.LoaderIgluError, IgluSchema] =
    for {
      json <- EitherT(resolver.lookupSchema(key)).leftMap(error => FailureDetails.LoaderIgluError.IgluError(key, error): FailureDetails.LoaderIgluError)
      schema <- EitherT.fromEither[F](Schema.parse(json).toRight(buildFailure(json, key)))
    } yield SelfDescribingSchema(SchemaMap(key), schema)

  def buildFailure(json: Json, key: SchemaKey): FailureDetails.LoaderIgluError =
    FailureDetails.LoaderIgluError.InvalidSchema(key, s"JSON ${json.noSpaces} cannot be parsed as JSON Schema"): FailureDetails.LoaderIgluError


  def getOrdered[F[_]: Monad: RegistryLookup: Clock](resolver: Resolver[F])
                                                    (vendor: String, name: String, model: Int): EitherT[F, FailureDetails.LoaderIgluError, Properties] = {

    val criterion = SchemaCriterion(vendor, name, "jsonschema", Some(model), None, None)
    val schemaList = resolver.listSchemas(vendor, name, model)
    for {
      schemaList <- EitherT[F, ClientError.ResolutionError, SchemaList](schemaList).leftMap(error => FailureDetails.LoaderIgluError.SchemaListNotFound(criterion, error))
      ordered <- DdlSchemaList.fromSchemaList(schemaList, fetch(resolver))
      properties = FlatSchema.extractProperties(ordered)
    } yield properties
  }

}
