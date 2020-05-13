package com.snowplowanalytics.snowplow.postgres.loader.streaming

import cats.effect.concurrent.Ref
import cats.effect.Sync

import com.snowplowanalytics.iglu.core.SchemaKey

import com.snowplowanalytics.iglu.schemaddl.ModelGroup
import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaList

import com.snowplowanalytics.snowplow.postgres.loader.streaming.sink.TableState

/** State of the DB schema */
case class PgState(tables: Map[ModelGroup, SchemaList]) {
  private[loader] def check(group: ModelGroup, entity: SchemaKey): TableState =
    tables.get(group) match {
      case Some(SchemaList.Full(schemas)) =>
        if (schemas.toList.map(_.self.schemaKey).contains(entity)) TableState.Match else TableState.Outdated
      case Some(SchemaList.Single(schema)) =>
        if (schema.self.schemaKey == entity) TableState.Match else TableState.Outdated
      case None =>
        TableState.Missing
    }
}

object PgState {
  def init[F[_]: Sync]: F[Ref[F, PgState]] =
    Ref.of[F, PgState](PgState(Map.empty))
}

