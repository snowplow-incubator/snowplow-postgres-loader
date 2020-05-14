package com.snowplowanalytics.snowplow.postgres.loader

import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.snowplow.postgres.loader.generated.ProjectSettings

case class Config(
  appName: String,
  stream: String,
  host: String,
  port: Int,
  database: String,
  username: String,
  password: String
)
object Config {
  val processor: Processor = Processor(ProjectSettings.name, ProjectSettings.version)
}
