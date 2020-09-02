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
package com.snowplowanalytics.snowplow.postgres.config

import com.zaxxer.hikari.HikariConfig
import DBConfig.JdbcUri

case class DBConfig(host: String,
                    port: Int,
                    database: String,
                    username: String,
                    password: String, // TODO: can be EC2 store
                    sslMode: String,
                    schema: String
) {
  def getJdbc: JdbcUri =
    JdbcUri(host, port, database, sslMode.toLowerCase().replace('_', '-'))
}

object DBConfig {

  case class JdbcUri(host: String, port: Int, database: String, sslMode: String) {
    override def toString =
      s"jdbc:postgresql://$host:$port/$database?sslmode=$sslMode"
  }

  def hikariConfig(dbConfig: DBConfig) = {
    val config = new HikariConfig()
    config.setDriverClassName("org.postgresql.Driver")
    config.setJdbcUrl(dbConfig.getJdbc.toString)
    config.setUsername(dbConfig.username)
    config.setPassword(dbConfig.password)
    // TODO: DBConfig could take a MaxConnections field, and set `config.setMaximumPoolSize`.
    config
  }

}
