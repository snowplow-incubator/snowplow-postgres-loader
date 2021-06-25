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
package com.snowplowanalytics.snowplow.postgres.logging

import doobie.util.log._
import org.log4s.Logger

/** Log doobie events using slf4j framework
 *
 *  This is largely based on the jdk-based log handler supplied by doobie: https://github.com/tpolecat/doobie/blob/f04a7a3cab5aecb50be0d1ad10fbdae6b8db5ec2/modules/core/src/main/scala/doobie/util/log.scala#L57
 *  It uses slf4j as the logging abstraction, so the end user can control the log output using their preferred log framework
 */
object Slf4jLogHandler {
  def apply(logger: Logger): LogHandler =
    LogHandler {
      case Success(s, a, e1, e2) =>
        logger.debug(s"""Successful Statement Execution:
                        |
                        |  ${s.linesIterator.dropWhile(_.trim.isEmpty).mkString("\n  ")}
                        |
                        | arguments = [${a.mkString(", ")}]
                        |   elapsed = ${e1.toMillis.toString} ms exec + ${e2.toMillis.toString} ms processing (${(e1 + e2).toMillis.toString} ms total)
        """.stripMargin)

      case ProcessingFailure(s, a, e1, e2, t) =>
        logger.debug(s"""Failed Resultset Processing:
                        |
                        |  ${s.linesIterator.dropWhile(_.trim.isEmpty).mkString("\n  ")}
                        |
                        | arguments = [${a.mkString(", ")}]
                        |   elapsed = ${e1.toMillis.toString} ms exec + ${e2.toMillis.toString} ms processing (failed) (${(e1 + e2)
          .toMillis
          .toString} ms total)
                        |   failure = ${t.getMessage}
        """.stripMargin)
        logger.error(s"Failed Resultset Processing: ${t.getMessage}")

      case ExecFailure(s, a, e1, t) =>
        logger.debug(s"""Failed Statement Execution:
                        |
                        |  ${s.linesIterator.dropWhile(_.trim.isEmpty).mkString("\n  ")}
                        |
                        | arguments = [${a.mkString(", ")}]
                        |   elapsed = ${e1.toMillis.toString} ms exec (failed)
                        |   failure = ${t.getMessage}
        """.stripMargin)
        logger.error(s"Failed StatementExecution: ${t.getMessage}")
    }
}
