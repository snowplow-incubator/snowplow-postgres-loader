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
package com.snowplowanalytics.snowplow.postgres.storage

import cats.{Eq, Show}

import com.snowplowanalytics.iglu.core.ParseError

/** Error with table comment, preventing from `PgState` initialisation */
sealed trait CommentIssue extends Product with Serializable

object CommentIssue {

  /** Table missing a comment */
  case class Missing(table: String) extends CommentIssue

  /** Comment is not an Iglu URI */
  case class Invalid(table: String, comment: String, error: ParseError) extends CommentIssue

  implicit val commentIssueShow: Show[CommentIssue] = Show.show {
    case Missing(table) =>
      s"Iglu comment is missing in table $table; The table will be ignored"
    case Invalid(table, comment, error) =>
      s"Comment on table $table ($comment) is not valid Iglu URI (${error.code})"
  }

  implicit val commentIssueEq: Eq[CommentIssue] = Eq.fromUniversalEquals[CommentIssue]
}
