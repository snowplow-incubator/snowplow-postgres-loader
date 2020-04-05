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
package com.snowplowanalytics.snowplow.postgres.loader

import cats.data.NonEmptyList

import com.snowplowanalytics.snowplow.badrows.FailureDetails

package object streaming {

  type IgluErrors = NonEmptyList[FailureDetails.LoaderIgluError]

  object IgluErrors {
    def of(error: FailureDetails.LoaderIgluError): NonEmptyList[FailureDetails.LoaderIgluError] =
      NonEmptyList.of(error)
  }
}
