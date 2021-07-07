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
package com.snowplowanalytics.snowplow.postgres.storage

import doobie.Fragment
import doobie.implicits._

import com.snowplowanalytics.snowplow.postgres.shredding.Type

object definitions {

  /** Columns prepended to every shredded type table */
  val metaColumns: List[(String, Type, Boolean)] = List(
    ("schema_vendor", Type.Varchar(128), true),
    ("schema_name", Type.Varchar(128), true),
    ("schema_format", Type.Varchar(128), true),
    ("schema_version", Type.Varchar(128), true),
    ("root_id", Type.Uuid, true),
    ("root_tstamp", Type.Timestamp, true)
  )

  val atomicColumns: List[(String, Type, Boolean)] = List(
    // App
    ("app_id", Type.Varchar(255), false),
    ("platform", Type.Varchar(255), false),
    // Date/time
    ("etl_tstamp", Type.Timestamp, false),
    ("collector_tstamp", Type.Timestamp, true),
    ("dvce_created_tstamp", Type.Timestamp, false),
    // Date/time
    ("event", Type.Varchar(128), false),
    ("event_id", Type.Uuid, true),
    ("txn_id", Type.Integer, false),
    // Versioning
    ("name_tracker", Type.Varchar(128), false),
    ("v_tracker", Type.Varchar(100), false),
    ("v_collector", Type.Varchar(100), true),
    ("v_etl", Type.Varchar(100), true),
    // User and visit
    ("user_id", Type.Varchar(255), false),
    ("user_ipaddress", Type.Varchar(45), false),
    ("user_fingerprint", Type.Varchar(50), false),
    ("domain_userid", Type.Varchar(36), false),
    ("domain_sessionidx", Type.Integer, false),
    ("network_userid", Type.Varchar(38), false),
    // Location
    ("geo_country", Type.Char(2), false),
    ("geo_region", Type.Char(3), false),
    ("geo_city", Type.Varchar(75), false),
    ("geo_zipcode", Type.Varchar(15), false),
    ("geo_latitude", Type.Double, false),
    ("geo_longitude", Type.Double, false),
    ("geo_region_name", Type.Varchar(100), false),
    // IP lookups
    ("ip_isp", Type.Varchar(100), false),
    ("ip_organization", Type.Varchar(100), false),
    ("ip_domain", Type.Varchar(100), false),
    ("ip_netspeed", Type.Varchar(100), false),
    // Page
    ("page_url", Type.Varchar(4096), false),
    ("page_title", Type.Varchar(2000), false),
    ("page_referrer", Type.Varchar(4096), false),
    // Page URL components
    ("page_urlscheme", Type.Varchar(16), false),
    ("page_urlhost", Type.Varchar(255), false),
    ("page_urlport", Type.Integer, false),
    ("page_urlpath", Type.Varchar(3000), false),
    ("page_urlquery", Type.Varchar(6000), false),
    ("page_urlfragment", Type.Varchar(3000), false),
    // Referrer URL components
    ("refr_urlscheme", Type.Varchar(16), false),
    ("refr_urlhost", Type.Varchar(255), false),
    ("refr_urlport", Type.Integer, false),
    ("refr_urlpath", Type.Varchar(6000), false),
    ("refr_urlquery", Type.Varchar(6000), false),
    ("refr_urlfragment", Type.Varchar(3000), false),
    // Referrer details
    ("refr_medium", Type.Varchar(25), false),
    ("refr_source", Type.Varchar(50), false),
    ("refr_term", Type.Varchar(255), false),
    // Marketing
    ("mkt_medium", Type.Varchar(255), false),
    ("mkt_source", Type.Varchar(255), false),
    ("mkt_term", Type.Varchar(255), false),
    ("mkt_content", Type.Varchar(500), false),
    ("mkt_campaign", Type.Varchar(255), false),
    // Custom structured event
    ("se_category", Type.Varchar(1000), false),
    ("se_action", Type.Varchar(1000), false),
    ("se_label", Type.Varchar(1000), false),
    ("se_property", Type.Varchar(1000), false),
    ("se_value", Type.Double, false),
    // Ecommerce
    ("tr_orderid", Type.Varchar(255), false),
    ("tr_affiliation", Type.Varchar(255), false),
    ("tr_total", Type.Double, false),
    ("tr_tax", Type.Double, false),
    ("tr_shipping", Type.Double, false),
    ("tr_city", Type.Varchar(255), false),
    ("tr_state", Type.Varchar(255), false),
    ("tr_country", Type.Varchar(255), false),
    ("ti_orderid", Type.Varchar(255), false),
    ("ti_sku", Type.Varchar(255), false),
    ("ti_name", Type.Varchar(255), false),
    ("ti_category", Type.Varchar(255), false),
    ("ti_price", Type.Double, false),
    ("ti_quantity", Type.Integer, false),
    // Page ping
    ("pp_xoffset_min", Type.Integer, false),
    ("pp_xoffset_max", Type.Integer, false),
    ("pp_yoffset_min", Type.Integer, false),
    ("pp_yoffset_max", Type.Integer, false),
    // User Agent
    ("useragent", Type.Varchar(1000), false),
    // Browser
    ("br_name", Type.Varchar(50), false),
    ("br_family", Type.Varchar(50), false),
    ("br_version", Type.Varchar(50), false),
    ("br_type", Type.Varchar(50), false),
    ("br_renderengine", Type.Varchar(50), false),
    ("br_lang", Type.Varchar(255), false),
    ("br_features_pdf", Type.Bool, false),
    ("br_features_flash", Type.Bool, false),
    ("br_features_java", Type.Bool, false),
    ("br_features_director", Type.Bool, false),
    ("br_features_quicktime", Type.Bool, false),
    ("br_features_realplayer", Type.Bool, false),
    ("br_features_windowsmedia", Type.Bool, false),
    ("br_features_gears", Type.Bool, false),
    ("br_features_silverlight", Type.Bool, false),
    ("br_cookies", Type.Bool, false),
    ("br_colordepth", Type.Varchar(12), false),
    ("br_viewwidth", Type.Integer, false),
    ("br_viewheight", Type.Integer, false),
    // Operating System
    ("os_name", Type.Varchar(50), false),
    ("os_family", Type.Varchar(50), false),
    ("os_manufacturer", Type.Varchar(50), false),
    ("os_timezone", Type.Varchar(50), false),
    // Device/Hardware
    ("dvce_type", Type.Varchar(50), false),
    ("dvce_ismobile", Type.Bool, false),
    ("dvce_screenwidth", Type.Integer, false),
    ("dvce_screenheight", Type.Integer, false),
    // Document
    ("doc_charset", Type.Varchar(128), false),
    ("doc_width", Type.Integer, false),
    ("doc_height", Type.Integer, false),
    // Currency
    ("tr_currency", Type.Char(3), false),
    ("tr_total_base", Type.Double, false),
    ("tr_tax_base", Type.Double, false),
    ("tr_shipping_base", Type.Double, false),
    ("ti_currency", Type.Char(3), false),
    ("ti_price_base", Type.Double, false),
    ("base_currency", Type.Char(3), false),
    // Geolocation
    ("geo_timezone", Type.Varchar(64), false),
    // Click ID
    ("mkt_clickid", Type.Varchar(128), false),
    ("mkt_network", Type.Varchar(64), false),
    // ETL tags
    ("etl_tags", Type.Varchar(500), false),
    // Time event was sent
    ("dvce_sent_tstamp", Type.Timestamp, false),
    // Referer
    ("refr_domain_userid", Type.Varchar(36), false),
    ("refr_dvce_tstamp", Type.Timestamp, false),
    // Session ID
    ("domain_sessionid", Type.Uuid, false),
    // Derived Type.Timestamp
    ("derived_tstamp", Type.Timestamp, false),
    // Event schema
    ("event_vendor", Type.Varchar(1000), false),
    ("event_name", Type.Varchar(1000), false),
    ("event_format", Type.Varchar(128), false),
    ("event_version", Type.Varchar(128), false),
    // Event fingerprint
    ("event_fingerprint", Type.Varchar(128), false),
    // True Type.Timestamp
    ("true_tstamp", Type.Timestamp, false)
  )

  def atomicSql(schema: String) = {
    val columns = atomicColumns
      .map {
        case (n, t, true)  => Fragment.const(s"$n ${t.ddl} NOT NULL")
        case (n, t, false) => Fragment.const(s"$n ${t.ddl}")
      }
      .foldLeft(Fragment.empty) { (acc, cur) =>
        val separator = if (acc == Fragment.empty) Fragment.const("\n") else Fragment.const(",\n")
        acc ++ separator ++ cur
      }

    val schemaFr = Fragment.const0(schema)
    val tableFr = Fragment.const0(EventsTableName)

    fr"""CREATE TABLE $schemaFr.$tableFr ($columns) WITH (OIDS=FALSE)"""
  }

  def columnToString(columnName: String, dataType: Type, nullable: Boolean) = {
    val notNull = if (nullable) "NULL" else "NOT NULL"
    s""""$columnName" ${dataType.ddl} $notNull"""
  }

  val EventsTableName = "events"
}
