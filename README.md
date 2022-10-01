[![License][license-image]][license]
[![Coverage Status][coveralls-image]][coveralls]
[![Test][test-image]][test]
[![Docker][docker-image]][docker]

# Snowplow Postgres Loader

## Overview

The Snowplow Postgres Loader consumes records from an Amazon Kinesis stream and Google Pubsub topic and writes them to PostgreSQL database.

## Find out more

| Technical Docs             | Setup Guide           | Roadmap              | Contributing                |
|:--------------------------:|:---------------------:|:--------------------:|:---------------------------:|
| ![i1][techdocs-image]      | ![i2][setup-image]    | ![i3][roadmap-image] |![i4][contributing-image]    |
| [Technical Docs][techdocs] | [Setup Guide][config] | [Roadmap][roadmap]   |[Contributing][contributing] |


## Copyright and License

Snowplow Postgres Loader is copyright 2020-2021 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0][license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[config]: https://docs.snowplow.io/docs/pipeline-components-and-applications/loaders-storage-targets/snowplow-postgres-loader/postgres-loader-configuration-reference/
[techdocs]: https://docs.snowplow.io/docs/pipeline-components-and-applications/loaders-storage-targets/snowplow-postgres-loader/
[roadmap]: https://github.com/snowplow/snowplow/projects/7
[contributing]: https://docs.snowplow.io/docs/contributing/

[docker]: https://hub.docker.com/r/snowplow/snowplow-postgres-loader/tags
[docker-image]: https://img.shields.io/docker/v/snowplow/snowplow-postgres-loader/latest

[test]: https://github.com/snowplow-incubator/snowplow-postgres-loader/actions?query=workflow%3ATest
[test-image]: https://github.com/snowplow-incubator/snowplow-postgres-loader/workflows/Test/badge.svg

[license]: http://www.apache.org/licenses/LICENSE-2.0
[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat

[coveralls]: https://coveralls.io/github/snowplow-incubator/snowplow-postgres-loader?branch=master
[coveralls-image]: https://coveralls.io/repos/github/snowplow-incubator/snowplow-postgres-loader/badge.svg?branch=master

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[roadmap-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/roadmap.png
[contributing-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/contributing.png
