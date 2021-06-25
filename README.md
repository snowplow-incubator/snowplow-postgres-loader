[![License][license-image]][license]
[![Coverage Status][coveralls-image]][coveralls]
[![Test][test-image]][test]
[![Docker][docker-image]][docker]

# Snowplow Postgres Loader

## Quickstart

Assuming [Docker][docker] is installed:

1. Add own `config.hocon` to specify connection and stream details, using [the examples][config] as a guide and [the docs site][config-docs] as a reference.
2. Add own [`resolver.json`][resolver] (all schemas must be on [Iglu Server 0.6.0+][iglu-server])
3. Run the Docker image:

```bash
$ docker run --rm -v $PWD/config:/snowplow/config \
    snowplow/snowplow-postgres-loader:latest \
    --resolver /snowplow/config/resolver.json \
    --config /snowplow/config/config.hocon
```

## Copyright and License

Snowplow Postgres Loader is copyright 2020-2021 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0][license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[config]: ./config/
[resolver]: ./config/resolver.json
[config-docs]: https://docs.snowplowanalytics.com/docs/pipeline-components-and-applications/loaders-storage-targets/snowplow-postgres-loader/postgres-loader-configuration-reference/

[docker]: https://www.docker.com/
[iglu-server]: https://github.com/snowplow-incubator/iglu-server

[docker]: https://hub.docker.com/r/snowplow/snowplow-postgres-loader/tags
[docker-image]: https://img.shields.io/docker/v/snowplow/snowplow-postgres-loader/latest

[test]: https://github.com/snowplow-incubator/snowplow-postgres-loader/actions?query=workflow%3ATest
[test-image]: https://github.com/snowplow-incubator/snowplow-postgres-loader/workflows/Test/badge.svg

[license]: http://www.apache.org/licenses/LICENSE-2.0
[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat

[coveralls]: https://coveralls.io/github/snowplow-incubator/snowplow-postgres-loader?branch=master
[coveralls-image]: https://coveralls.io/repos/github/snowplow-incubator/snowplow-postgres-loader/badge.svg?branch=master
