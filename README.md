[![License][license-image]][license]
[![Coverage Status][coveralls-image]][coveralls]
[![Test][test-image]][test]
[![Release][release-image]][release]

# Snowplow Postgres Loader

## Quickstart

Assuming [Docker][docker] is installed:

1. Add own `config.json` (specify connection and stream details)
2. Add own `resolver.json` (all schemas must be on [Iglu Server][iglu-server])
3. Run the Docker image:

```bash
$ docker run --rm -v $PWD/config:/snowplow/config snowplow-postgres-loader \
    --resolver /snowplow/config/resolver.json \
    --config /snowplow/config/config.json
```

## Copyright and License

Snowplow pg-loader is copyright 2020 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0][license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[docker]: https://www.docker.com/
[iglu-server]: https://github.com/snowplow-incubator/iglu-server

[release-image]: http://img.shields.io/badge/release-0.1.0-blue.svg?style=flat
[release]: https://github.com/snowplow/pgloader/releases

[test]: https://github.com/snowplow/enrich/actions?query=workflow%3ATest
[test-image]: https://github.com/snowplow/enrich/workflows/Test/badge.svg

[license]: http://www.apache.org/licenses/LICENSE-2.0
[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat

[coveralls]: https://coveralls.io/github/snowplow/enrich?branch=master
[coveralls-image]: https://coveralls.io/repos/github/snowplow/enrich/badge.svg?branch=master
