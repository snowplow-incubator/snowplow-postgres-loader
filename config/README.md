## Example snowplow-postgres-loader configuration files

These example hocon files can be provided with the `--config` command line argument.

* [`config.kinesis.minimal.hocon`](./config.kinesis.minimal.hocon): A minimal config file containing
only the required fields when streaming from AWS kinesis.
* [`config.pubsub.minimal.hocon`](./config.pubsub.minimal.hocon): A minimal config file containing
only the required fields when streaming from GCP PubSub.
* [`config.kinesis.reference.hocon`](./config.kinesis.reference.hocon): A complete config file demonstrating
all relevant fields when streaming from AWS kinesis
* [`config.pubsub.reference.hocon`](./config.pubsub.reference.hocon): A complete config file demonstrating
all relevant fields when streaming from GCP PubSub.

#### Iglu resolver config

[`resolver.json`](./resolver.json) is an example iglu resolver file, to be provided with the `--resolver`
command line argument, required in addition to the main hocon file.

All repositories listed in the resolver must be Iglu Servers version 0.6.0 or above.


#### Links

See [the lightbend config readme](https://github.com/lightbend/config/blob/main/README.md) for examples
of how to provide config parameters using environment variable substitutions or system properties.

See [the snowplow docs site](https://docs.snowplowanalytics.com/docs/pipeline-components-and-applications/loaders-storage-targets/snowplow-postgres-loader/postgres-loader-configuration-reference/)
for the full snowplow-postgres-loader configuration reference.
