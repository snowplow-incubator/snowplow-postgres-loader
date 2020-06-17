#!/bin/sh

set -e

if [ -z ${GITHUB_WORKSPACE+x} ]; then
    echo "GITHUB_WORKSPACE is unset";
    exit 1
fi

IGLUCTL_ZIP="igluctl_0.6.0.zip"
IGLUCTL_URI="http://dl.bintray.com/snowplow/snowplow-generic/$IGLUCTL_ZIP"
IGLUCENTRAL_PATH="$GITHUB_WORKSPACE/iglu-central"
SCHEMAS_PATH="$IGLUCENTRAL_PATH/schemas/" 
TEST_SCHEMAS="$GITHUB_WORKSPACE/.github/schemas/"
POSTGRES_PASSWORD=mysecretpassword

git clone https://github.com/snowplow/iglu-central.git $IGLUCENTRAL_PATH

docker run \
    -p 8080:8080 \
    -v $GITHUB_WORKSPACE/.github:/iglu \
    --rm -d \
    snowplow-docker-registry.bintray.io/snowplow/iglu-server:0.6.0 \
    --config /iglu/server.conf

wget $IGLUCTL_URI
unzip -j $IGLUCTL_ZIP

echo "Waiting for Iglu Server..."
sleep 6

./igluctl static push \
    $SCHEMAS_PATH \
    http://localhost:8080/ \
    48b267d7-cd2b-4f22-bae4-0f002008b5ad \
    --public

./igluctl static push \
    $TEST_SCHEMAS \
    http://localhost:8080/ \
    48b267d7-cd2b-4f22-bae4-0f002008b5ad \
    --public

PGPASSWORD=$POSTGRES_PASSWORD psql -h localhost -U postgres -c "CREATE DATABASE snowplow"
