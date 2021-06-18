#!/bin/sh

set -e

if [ -z ${GITHUB_WORKSPACE+x} ]; then
    echo "GITHUB_WORKSPACE is unset";
    exit 1
fi

IGLUCTL_URI="https://github.com/snowplow-incubator/igluctl/releases/download/0.7.2/igluctl"
IGLUCENTRAL_PATH="$GITHUB_WORKSPACE/iglu-central"
SCHEMAS_PATH="$IGLUCENTRAL_PATH/schemas/" 
TEST_SCHEMAS="$GITHUB_WORKSPACE/.github/schemas/"
POSTGRES_PASSWORD=mysecretpassword

git clone https://github.com/snowplow/iglu-central.git $IGLUCENTRAL_PATH

docker run \
    -p 8080:8080 \
    -v $GITHUB_WORKSPACE/.github:/iglu \
    --rm -d \
    snowplow/iglu-server:0.6.1 \
    --config /iglu/server.conf

echo "Waiting for Iglu Server..."
sleep 5

wget $IGLUCTL_URI
chmod 755 igluctl

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
