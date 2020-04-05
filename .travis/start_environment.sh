#!/bin/sh

set -e

if [ -z ${TRAVIS_BUILD_DIR+x} ]; then 
    echo "TRAVIS_BUILD_DIR is unset"; 
    exit 1
fi

IGLUCTL_ZIP="igluctl_0.6.0.zip"
IGLUCTL_URI="http://dl.bintray.com/snowplow/snowplow-generic/$IGLUCTL_ZIP"
IGLUCENTRAL_PATH="$TRAVIS_BUILD_DIR/iglu-central"
SCHEMAS_PATH="$IGLUCENTRAL_PATH/schemas/" 
TEST_SCHEMAS="$TRAVIS_BUILD_DIR/.travis/schemas/"
POSTGRES_PASSWORD=mysecretpassword

# git clone https://github.com/snowplow/iglu-central.git $IGLUCENTRAL_PATH
# 
# docker run \
#     -p 8080:8080 \
#     -v $TRAVIS_BUILD_DIR/.travis:/iglu \
#     --rm -d \
#     snowplow-docker-registry.bintray.io/snowplow/iglu-server:0.6.0 \
#     --config /iglu/server.conf
# 
# wget $IGLUCTL_URI
# unzip -j $IGLUCTL_ZIP
# 
# echo "Waiting for Iglu Server..."
# sleep 6
# 
# ./igluctl static push \
#     $SCHEMAS_PATH \
#     http://localhost:8080/ \
#     48b267d7-cd2b-4f22-bae4-0f002008b5ad \
#     --public

./igluctl static push \
    $TEST_SCHEMAS \
    http://localhost:8080/ \
    48b267d7-cd2b-4f22-bae4-0f002008b5ad \
    --public


# docker run \
#     -p 5432:5432 \
#     --rm \
#     --name pg-loader-box \
#     -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
#     -d postgres
# 
# echo "Waiting for Postgres..."
# sleep 3
# 
# PGPASSWORD=$POSTGRES_PASSWORD psql -h localhost -U postgres -c "CREATE DATABASE snowplow"
