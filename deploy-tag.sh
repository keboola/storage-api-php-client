#!/bin/bash

docker login -u="$QUAY_USERNAME" -p="$QUAY_PASSWORD" quay.io
docker tag keboola/storage-api-tests quay.io/keboola/storage-api-tests:$TRAVIS_TAG
docker images
docker push quay.io/keboola/storage-api-tests:$TRAVIS_TAG
