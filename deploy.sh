#!/bin/bash

docker login -e="." -u="$QUAY_USERNAME" -p="$QUAY_PASSWORD" quay.io
docker tag keboola/storage-api-tests quay.io/keboola/storage-api-tests:latest
docker images
docker push quay.io/keboola/storage-api-tests:latest