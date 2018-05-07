#!/bin/bash

ENV=$1

echo "Branch: ${CI_BRANCH}"

echo "Deploying API stack..."

/app/bin/rancher --debug --config /app/config/environ.$ENV.json up \
  --file         /app/config/stacks/api.docker.yml \
  --rancher-file /app/config/stacks/api.rancher.yml \
  -d \
  -s api \
  --force-upgrade \
  --pull &

echo "API stack deploy complete"

wait

echo "Cleaning up"

/app/bin/rancher --debug --config /app/config/environ.$ENV.json up \
  --file         /app/config/stacks/api.docker.yml \
  --rancher-file /app/config/stacks/api.rancher.yml \
  -d \
  -s api \
  --confirm-upgrade &

wait

echo "Deployed"
