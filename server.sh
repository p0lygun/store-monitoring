#!/bin/bash
if [ $# -ge 1  ]; then
  docker-compose -f docker/local/docker-compose.yml --env-file .env "$@"
else
  docker-compose -f docker/local/docker-compose.yml --env-file .env.template up --build
fi

