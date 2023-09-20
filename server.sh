#!/bin/bash
docker-compose -f docker/local/docker-compose.yml --env-file .env "$@"
