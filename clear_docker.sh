#!/bin/bash
docker ps -aq | xargs -r docker rm -f
