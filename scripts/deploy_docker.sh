#!/bin/bash

set -x
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}

VERSION=`git describe --always`

docker push raintank/statsdaemon:$VERSION
docker push raintank/statsdaemon:latest
