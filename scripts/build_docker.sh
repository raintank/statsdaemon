#!/bin/bash

set -x
# Find the directory we exist within
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd ${DIR}

VERSION=`git describe --always`

mkdir build
cp ../build/statsdaemon build/
cp ../statsdaemon.ini build/

docker build -t raintank/statsdaemon:$VERSION .
docker tag raintank/statsdaemon:$VERSION raintank/statsdaemon:latest
