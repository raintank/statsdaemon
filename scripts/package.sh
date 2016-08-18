#!/bin/bash
set -x
BASE=$(dirname $0)
CODE_DIR=$(readlink -e "$BASE/../")

sudo apt-get install rpm # to be able to make rpms

BUILD_ROOT=$CODE_DIR/build

ARCH="$(uname -m)"
VERSION=$(git describe --long --always)

## debian wheezy
BUILD=${BUILD_ROOT}/sysvinit
mkdir -p ${BUILD}/usr/sbin
mkdir -p ${BUILD}/etc

cp ${BASE}/statsdaemon.ini ${BUILD}/etc/
cp ${BUILD_ROOT}/statsdaemon ${BUILD}/usr/sbin/

PACKAGE_NAME="${BUILD}/statsdaemon-${VERSION}_${ARCH}.deb"
fpm -s dir -t deb \
  -v ${VERSION} -n statsdaemon -a ${ARCH} --description "Metrics aggregation daemon like statsd, in Go." \
  --deb-init ${BASE}/config/sysvinit/init.d/statsdaemon \
  --deb-default ${BASE}/config/sysvinit/default/statsdaemon \
  -C ${BUILD} -p ${PACKAGE_NAME} .

## ubuntu 14.04
BUILD=${BUILD_ROOT}/upstart

mkdir -p ${BUILD}/usr/sbin
mkdir -p ${BUILD}/etc

cp ${BASE}/statsdaemon.ini ${BUILD}/etc/
cp ${BUILD_ROOT}/statsdaemon ${BUILD}/usr/sbin/

PACKAGE_NAME="${BUILD}/statsdaemon-${VERSION}_${ARCH}.deb"
fpm -s dir -t deb \
  -v ${VERSION} -n statsdaemon -a ${ARCH} --description "Metrics aggregation daemon like statsd, in Go." \
  --deb-upstart ${BASE}/config/upstart/statsdaemon \
  -C ${BUILD} -p ${PACKAGE_NAME} .

## ubuntu 16.04, Debian 8, CentOS 7
BUILD=${BUILD_ROOT}/systemd

mkdir -p ${BUILD}/usr/sbin
mkdir -p ${BUILD}/lib/systemd/system/
mkdir -p ${BUILD}/etc
mkdir -p ${BUILD}/var/run/statsdaemon

cp ${BASE}/statsdaemon.ini ${BUILD}/etc/
cp ${BUILD_ROOT}/statsdaemon ${BUILD}/usr/sbin/
cp ${BASE}/config/systemd/statsdaemon.service $BUILD/lib/systemd/system/

PACKAGE_NAME="${BUILD}/statsdaemon-${VERSION}_${ARCH}.deb"
fpm -s dir -t deb \
  -v ${VERSION} -n statsdaemon -a ${ARCH} --description "Metrics aggregation daemon like statsd, in Go." \
  --license "Apache2.0" -C ${BUILD} -p ${PACKAGE_NAME} .

BUILD=${BUILD_ROOT}/systemd-centos7

mkdir -p ${BUILD}/usr/sbin
mkdir -p ${BUILD}/lib/systemd/system/
mkdir -p ${BUILD}/etc
mkdir -p ${BUILD}/var/run/statsdaemon

cp ${BASE}/statsdaemon.ini ${BUILD}/etc/
cp ${BUILD_ROOT}/statsdaemon ${BUILD}/usr/sbin/
cp ${BASE}/config/systemd/statsdaemon.service $BUILD/lib/systemd/system/

PACKAGE_NAME="${BUILD}/statsdaemon-${VERSION}.el7.${ARCH}.rpm"
fpm -s dir -t rpm \
  -v ${VERSION} -n statsdaemon -a ${ARCH} --description "Metrics aggregation daemon like statsd, in Go." \
  --license "Apache2.0" -C ${BUILD} -p ${PACKAGE_NAME} .

## CentOS 6
BUILD=${BUILD_ROOT}/upstart-0.6.5

mkdir -p ${BUILD}/etc/init
mkdir -p ${BUILD}/usr/sbin
mkdir -p ${BUILD}/etc

cp ${BASE}/statsdaemon.ini ${BUILD}/etc/
cp ${BUILD_ROOT}/statsdaemon ${BUILD}/usr/sbin/
cp ${BASE}/config/upstart-0.6.5/statsdaemon.conf $BUILD/etc/init

PACKAGE_NAME="${BUILD}/statsdaemon-${VERSION}.el6.${ARCH}.rpm"
fpm -s dir -t rpm \
  -v ${VERSION} -n statsdaemon -a ${ARCH} --description "Metrics aggregation daemon like statsd, in Go." \
  -C ${BUILD} -p ${PACKAGE_NAME} .

