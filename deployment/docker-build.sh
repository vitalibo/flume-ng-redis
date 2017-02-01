#!/usr/bin/env bash

VERSION='1.7.0'
if [ ! -z "$1" ]; then
    VERSION=$1
fi

type docker-machine &>/dev/null
if [ $? -eq 0 ]; then
    eval $(docker-machine env default)
fi

docker build --build-arg "VERSION=${VERSION}" -t "flume-ng:${VERSION}" .
