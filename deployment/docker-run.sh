#!/usr/bin/env bash

type docker-machine &>/dev/null
if [ $? -eq 0 ]; then
    eval $(docker-machine env default)
fi

docker-compose up