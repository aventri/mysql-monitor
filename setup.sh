#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

pip install --upgrade pip
pip install --no-cache-dir -r $DIR/requirements.txt

if [ ! -f $DIR/data/cacert.pem ]; then
	echo "Downloading certificate authority bundle"
	curl --silent -o $DIR/data/cacert.pem https://curl.haxx.se/ca/cacert.pem
fi

if [ ! -f $DIR/config.cfg ]; then
    cp $DIR/config-example.cfg $DIR/config.cfg
fi
