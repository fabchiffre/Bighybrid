#!/bin/bash

export LD_LIBRARY_PATH=/usr/local/lib

mkdir -p content
touch content/storage_content.txt

./hello_bighybrid.bin 2>&1 | /usr/local/bin/simgrid-colorizer
