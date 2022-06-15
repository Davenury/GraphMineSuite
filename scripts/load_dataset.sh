#!/bin/bash

directory=$(dirname $0)

wget http://snap.stanford.edu/data/twitter.tar.gz
tar -xvf twitter.tar.gz -C $directory/../
