#!/usr/bin/env bash

if [ $# -ne 1 ]; then
  echo 'usage: '$0' nettyjarpath' 1>&2
  exit 1
fi

for p in finagle-core finagle-test; do
  rm -f $p/lib_managed/compile/netty*
  mkdir -p $p/libs/
  rm -f $p/libs/netty*
  ln -s $1 $p/libs
done
