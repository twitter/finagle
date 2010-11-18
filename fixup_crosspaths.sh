#!/bin/sh

for dir in lib_managed target; do
  (cd $dir; rm -f scala_2.8.1; ln -s . scala_2.8.1)
done