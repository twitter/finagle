#!/bin/bash

for d in finagle-*; do
	t=$d/target/doc/main/api
	if [ -d $t ]; then
		echo "copying $d" >&2
		mkdir -p api/$d
		rm -rf api/$d/*
		cp -r $t/* api/$d
	fi
done
