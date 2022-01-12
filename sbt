#!/bin/bash

set -eo pipefail

sbtver="1.6.1"
sbtjar="sbt-launch-$sbtver.jar"
sbtsha128="b69ab7f114d09d859b5e349acdb05d32c9d49072"

sbtrepo="https://repo1.maven.org/maven2/org/scala-sbt/sbt-launch"

if [ ! -f "sbt-launch.jar" ]; then
  echo "downloading $PWD/$sbtjar" 1>&2
  if ! curl --location --silent --fail -o "sbt-launch.jar" "$sbtrepo/$sbtver/$sbtjar"; then
    exit 1
  fi
fi

checksum=`openssl dgst -sha1 sbt-launch.jar | awk '{ print $2 }'`
if [ "$checksum" != $sbtsha128 ]; then
  echo "bad $PWD/sbt-launch.jar.  delete $PWD/sbt-launch.jar and run $0 again."
  exit 1
fi

[ -f ~/.sbtconfig ] && . ~/.sbtconfig

# the -DSKIP_SBT flag is set to skip tests that shouldn't be run with sbt.
java -ea                          \
  $SBT_OPTS                       \
  $JAVA_OPTS                      \
  -DSKIP_SBT=1                    \
  -jar "sbt-launch.jar" "$@"
