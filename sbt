#!/bin/bash

root=$(
  cd $(dirname $(readlink $0 || echo $0))/..
  /bin/pwd
)

sbtjar=sbt-launch-0.7.5.jar

if [ ! -f $sbtjar ]; then
  echo 'downloading '$sbtjar 1>&2
  curl -O http://simple-build-tool.googlecode.com/files/$sbtjar
fi

test -f $sbtjar || exit 1

java -ea                          \
  $JAVA_OPTS                      \
  -Djava.net.preferIPv4Stack=true \
  -XX:+AggressiveOpts             \
  -XX:+UseParNewGC                \
  -XX:+UseConcMarkSweepGC         \
  -XX:+CMSParallelRemarkEnabled   \
  -XX:+CMSClassUnloadingEnabled   \
  -XX:MaxPermSize=1024m           \
  -XX:SurvivorRatio=128           \
  -XX:MaxTenuringThreshold=0      \
  -Xss8M                          \
  -Xms512M                        \
  -Xmx3G                          \
  -server                         \
  -jar $sbtjar "$@"
