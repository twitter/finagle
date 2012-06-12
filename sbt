#!/bin/bash

root=$(
  cd $(dirname $(readlink $0 || echo $0))/..
  /bin/pwd
)

sbtjar=sbt-launch.jar

if [ ! -f $sbtjar ]; then
  echo 'downloading '$sbtjar 1>&2
  curl -O http://repo.typesafe.com/typesafe/ivy-releases/org.scala-tools.sbt/sbt-launch/0.11.2/$sbtjar
fi

test -f $sbtjar || exit 1

get_md5() {
  local input_file=$1
  local openssl_vers=$(openssl version|awk '{print $2}')

  case "${openssl_vers}" in
    0.9.*)
      openssl md5 < $input_file
    ;;
    1.0.*)
      openssl md5 < $input_file|awk '{print $2}'
    ;;
    *)
      echo "unrecognized version of openssl" >&2
      exit 42
    ;;
  esac
}


if [ $(get_md5 $sbtjar) != 2886cc391e38fa233b3e6c0ec9adfa1e ]; then
  echo 'bad sbtjar!' 1>&2
  exit 1
fi

test -f ~/.sbtconfig && . ~/.sbtconfig

java -ea                          \
  $SBT_OPTS                       \
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
