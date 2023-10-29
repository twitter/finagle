#!/usr/bin/env bash

print_env () {
  echo "FINAGLE_USE_NETTY_4_SNAPSHOT=$FINAGLE_USE_NETTY_4_SNAPSHOT"
  echo "FINAGLE_NETTY_4_VERSION=$FINAGLE_NETTY_4_VERSION"
  echo "FINAGLE_NETTY_4_TCNATIVE_VERSION=$FINAGLE_NETTY_4_TCNATIVE_VERSION"
}

unset_env() {
  unset FINAGLE_NETTY_4_VERSION
  unset FINAGLE_NETTY_4_TCNATIVE_VERSION
  unset FINAGLE_USE_NETTY_4_SNAPSHOT
  print_env
}

if [ $# -ne 1 ]; then
  USAGE_MSG=('usage: source ' $0 ' <true|false>')
  echo "${USAGE_MSG[*]}" 1>&2
  echo '  true            Netty SNAPSHOT version info can be sourced into environment variables for Finagle sbt build'
  echo '  false           Netty SNAPSHOT version info will be unset from environment variables'
  echo -e '\n\nDefault usage is "false", ensuring environment variables are unset:'
  unset_env
  exit 0
fi

USE_SNAPSHOT=$1

if [ "$USE_SNAPSHOT" = "true" ]; then
  echo "Setting Finagle Netty Snapshot environment variables"
  echo "Loading latest Netty info..."
  FILE=netty4-snapshot-pom.xml
  curl -s --connect-timeout 5 --max-time 10 --retry 5 --retry-max-time 60 https://raw.githubusercontent.com/netty/netty/4.1/pom.xml > $FILE
  echo "Parsing Netty version info..."
  FINAGLE_USE_NETTY_4_SNAPSHOT=true
  FINAGLE_NETTY_4_VERSION=$(grep -m 2 "<version>" $FILE | grep SNAPSHOT | awk -F'[<>]' '/version/{print $3}')
  FINAGLE_NETTY_4_TCNATIVE_VERSION=$(grep -m 1 "<tcnative.version>" $FILE | awk -F'[<>]' '/version/{print $3}')
  rm $FILE

  export FINAGLE_USE_NETTY_4_SNAPSHOT
  export FINAGLE_NETTY_4_VERSION
  export FINAGLE_NETTY_4_TCNATIVE_VERSION

  print_env
else
  echo "Using released Netty version. Unsetting Finagle Netty Snapshot environment variables"
  unset_env
fi
