#!/bin/bash

HERE=`dirname $0`

u=`echo $HERE | cksum | awk '{print $1}'`
a=`expr $u % 5678`
port=`expr 1024 + $a`
dport=`expr $port + 1`

echo "(sbt jmx @ $port, debug @ $dport; pwd=$PWD)"

function find_sbt_root {
  while [ ! -d project -a "x$PWD" != "x/" ] ; do
    cd ..
  done

  if [ "x$PWD" = "/" ]; then
    echo "couldn't find sbt project!" 1>&2
    exit 1
  fi
}

find_sbt_root

if [ -f 'sbtlocal.sh' ]; then
    source sbtlocal.sh
fi

if [ "$PROFILE" = "1" ]; then
    JAVA_OPTS="-agentlib:yjpagent $JAVA_OPTS"
fi

exec java \
    -ea\
    -Xdebug -Xrunjdwp:transport=dt_socket,address=$dport,server=y,suspend=n \
    -Djava.library.path="$JAVA_LIB_PATH" \
    -Djava.net.preferIPv4Stack=true \
    -Dhttp.connection.timeout=2 \
    -Dhttp.connection-manager.timeout=2 \
    -Dhttp.socket.timeout=6 \
    -Dcom.sun.management.jmxremote.ssl=false \
    -Dcom.sun.management.jmxremote.authenticate=false \
    -Dcom.sun.management.jmxremote.port=$port \
    -Dcom.sun.management.jmxremote \
    -XX:+AggressiveOpts \
    -XX:+UseParNewGC \
    -XX:+UseConcMarkSweepGC \
    -XX:+CMSParallelRemarkEnabled \
    -XX:+CMSClassUnloadingEnabled \
    -XX:MaxPermSize=256m \
    -XX:SurvivorRatio=128 \
    -XX:MaxTenuringThreshold=0 \
    -Xss80M \
    -Xms80M \
    -Xmx400M \
    -server \
    -jar "$HERE/sbt-launch-0.7.4.jar" "$@"
