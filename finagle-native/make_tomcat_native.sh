#!/bin/sh

VER="1.1.22"
REL="tomcat-native-$VER"
URL="http://www.apache.org/dist/tomcat/tomcat-connectors/native/$VER/source/$REL-src.tar.gz"
TARBALL="$REL-src.tar.gz"

here=`dirname $0`

function die {
    echo $@ 1>&2
    exit 1
}

test -d $REL && die "directory '$REL' already exists. remove it to continue."


test -f $TARBALL || \
    curl -O $URL || \
    die "could not download tarball '$TARBALL' from '$URL'"

tar zxf $TARBALL
cd $REL
patch < $here/tomcat-native-$VER.finagle.patch || die "patch did not apply"
cd -
