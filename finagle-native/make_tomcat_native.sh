#!/bin/bash

VER="1.1.22"
REL="tomcat-native-$VER"
URL="http://www.apache.org/dist/tomcat/tomcat-connectors/native/$VER/source/$REL-src.tar.gz"
TARBALL="$REL-src.tar.gz"
SRC="$REL-src"

here=`dirname $0`

function die {
    echo $@ 1>&2
    exit 1
}

test -d $REL && die "directory '$REL' already exists. remove it to continue."


test -f $TARBALL || \
    (echo "Fetching $REL"; curl -# -O $URL) || \
    die "could not download tarball '$TARBALL' from '$URL'"

tar zxf $TARBALL
cd $SRC > /dev/null
git apply < ../tomcat-native-$VER.finagle.patch || die "patch did not apply"
cd - > /dev/null
rm $TARBALL

echo
echo "-------------------------------------------------------------------------------------"
echo "$REL in $SRC has been patched to support Finagle."
echo ""
echo "You should follow the instructions at http://tomcat.apache.org/native-doc/ to build."
echo ""
echo "You will need these:"
echo "  APR:     http://apr.apache.org/"
echo "  OpenSSL: http://openssl.org/"
echo ""
echo "You need OpenSSL 1.0.1 or greater; or 1.0.0g with an SPDY NPN patch applied."
echo "For OpenSSL 1.0.0g, an NPN patch is included in this directory for convenience."
echo "To use the patch, 'git apply < patchfile' it in the 1.0.0g source tree and build."
echo "-------------------------------------------------------------------------------------"
echo
