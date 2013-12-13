#!/bin/bash

VER="1.1.27"
REL="tomcat-native-$VER"
SRC="$REL-src"
TARBALL="$SRC.tar.gz"
URL="http://archive.apache.org/dist/tomcat/tomcat-connectors/native/$VER/source/$TARBALL"

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
patch -p1 < ../tomcat-native-$VER.finagle.patch || die "patch did not apply"
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
echo "For OpenSSL 1.0.0g, an adaptation of Google's NPN patch is available at:"
echo "   https://gist.github.com/1772441"
echo ""
echo "To use this patch, 'patch -p1 < openssl-1.0.0g-npn.patch' in the OpenSSL 1.0.0g tree."
echo "-------------------------------------------------------------------------------------"
echo
