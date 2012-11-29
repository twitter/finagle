#!/bin/sh

cd sslCA

# now make the intermediate
if [ ! -d ca2012 ]; then
  mkdir ca2012
fi

cp ../openssl-intermediate.conf ca2012/openssl.conf
cd ca2012

for d in certs private newcerts; do
  if [ ! -d $d ]; then
     mkdir $d
  fi
done

if [ ! -f serial ]; then
  echo 1000 > serial
fi

touch index.txt

# request and sign the request
echo "=== generate intermediate key ==="
openssl genrsa -des3 -out private/cakey.pem 2048
openssl req -new -sha1 -key private/cakey.pem -out ca2012.csr -config openssl.conf

echo "=== sign intermediate key ==="
mv ca2012.csr ..
cd ..
openssl ca -extensions v3_ca -days 1825 -out ca2012.crt -in ca2012.csr -config openssl.conf

# move all the keys back to the ca2012 dir, and we're done.
mv ca2012.csr ca2012.crt ca2012
