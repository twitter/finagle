#!/bin/bash -x

# arguments assume that these three files are in the current dir
# basename allows them to be passed in with absolute paths though
MAKECERT_SH=`basename $1`
OPENSSL_INTERMEDIATE_CONF=`basename $2`
OPENSSL_ROOT_CONF=`basename $3`
echo "=== config files ==="
echo "makecert: $MAKECERT_SH"
echo "openssl_intermediate: $OPENSSL_INTERMEDIATE_CONF"
echo "openssl_root: $OPENSSL_ROOT_CONF"
echo "===================="

# does not work properly with an existing sslCA
if [ -d sslCA ]; then
  rm -rf sslCA
fi

# Build the Root CA
if [ ! -d sslCA ]; then 
  mkdir ./sslCA
fi

for d in certs private newcerts; do
  if [ ! -d ./sslCA/$d ]; then
     mkdir ./sslCA/$d
     cp $OPENSSL_ROOT_CONF ./sslCA/openssl.conf
  fi
done

# update serial
cd sslCA
if [ ! -f serial ]; then
  echo 1000 > serial
fi

touch index.txt
echo "unique_subject = yes" > index.txt.attr

echo
echo "=== generate CA key ==="
echo

if [ ! -f private/cacert.key ]; then 
  openssl genrsa -out private/cacert.key 2048
fi

if [ ! -s private/cacert.key ]; then 
    echo "Exit: Key creation failed."
    exit 1 
fi

# use the key to sign itself
echo
echo "=== self-sign the root ==="
echo
if [ ! -f cacert.pem ]; then 
  openssl req -new -x509 -days 3650 -extensions v3_ca -sha1 -nodes \
      -key private/cacert.key -out cacert.pem \
      -config openssl.conf \
      -batch
fi

# --- setup the intermediate CA --- --- --- --- --- --- --- --- --- 

if [ ! -d intermediate_ca ]; then
  mkdir intermediate_ca
  touch intermediate_ca/index.txt
fi

cp "../$OPENSSL_INTERMEDIATE_CONF" intermediate_ca/openssl.conf

cd intermediate_ca

for d in certs private newcerts; do
  if [ ! -d $d ]; then
     mkdir $d
  fi
done

if [ ! -f serial ]; then
  echo 1000 > serial
fi

# request and sign the request
echo
echo "=== generate intermediate key ==="
echo
openssl genrsa -out private/int_cakey.pem 2048

if [ ! -f private/int_cakey.pem ]; then
  echo "failed to generate int_cakey.pem for intermediate"
  exit
fi

if [ ! -f openssl.conf ]; then
  echo "where is openssl.conf?"
  exit
fi

openssl req -new -sha1 -key private/int_cakey.pem -out intermediate_ca.csr -config openssl.conf -batch

if [ ! -f intermediate_ca.csr ]; then
  echo "failed to generate CSR for intermediate"
  exit
fi

echo
echo "=== sign intermediate key with the CA ==="
echo
mv intermediate_ca.csr ..

cd ..
openssl ca -extensions v3_ca -days 1825 -out intermediate_ca.crt -in intermediate_ca.csr -config openssl.conf -batch

if [ ! -s intermediate_ca.crt ]; then
  echo "failed to sign CSR for intermediate"
  exit
fi

# move all the keys back to the intermediate_ca dir, and we're done.
mv intermediate_ca.csr intermediate_ca.crt intermediate_ca

# make the root/intermediate chain 
cd intermediate_ca 

cat intermediate_ca.crt > chain.cert

# ... then root CA
cat ../cacert.pem >> chain.cert

# -- finally, make a certificate --
cp ../../$MAKECERT_SH .
./$MAKECERT_SH test.example.com


# construct a final, final cert chain.
# concatencate the SSL certs to make the chain
cd ../..

if [ ! -f sslCA/intermediate_ca/newcerts/1000.pem ]; then
  echo "1000.pem missing"
  exit
fi

if [ ! -f sslCA/intermediate_ca/intermediate_ca.crt ]; then
  echo "intermediate_ca.crt missing"
  exit
fi

# first the leaf 
cat sslCA/intermediate_ca/newcerts/1000.pem  > test.example.com.chain
# then the intermediate 
cat sslCA/intermediate_ca/intermediate_ca.crt >>  test.example.com.chain
# then root CA
cat sslCA/cacert.pem >>  test.example.com.chain

if [ ! -f test.example.com.chain ]; then
  echo "test.example.com.chain file missing"
  exit
fi

# copy files back to root
cp sslCA/intermediate_ca/certs/test.example.com.cert .
cp sslCA/intermediate_ca/private/test.example.com.key .
cp sslCA/cacert.pem .

# Destroy the CA.
# note if sslCA is already present when run, you'll see errors
rm -rf sslCA

echo
echo "=== chain generation complete ==="
echo
