#!/bin/sh

# have to do this or req fails miserably
export OPENSSL_CONF=`pwd`/openssl.conf

if [ "$1" == "" ]; then
  echo "usage: $0 fqdn.goes.here.com san1,san2,san3 ..."
  echo 
  exit 127
fi

FQDN=$1
CONF=`pwd`/openssl.conf
SANS=$2

if [ "${SANS}" != "" ]; then
  # we need to do the work to generate SANS, which means a new 
  # config file
  cp openssl.conf-san openssl.conf-san-${FQDN}
  export OPENSSL_CONF=`pwd`/openssl.conf-san-${FQDN}

  IFS=,
  N=1

  # always include the first one as the 1st SAN for safety 
  echo "DNS.${N}=${FQDN}" >> $OPENSSL_CONF

  for s in $SANS; 
  do
    N=$(( $N + 1 ))
    echo "DNS.${N}=${s}" >> $OPENSSL_CONF     
  done
  IFS=" "

fi

if [ -f private/${FQDN}.key ]; then 
  echo "${FQDN}.key exists. Remove it before re-generating."
  exit 1
fi

echo
echo "=== genkey ==="
echo
openssl genrsa -out private/${FQDN}.key 2048

echo
echo "=== request csr ==="
echo

# create request
echo "




${FQDN}




" | openssl req -new -key private/${FQDN}.key -out ${FQDN}.csr 

echo 
echo "=== sign ==="
echo

# sign the request with the Intermediate CA
openssl ca -batch -config ${CONF} -policy policy_anything \
    -out ${FQDN}.cert -infiles ${FQDN}.csr 

# and store the server files in the certs/ directory<br />
mv ${FQDN}.csr ${FQDN}.cert certs/

# remove our temporary config file 
if [ "${SANS}" != "" ]; then
  rm openssl.conf-san-${FQDN}
fi
