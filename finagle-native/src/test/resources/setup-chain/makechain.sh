#!/bin/sh

# concatencate the SSL certs to make the chain
# first the intermediate 
cat sslCA/intermediate_ca/intermediate_ca.crt > chain.cert

# ... then root CA
cat sslCA/cacert.pem >> chain.cert
