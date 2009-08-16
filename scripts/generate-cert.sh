#!/bin/bash
if [ ! -x /usr/bin/certtool ] ; then
    echo "certtool not found! is gnutls installed?"
    exit 1
fi

certtool --generate-privkey --outfile ca-key.pem
certtool --generate-self-signed --load-privkey ca-key.pem --outfile ca-cert.pem