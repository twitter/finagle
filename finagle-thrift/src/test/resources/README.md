From http://stackoverflow.com/questions/10175812/how-to-create-a-self-signed-certificate-with-openssl ..

`openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days 999 -nodes`

.. which means that the certificate used in the `com.twitter.finagle.thrift.EndToEndTest` `"Configuring SSL over stack param"`
 test-case will be invalid from May 3, 2018.
