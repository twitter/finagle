This is an experimental library, use at your own risk!

## finagle-http2

finagle-http2 provides support for the http2 protocol in finagle

## design principles

The plan for milestone 0 is to provide the same API we have for http/1.1, but
also add some of http2's reliability features on top.  This means that we'll be
making requests and responses use finagle-http Request and Response types.  We
intend to use the netty4 implementation of http2 to power our implementation.

## non-goals for M0

### first class streaming

We intend to support streaming as a first class primitive in finagle-http2, but
it's not a priority for us, and will require changing finagle fundamentally to
focus more clearly on session management, where RPC is a special case of session
management, and streaming is a different special case.  For milestone 0, streams
will be fields on requests and responses, the same way they are for http/1.1.

## current state

This library is in a very experimental state (only supports a Transporter, and
only for cleartext), and may churn significantly in the coming months.  Please
let us know if you would like to help work on it!
