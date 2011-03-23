package com.twitter.finagle.tracing

/**
 * Endpoints describe a TCP endpoint that terminates RPC
 * communication.
 */

case class Endpoint(ipv4: Int, port: Short)
