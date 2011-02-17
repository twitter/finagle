package com.twitter.finagle.thrift

/**
 * Defines a (framed) thrift request, simply composed of the raw
 * message & a boolean indicating whether it is a one-shot message or
 * not.
 */
case class ThriftClientRequest(message: Array[Byte], oneway: Boolean)
