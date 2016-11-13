package com.twitter.finagle.thrift;

/**
 * Defines a (framed) thrift request, simply composed of the raw
 * message and a boolean indicating whether it is a one-shot message or
 * not.
 */

public class ThriftClientRequest {
  public byte[] message;
  public boolean oneway;

  public ThriftClientRequest(byte[] message, boolean oneway) {
    this.message = message;
    this.oneway = oneway;
  }
}
