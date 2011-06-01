package com.twitter.finagle.thrift;

import com.twitter.finagle.tracing.Tracer;

import java.net.InetSocketAddress;

/**
 * Defines a (framed) thrift request, simply composed of the raw
 * message & a boolean indicating whether it is a one-shot message or
 * not.
 */

public class ThriftClientRequest {
  public byte[] message;
  public boolean oneway;

  public Tracer tracer;

  public ThriftClientRequest(byte[] message, boolean oneway) {
    this.message = message;
    this.oneway = oneway;
  }
}