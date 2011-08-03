package com.twitter.finagle.memcached.java;

import org.jboss.netty.buffer.ChannelBuffer;

public class ResultWithCAS {
  public final ChannelBuffer value;
  public final ChannelBuffer casUnique;

  public ResultWithCAS(ChannelBuffer value, ChannelBuffer casUnique) {
    if (value == null) throw new NullPointerException("value cannot be null");
    if (casUnique == null) throw new NullPointerException("casUnique cannot be null");

    this.value = value;
    this.casUnique = casUnique;
  }
}
