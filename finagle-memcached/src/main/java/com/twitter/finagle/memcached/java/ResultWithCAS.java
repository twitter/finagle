package com.twitter.finagle.memcached.java;

import org.jboss.netty.buffer.ChannelBuffer;

class ResultWithCAS {
  ChannelBuffer value;
  ChannelBuffer casUnique;
}