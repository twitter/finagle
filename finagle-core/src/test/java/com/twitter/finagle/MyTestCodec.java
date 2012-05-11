package com.twitter.finagle;

import org.jboss.netty.channel.ChannelPipelineFactory;

/**
 * A compilation test for defining codecs in java.
 */

public class MyTestCodec extends AbstractCodec<String, String> {
  // This is the only necessary definition.
  public ChannelPipelineFactory pipelineFactory() {
    return null;
  }
}
