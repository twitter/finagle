package com.twitter.finagle.redis;

import org.jboss.netty.channel.ChannelPipelineFactory;

import com.twitter.finagle.ClientCodecConfig;
import com.twitter.finagle.Codec;
import com.twitter.finagle.ServerCodecConfig;
import com.twitter.finagle.redis.protocol.Command;
import com.twitter.finagle.redis.protocol.Reply;
import com.twitter.util.Function;

/**
 * A compilation test for extending the Redis codec in java.
 */

public class TestCodec extends Redis {

  public Function<ClientCodecConfig, Codec<Command, Reply>> client() {
    return null;
  }

  public Function<ServerCodecConfig, Codec<Command, Reply>> server() {
    return null;
  }

  public ChannelPipelineFactory pipelineFactory() {
    return null;
  }
}
