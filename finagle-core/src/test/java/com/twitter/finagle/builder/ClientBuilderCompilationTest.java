package com.twitter.finagle.builder;

import org.jboss.netty.channel.ChannelPipelineFactory;
import org.junit.Test;

import com.twitter.finagle.AbstractCodec;
import com.twitter.finagle.ClientCodecConfig;
import com.twitter.finagle.Codec;
import com.twitter.finagle.ServiceFactory;
import com.twitter.util.Function;

public class ClientBuilderCompilationTest {

  @Test
  public void testStackClientOfCodec() {
    final Codec<String, String> codec = new AbstractCodec<String, String>() {
      public ChannelPipelineFactory pipelineFactory() {
        return null;
      }
    };
    Function<ClientCodecConfig, Codec<String, String>> clientCodecFactory =
      new Function<ClientCodecConfig, Codec<String, String>>() {
        public Codec<String, String> apply(ClientCodecConfig config) {
          return codec;
        }
      };
    ServiceFactory<String, String> client =
      ClientBuilder.stackClientOfCodec(clientCodecFactory)
        .newClient("/s/foo/bar");
  }
}
