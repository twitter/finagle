package com.twitter.finagle.thrift;

import com.twitter.finagle.stats.DefaultStatsReceiver;

import org.junit.Assert;
import org.junit.Test;

import scala.Option;

public class ThriftClientCodecTest {

  @Test
  public void testCodecFactoryCreation() {
    // ThriftClientBufferedCodec
    Assert.assertTrue(ThriftClientBufferedCodec.get() instanceof ThriftClientBufferedCodecFactory);
    Assert.assertTrue(
        ThriftClientBufferedCodec.apply() instanceof ThriftClientBufferedCodecFactory);
    Assert.assertTrue(
        ThriftClientBufferedCodec.apply(Protocols.factory(DefaultStatsReceiver.self()))
            instanceof ThriftClientBufferedCodecFactory);

    //ThriftClientFramedCodec
    Assert.assertTrue(ThriftClientFramedCodec.get() instanceof ThriftClientFramedCodecFactory);
    Assert.assertTrue(
        ThriftClientFramedCodec
            .apply(Option.<ClientId>empty()) instanceof ThriftClientFramedCodecFactory);
  }
}
