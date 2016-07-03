package com.twitter.finagle.thrift;

import org.junit.Assert;
import org.junit.Test;

import scala.Option;

public class ThriftClientCodecTest {

  @Test
  public void testCodecFactoryCreation() {
    //ThriftClientFramedCodec
    Assert.assertTrue(ThriftClientFramedCodec.get() instanceof ThriftClientFramedCodecFactory);
    Assert.assertTrue(
        ThriftClientFramedCodec
            .apply(Option.<ClientId>empty()) instanceof ThriftClientFramedCodecFactory);
  }
}
