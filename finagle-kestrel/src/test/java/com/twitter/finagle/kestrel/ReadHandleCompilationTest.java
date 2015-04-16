package com.twitter.finagle.kestrel;

import java.util.Arrays;

import scala.runtime.BoxedUnit;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Assert;
import org.junit.Test;

import com.twitter.concurrent.Broker;
import com.twitter.concurrent.Offer;

public class ReadHandleCompilationTest {
  static final Broker<ReadMessage> MESSAGES = new Broker<ReadMessage>();
  static final Broker<Throwable> ERROR = new Broker<Throwable>();
  static final Broker<BoxedUnit> CLOSER = new Broker<BoxedUnit>();

  /**
   * Tests read message.
   */
  @Test
  public void testReadMessage() {
    ChannelBuffer buffer = ChannelBuffers.wrappedBuffer("abc".getBytes());
    Broker<BoxedUnit> ack = new Broker<BoxedUnit>();
    Broker<BoxedUnit> abort = new Broker<BoxedUnit>();
    ReadMessage message =
      new ReadMessage(buffer, ack.send(BoxedUnit.UNIT), abort.send(BoxedUnit.UNIT));

    Assert.assertNotNull(message);
  }

  public static class OwnReadHandle extends ReadHandle {
    @Override
    public Offer<Throwable> error() {
      return ERROR.recv();
    }

    @Override
    public void close() {
      // do nothing
    }

    @Override
    public Offer<ReadMessage> messages() {
      return MESSAGES.recv();
    }

    public int ten() {
      return 10;
    }
  }

  @Test
  public void testOwnReadHandleImplementation() {
    OwnReadHandle own = new OwnReadHandle();
    Assert.assertEquals(10, own.ten());
  }

  @Test
  public void testReadHandleConstructor() {
    ReadHandle handle = ReadHandle.fromOffers(MESSAGES.recv(), ERROR.recv(), CLOSER.recv());
    Assert.assertNotNull(handle);
  }

  /**
   * Tests {@code ReadHandle} merging.
   */
  @Test
  public void testReadHandleMerge() {
    ReadHandle a = ReadHandle.fromOffers(MESSAGES.recv(), ERROR.recv(), CLOSER.recv());
    ReadHandle b = ReadHandle.fromOffers(MESSAGES.recv(), ERROR.recv(), CLOSER.recv());
    ReadHandle c = ReadHandle.merged(Arrays.asList(a, b).iterator());

    Assert.assertNotNull(c);
  }
}
