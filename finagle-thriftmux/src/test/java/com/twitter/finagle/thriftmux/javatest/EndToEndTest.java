package com.twitter.finagle.thriftmux.javatest;

import org.junit.Test;

import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.thriftmux.thriftscala.TestService;
import com.twitter.finagle.ThriftMux;
import com.twitter.finagle.ThriftMuxServer;
import com.twitter.finagle.ThriftMuxClient;
import com.twitter.util.Future;

import static junit.framework.Assert.assertEquals;

public class EndToEndTest {
  @Test
  public void test() {
    ListeningServer server = ThriftMux.serveIface(":*", new TestService.FutureIface() {
      public Future<String> query(String x) {
        return Future.value(x+x);
      }
    });

    TestService.FutureIface client = ThriftMux.newIface(server, TestService.FutureIface.class);
    assertEquals(client.query("ok").get(), "okok");
  }

  @Test
  public void testInterfaces() {
    // ensure we are java friendly
    ListeningServer server = ThriftMuxServer.serveIface(":*", new TestService.FutureIface() {
      public Future<String> query(String x) {
        return Future.value(x+x);
      }
    });

    TestService.FutureIface client = ThriftMuxClient.newIface(server, TestService.FutureIface.class);
    assertEquals(client.query("ok").get(), "okok");
  }
}
