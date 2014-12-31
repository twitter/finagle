package com.twitter.finagle.thriftmux.javatest;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Test;

import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Service;
import com.twitter.finagle.ThriftMux;
import com.twitter.finagle.ThriftMuxClient;
import com.twitter.finagle.ThriftMuxServer;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.finagle.thriftmux.thriftscala.TestService;
import com.twitter.finagle.thriftmux.thriftscala.TestService$FinagleService;
import com.twitter.util.Future;

import static junit.framework.Assert.assertEquals;

public class EndToEndTest {

  /**
   * Tests interfaces.
   */
  @Test
  public void testInterfaces() {
    ListeningServer server =
      ThriftMux.server().serveIface("localhost:*", new TestService.FutureIface() {
        public Future<String> query(String x) {
          return Future.value(x + x);
        }
      });

    TestService.FutureIface client =
      ThriftMux.client().newIface(server, TestService.FutureIface.class);
    assertEquals(client.query("ok").get(), "okok");
  }

  /**
   * Tests builders.
   */
  @Test
  public void testBuilders() {
    InetSocketAddress addr = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);

    TestService.FutureIface iface = new TestService.FutureIface() {
      public Future<String> query(String x) {
        return Future.value(x + x);
      }
    };

    Service<byte[], byte[]> service = new TestService$FinagleService(
      iface,
      new TBinaryProtocol.Factory()
    );

   ServerBuilder.safeBuild(
      service,
      ServerBuilder.get()
        .name("java-test-server")
        .bindTo(addr)
        .stack(ThriftMuxServer.get())
    );

   ClientBuilder.safeBuild(
     ClientBuilder.get()
       .name("java-test-client")
       .hosts(addr)
       .stack(ThriftMuxClient.withClientId(new ClientId("java-test-client")))
   );
  }
}
