package com.twitter.finagle.thriftmux.javatest;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Test;

import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Service;
import com.twitter.finagle.Stack;
import com.twitter.finagle.ThriftMux;
import com.twitter.finagle.ThriftMuxClient;
import com.twitter.finagle.ThriftMuxServer;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.param.Label;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.finagle.thriftmux.thriftscala.TestService;
import com.twitter.finagle.thriftmux.thriftscala.TestService$FinagleService;
import com.twitter.util.Await;
import com.twitter.util.Future;

import static junit.framework.Assert.assertEquals;

public class EndToEndTest {

  /**
   * Tests interfaces.
   */
  @Test
  public void testInterfaces() throws Exception {
    ListeningServer server =
      ThriftMux.server().serveIface("localhost:*", new TestService.FutureIface() {
        public Future<String> query(String x) {
          return Future.value(x + x);
        }
      });

    TestService.FutureIface client =
      ThriftMux.client().newIface(server, TestService.FutureIface.class);
    assertEquals(Await.result(client.query("ok")), "okok");
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

    ServerBuilder.get().stack(ThriftMux.server());

    ThriftMux.Server withParams = ThriftMux.server()
      .withParams(Stack.Params$.MODULE$.empty());
    ServerBuilder.get().stack(withParams);

    ThriftMux.Server configured = ThriftMux.server()
      .configured(new Label("hi").mk());
    ServerBuilder.get().stack(configured);

    ThriftMux.Server withParamsAndConfigured = ThriftMux.server()
      .withParams(Stack.Params$.MODULE$.empty())
      .configured(new Label("hi").mk());
    ServerBuilder.get().stack(withParamsAndConfigured);

    ClientBuilder.safeBuild(
      ClientBuilder.get()
        .name("java-test-client")
        .hosts(addr)
        .stack(ThriftMuxClient.withClientId(new ClientId("java-test-client")))
    );

    ClientBuilder.get()
      .stack(ThriftMux.client());
  }
}
