package com.twitter.finagle.thriftmux.javatest;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import scala.runtime.AbstractFunction1;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.twitter.finagle.Addresses;
import com.twitter.finagle.FailedFastException;
import com.twitter.finagle.Filter;
import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Names;
import com.twitter.finagle.Service;
import com.twitter.finagle.SimpleFilter;
import com.twitter.finagle.Stack;
import com.twitter.finagle.ThriftMux;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.mux.Request;
import com.twitter.finagle.mux.Response;
import com.twitter.finagle.param.Label;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.finagle.thrift.MethodMetadata;
import com.twitter.finagle.thrift.RichServerParam;
import com.twitter.finagle.thriftmux.thriftscala.TestService;
import com.twitter.finagle.thriftmux.thriftscala.TestService$FinagleService;
import com.twitter.util.Await;
import com.twitter.util.Closable;
import com.twitter.util.Closables;
import com.twitter.util.Duration;
import com.twitter.util.Future;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class EndToEndTest {

  public static class TestServiceImpl implements TestService.MethodPerEndpoint {
    @Override
    public Future<String> query(String x) {
      assertTrue(MethodMetadata.current().exists(
          new AbstractFunction1<MethodMetadata, Object>() {
            @Override
            public Object apply(MethodMetadata v1) {
              return "query".equals(v1.methodName());
            }
          }
        )
      );
      return Future.value(x + x);
    }
    @Override
    public Future<String> question(String y) {
      assertTrue(MethodMetadata.current().exists(
          new AbstractFunction1<MethodMetadata, Object>() {
            @Override
            public Object apply(MethodMetadata v1) {
              return "question".equals(v1.methodName());
            }
          }
          )
      );
      return Future.value(y + y);
    }
    @Override
    public Future<String> inquiry(String z) {
      assertTrue(MethodMetadata.current().exists(
          new AbstractFunction1<MethodMetadata, Object>() {
            @Override
            public Object apply(MethodMetadata v1) {
              return "inquiry".equals(v1.methodName());
            }
          }
          )
      );
      return Future.value(z + z);
    }

    @Override
    public Closable asClosable() {
      return Closables.NOP;
    }
  }

  public static class TestJavaServiceImpl
      implements com.twitter.finagle.thriftmux.thriftjava.TestService.ServiceIface {
    @Override
    public Future<String> query(String x) {
      assertTrue(MethodMetadata.current().exists(
          new AbstractFunction1<MethodMetadata, Object>() {
            @Override
            public Object apply(MethodMetadata v1) {
              return "query".equals(v1.methodName());
            }
          }
          )
      );
      return Future.value(x + x);
    }
    @Override
    public Future<String> question(String y) {
      assertTrue(MethodMetadata.current().exists(
          new AbstractFunction1<MethodMetadata, Object>() {
            @Override
            public Object apply(MethodMetadata v1) {
              return "question".equals(v1.methodName());
            }
          }
          )
      );
      return Future.value(y + y);
    }
    @Override
    public Future<String> inquiry(String z) {
      assertTrue(MethodMetadata.current().exists(
          new AbstractFunction1<MethodMetadata, Object>() {
            @Override
            public Object apply(MethodMetadata v1) {
              return "inquiry".equals(v1.methodName());
            }
          }
          )
      );
      return Future.value(z + z);
    }
  }

  /**
   * Tests interfaces.
   */
  @Test
  public void testInterfaces() throws Exception {
    ListeningServer server =
        ThriftMux.server().serveIface("localhost:*", new TestServiceImpl());

    TestService.FutureIface client =
        ThriftMux.client().newIface(
            Names.bound(Addresses.newInetAddress((InetSocketAddress) server.boundAddress())),
            "a_client",
            TestService.FutureIface.class);
    try {
      assertEquals(Await.result(client.query("ok")), "okok");
    } finally {
      Await.result(client.asClosable().close(), Duration.fromSeconds(2));
      Await.result(server.close(), Duration.fromSeconds(2));
    }
  }

  /**
   * Tests Java interfaces.
   */
  @Test
  public void testJavaInterfaces() throws Exception {
    ListeningServer server =
        ThriftMux.server().serve(
            "localhost:*",
            new com.twitter.finagle.thriftmux.thriftjava.TestService.Service(
                new TestJavaServiceImpl()));

    com.twitter.finagle.thriftmux.thriftjava.TestService.ServiceIface client =
        ThriftMux.client().newIface(
            Names.bound(Addresses.newInetAddress((InetSocketAddress) server.boundAddress())),
            "a_client",
            com.twitter.finagle.thriftmux.thriftjava.TestService.ServiceIface.class);
    try {
      assertEquals(Await.result(client.query("ok")), "okok");
    } finally {
      Await.result(server.close(), Duration.fromSeconds(2));
    }
  }

  /**
   * Tests builders.
   */
  @Test
  public void testBuilders() {
    InetSocketAddress addr = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);

    TestService.MethodPerEndpoint iface = new TestServiceImpl();

    RichServerParam serverParam = new RichServerParam(new TBinaryProtocol.Factory());

    Service<byte[], byte[]> service = new TestService$FinagleService(iface, serverParam);

    ServerBuilder.safeBuild(
      service,
      ServerBuilder.get()
        .name("java-test-server")
        .bindTo(addr)
        .stack(ThriftMux.server())
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
        .stack(ThriftMux.client().withClientId(new ClientId("java-test-client")))
    );

    ClientBuilder.get()
      .stack(ThriftMux.client());
  }

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  /**
   * Tests client with filtered
   */
  @Test
  public void testFilteredClient() throws Exception {
    Filter<Request, Response, Request, Response> filter
        = new SimpleFilter<Request, Response>() {
      @Override
      public Future<Response> apply(Request request, Service<Request, Response> service) {
        return Future.exception(new FailedFastException("client unhappy"));
      }
    };

    InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    ListeningServer server =
      ThriftMux.server().serveIface(address, new TestServiceImpl());

    TestService.FutureIface client =
        ThriftMux.client().filtered(filter).newIface(
            Names.bound(Addresses.newInetAddress((InetSocketAddress) server.boundAddress())),
            "a_client",
            TestService.FutureIface.class);
    try {
      expectedEx.expectMessage("client unhappy");
      Await.result(client.query("hi"));
    } finally {
      Await.result(client.asClosable().close(), Duration.fromSeconds(2));
      Await.result(server.close(), Duration.fromSeconds(2));
    }
  }

  /**
   * Tests client with filtered over Java service
   */
  @Test
  public void testFilteredClientWithJavaServer() throws Exception {
    Filter<Request, Response, Request, Response> filter
        = new SimpleFilter<Request, Response>() {
      @Override
      public Future<Response> apply(Request request, Service<Request, Response> service) {
        return Future.exception(new FailedFastException("client unhappy"));
      }
    };

    InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    ListeningServer server =
        ThriftMux.server().serveIface(address, new TestServiceImpl());

    TestService.FutureIface scalaClient =
        ThriftMux.client().filtered(filter).newIface(
            Names.bound(Addresses.newInetAddress((InetSocketAddress) server.boundAddress())),
            "a_client",
            TestService.FutureIface.class);

    try {
      expectedEx.expectMessage("client unhappy");
      Await.result(scalaClient.query("hi"));

      com.twitter.finagle.thriftmux.thriftjava.TestService.ServiceIface javaClient =
          ThriftMux.client().newIface(
              Names.bound(Addresses.newInetAddress((InetSocketAddress) server.boundAddress())),
              "a_client",
              com.twitter.finagle.thriftmux.thriftjava.TestService.ServiceIface.class);

      expectedEx.expectMessage("client unhappy");
      Await.result(javaClient.query("hi"));
    } finally {
      Await.result(scalaClient.asClosable().close(), Duration.fromSeconds(2));
      Await.result(server.close(), Duration.fromSeconds(2));
    }
  }

  /**
   * Tests server with filtered
   */
  @Test
  public void testFilteredServer() throws Exception {
    Filter<Request, Response, Request, Response> filter
        = new SimpleFilter<Request, Response>() {
      @Override
      public Future<Response> apply(Request request, Service<Request, Response> service) {
        return Future.exception(new FailedFastException("server unhappy"));
      }
    };

    InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    ListeningServer server =
      ThriftMux.server()
        .filtered(filter)
        .serveIface(address, new TestServiceImpl());

    TestService.FutureIface client =
        ThriftMux.client().newIface(
            Names.bound(Addresses.newInetAddress((InetSocketAddress) server.boundAddress())),
            "a_client",
            TestService.FutureIface.class);
    try {
      expectedEx.expectMessage("server unhappy");
      Await.result(client.query("hi"));
    } finally {
      Await.result(client.asClosable().close(), Duration.fromSeconds(2));
      Await.result(server.close(), Duration.fromSeconds(2));
    }
  }

  /**
   * Tests Java server with filtered
   */
  @Test
  public void testFilteredJavaServer() throws Exception {
    Filter<Request, Response, Request, Response> filter
        = new SimpleFilter<Request, Response>() {
      @Override
      public Future<Response> apply(Request request, Service<Request, Response> service) {
        return Future.exception(new FailedFastException("server unhappy"));
      }
    };

    InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    ListeningServer server =
        ThriftMux.server()
            .filtered(filter)
            .serve(
                address,
                new com.twitter.finagle.thriftmux.thriftjava.TestService.Service(
                    new TestJavaServiceImpl()));

    TestService.FutureIface client =
        ThriftMux.client().newIface(
            Names.bound(Addresses.newInetAddress((InetSocketAddress) server.boundAddress())),
            "a_client",
            TestService.FutureIface.class);
    try {
      expectedEx.expectMessage("server unhappy");
      Await.result(client.query("hi"));
    } finally {
      Await.result(client.asClosable().close(), Duration.fromSeconds(2));
      Await.result(server.close(), Duration.fromSeconds(2));
    }
  }
}
