package com.twitter.finagle.thriftmux.javatest;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import scala.Some;
import scala.collection.immutable.Map.Map3;
import scala.runtime.AbstractFunction1;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.twitter.io.Bufs;
import com.twitter.finagle.Address;
import com.twitter.finagle.Addresses;
import com.twitter.finagle.FailedFastException;
import com.twitter.finagle.Filter;
import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Names;
import com.twitter.finagle.Service;
import com.twitter.finagle.ServiceFactory;
import com.twitter.finagle.SimpleFilter;
import com.twitter.finagle.ThriftMux;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.mux.Request;
import com.twitter.finagle.mux.Response;
import com.twitter.finagle.mux.Responses;
import com.twitter.finagle.partitioning.zk.ZkMetadata;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.finagle.thrift.MethodMetadata;
import com.twitter.finagle.thrift.RichServerParam;
import com.twitter.finagle.thrift.exp.partitioning.ClientCustomStrategies;
import com.twitter.finagle.thrift.exp.partitioning.ClientHashingStrategy;
import com.twitter.finagle.thrift.exp.partitioning.CustomPartitioningStrategy;
import com.twitter.finagle.thrift.exp.partitioning.PartitioningStrategy;
import com.twitter.finagle.thriftmux.thriftjava.FanoutTestService;
import com.twitter.finagle.thriftmux.thriftjava.TestService;
import com.twitter.hashing.KeyHashers;
import com.twitter.scrooge.ThriftStructIface;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.twitter.util.Return;
import com.twitter.util.Try;

import static org.junit.Assert.assertArrayEquals;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class EndToEndTest {

  private <T> T await(Future<T> f) throws Exception {
    return Await.result(f, Duration.fromSeconds(2));
  }

  public static class TestServiceImpl implements TestService.ServiceIface {
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

    TestService.ServiceIface client =
        ThriftMux.client().newIface(
            Names.bound(Addresses.newInetAddress((InetSocketAddress) server.boundAddress())),
            "a_client",
            TestService.ServiceIface.class);
    try {
      assertEquals(await(client.query("ok")), "okok");
    } finally {
      await(server.close());
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
      assertEquals(await(client.query("ok")), "okok");
    } finally {
      await(server.close());
    }
  }

  /**
   * Tests builders.
   */
  @Test
  public void testBuilders() {
    InetSocketAddress addr = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);

    TestService.ServiceIface iface = new TestServiceImpl();

    RichServerParam serverParam = new RichServerParam(new TBinaryProtocol.Factory());

    Service<byte[], byte[]> service = new TestService.Service(iface, serverParam);

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

    TestService.ServiceIface client =
        ThriftMux.client().filtered(filter).newIface(
            Names.bound(Addresses.newInetAddress((InetSocketAddress) server.boundAddress())),
            "a_client",
            TestService.ServiceIface.class);
    try {
      expectedEx.expectMessage("client unhappy");
      await(client.query("hi"));
    } finally {
      await(server.close());
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

    com.twitter.finagle.thriftmux.thriftscala.TestService.FutureIface scalaClient =
        ThriftMux.client().filtered(filter).newIface(
            Names.bound(Addresses.newInetAddress((InetSocketAddress) server.boundAddress())),
            "a_client",
            com.twitter.finagle.thriftmux.thriftscala.TestService.FutureIface.class);

    try {
      expectedEx.expectMessage("client unhappy");
      await(scalaClient.query("hi"));

      com.twitter.finagle.thriftmux.thriftjava.TestService.ServiceIface javaClient =
          ThriftMux.client().newIface(
              Names.bound(Addresses.newInetAddress((InetSocketAddress) server.boundAddress())),
              "a_client",
              com.twitter.finagle.thriftmux.thriftjava.TestService.ServiceIface.class);

      expectedEx.expectMessage("client unhappy");
      await(javaClient.query("hi"));
    } finally {
      await(scalaClient.asClosable().close());
      await(server.close());
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

    TestService.ServiceIface client =
        ThriftMux.client().newIface(
            Names.bound(Addresses.newInetAddress((InetSocketAddress) server.boundAddress())),
            "a_client",
            TestService.ServiceIface.class);
    try {
      expectedEx.expectMessage("server unhappy");
      await(client.query("hi"));
    } finally {
      await(server.close());
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

    TestService.ServiceIface client =
        ThriftMux.client().newIface(
            Names.bound(Addresses.newInetAddress((InetSocketAddress) server.boundAddress())),
            "a_client",
            TestService.ServiceIface.class);
    try {
      expectedEx.expectMessage("server unhappy");
      await(client.query("hi"));
    } finally {
      await(server.close());
    }
  }

  @Test
  public void testPartitioningParams() {
    PartitioningStrategy myStrat = ClientHashingStrategy.create(com.twitter.util.Function.func(
      (ThriftStructIface iface) -> Collections.emptyMap()));
    ThriftMux.Client client = ThriftMux.client()
      .withPartitioning().strategy(myStrat)
      .withPartitioning().ejectFailedHost(false)
      .withPartitioning().keyHasher(KeyHashers.KETAMA())
      .withPartitioning().numReps(5);
  }

  public static class FanoutTestJavaServiceImpl implements FanoutTestService.ServiceIface {
    private UnaryOperator<String> fn;

    public FanoutTestJavaServiceImpl(UnaryOperator<String> fn) {
      this.fn = fn;
    }

    @Override
    public Future<List<String>> query(List<String> x) {
      List<String> out = new ArrayList<>();
      for (String item : x) {
        out.add(this.fn.apply(item));
      }
      return Future.value(out);
    }
  }

  private static Filter<Request, Response, byte[], byte[]> muxFilter =
    new Filter<Request, Response, byte[], byte[]>() {
      public Future<Response> apply(Request req, Service<byte[], byte[]> svc) {
        return svc.apply(Bufs.ownedByteArray(req.body()))
          .map(com.twitter.util.Function.func(bytes -> Responses.make(Bufs.ownedBuf(bytes))));
      }
    };

  private Address createAddress(int shardId, FanoutTestService.ServiceIface iface) {
    ServiceFactory<Request, Response> factory =
      ServiceFactory.constant(muxFilter.andThen(new FanoutTestService.Service(iface)));
    ZkMetadata zkMetadata = new ZkMetadata(
      new Some<>(shardId),
      scala.collection.immutable.Map$.MODULE$.empty());

    // it's very challenging to get map concat to be consistent across multiple versions of scala
    // from Java-land.
    scala.collection.immutable.Map<String, Object> addrMetadata =
      new Map3<>("hostname", "localhost", "port", 80, "zk_metadata", zkMetadata);

    return new Address.ServiceFactory<>(factory, addrMetadata);
  }

  private BiFunction<
    List<List<String>>,
    List<Throwable>,
    Try<List<String>>
  > flatListResponse = (responses, throwables) -> {
    List<String> out = new ArrayList<>();
    for (List<String> response : responses) {
      out.addAll(response);
    }
    return new Return<>(out);
  };

  private Function<
    List<FanoutTestService.query_args>,
    FanoutTestService.query_args
  > flatListRequest = list -> {
    List<String> out = new ArrayList<>();
    for (FanoutTestService.query_args args : list) {
      out.addAll(args.x);
    }
    return new FanoutTestService.query_args(out);
  };

  private PartitioningStrategy getConsistentHashingStrategy() {
    ClientHashingStrategy strategy = ClientHashingStrategy.create(com.twitter.util.Function.func(
      (ThriftStructIface struct) -> {
        Map<Object, ThriftStructIface> map = new HashMap<>();
        for (String item : ((FanoutTestService.query_args) struct).x) {
          map.put(item.length() % 2, new FanoutTestService.query_args(Arrays.asList(item)));
        }
        return map;
      }));
    strategy.responseMergerRegistry().addResponseMerger(
      new FanoutTestService.query(),
      flatListResponse
    );
    strategy.requestMergerRegistry().addRequestMerger(
      new FanoutTestService.query(),
      flatListRequest
    );
    return strategy;
  }

  private PartitioningStrategy getCustomStrategy() {
    CustomPartitioningStrategy strategy =
      ClientCustomStrategies.noResharding(com.twitter.util.Function.func(
        (ThriftStructIface struct) -> {
          Map<Integer, ThriftStructIface> map = new HashMap<>();
          for (String item : ((FanoutTestService.query_args) struct).x) {
            map.put(item.length() % 2, new FanoutTestService.query_args(Arrays.asList(item)));
          }
          return Future.value(map);
        }));
    strategy.responseMergerRegistry().addResponseMerger(
      new FanoutTestService.query(),
      flatListResponse
    );
    return strategy;
  }

  /**
   * Tests whether you can shard requests in Java using a consistent hashing partitioning strategy
   */
  @Test
  public void testConsistentHashingPartitioningJava() throws Exception {
    FanoutTestJavaServiceImpl identity = new FanoutTestJavaServiceImpl(x -> x);
    FanoutTestJavaServiceImpl reverse = new FanoutTestJavaServiceImpl(x ->
      new StringBuilder(x).reverse().toString());

    FanoutTestService.ServiceIface client =
      ThriftMux.client()
        .withPartitioning().strategy(getConsistentHashingStrategy())
        .newIface(
          Names.bound(createAddress(0, identity), createAddress(1, reverse)),
          "a_client",
          FanoutTestService.ServiceIface.class);

    List<String> hiList = await(client.query(Arrays.asList("hi")));
    String[] expectedHi = {"hi"};
    assertArrayEquals(expectedHi, hiList.toArray());

    List<String> eybList = await(client.query(Arrays.asList("bye")));
    String[] expectedEyb = {"eyb"};
    assertArrayEquals(expectedEyb, eybList.toArray());
  }

  /**
   * Tests whether you can shard requests in Java using a custom partitioning strategy
   */
  @Test
  public void testCustomPartitioningJava() throws Exception {
    FanoutTestJavaServiceImpl identity = new FanoutTestJavaServiceImpl(x -> x);
    FanoutTestJavaServiceImpl reverse = new FanoutTestJavaServiceImpl(x ->
      new StringBuilder(x).reverse().toString());

    FanoutTestService.ServiceIface client =
      ThriftMux.client()
        .withPartitioning().strategy(getCustomStrategy())
        .newIface(
          Names.bound(createAddress(0, identity), createAddress(1, reverse)),
          "a_client",
          FanoutTestService.ServiceIface.class);

    List<String> hiList = await(client.query(Arrays.asList("hi")));
    String[] expectedHi = {"hi"};
    assertArrayEquals(expectedHi, hiList.toArray());

    List<String> eybList = await(client.query(Arrays.asList("bye")));
    String[] expectedEyb = {"eyb"};
    assertArrayEquals(expectedEyb, eybList.toArray());
  }

  /**
   * Tests whether you can fanout requests in Java using a consistent hashing partitioning strategy
   */
  @Test
  public void testConsistentHashingFanoutJava() throws Exception {
    FanoutTestJavaServiceImpl identity = new FanoutTestJavaServiceImpl(x -> x);
    FanoutTestJavaServiceImpl reverse = new FanoutTestJavaServiceImpl(x ->
      new StringBuilder(x).reverse().toString());

    FanoutTestService.ServiceIface client =
      ThriftMux.client()
        .withPartitioning().strategy(getConsistentHashingStrategy())
        .newIface(
          Names.bound(createAddress(0, identity), createAddress(1, reverse)),
          "a_client",
          FanoutTestService.ServiceIface.class);

    List<String> actual = await(client.query(Arrays.asList("hi", "bye")));
    actual.sort((left, right) -> left.compareTo(right));
    String[] expected = {"eyb", "hi"};
    assertArrayEquals(expected, actual.toArray());
  }

  /**
   * Tests whether you can fanout requests in Java using a custom partitioning strategy
   */
  @Test
  public void testCustomFanoutJava() throws Exception {
    FanoutTestJavaServiceImpl identity = new FanoutTestJavaServiceImpl(x -> x);
    FanoutTestJavaServiceImpl reverse = new FanoutTestJavaServiceImpl(x ->
      new StringBuilder(x).reverse().toString());

    FanoutTestService.ServiceIface client =
      ThriftMux.client()
        .withPartitioning().strategy(getCustomStrategy())
        .newIface(
          Names.bound(createAddress(0, identity), createAddress(1, reverse)),
          "a_client",
          FanoutTestService.ServiceIface.class);

    List<String> actual = await(client.query(Arrays.asList("hi", "bye")));
    actual.sort((left, right) -> left.compareTo(right));
    String[] expected = {"eyb", "hi"};
    assertArrayEquals(expected, actual.toArray());
  }
}
