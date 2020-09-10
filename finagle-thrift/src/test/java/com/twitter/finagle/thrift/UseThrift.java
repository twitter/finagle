package com.twitter.finagle.thrift;

import java.util.List;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Test;

import com.twitter.finagle.Thrift;
import com.twitter.finagle.param.Label;
import com.twitter.finagle.param.Tracer;
import com.twitter.finagle.tracing.NullTracer;
import com.twitter.test.B;
import com.twitter.test.SomeStruct;
import com.twitter.util.Future;

public class UseThrift {

  /**
   * Tests Java usage of the Thrift client and server. The client and server API should be as
   * accessible in Java as it is in Scala.
   */
  @Test
  public void testClientServerCompilation() {
    Thrift.client().newIface(":*", B.ServiceIface.class);

    Thrift.server().serveIface(":*", new BServiceImpl());

    Thrift.client()
        .withProtocolFactory(new TBinaryProtocol.Factory())
        .configured(new Label("test").mk())
        .withClientId(new ClientId("id"))
        .configured(new Tracer(new NullTracer()).mk())
        .newIface(":*", B.ServiceIface.class);

    Thrift.server()
        .withProtocolFactory(new TBinaryProtocol.Factory())
        .configured(new Label("test").mk())
        .withBufferedTransport()
        .configured(new Tracer(new NullTracer()).mk())
        .serve(":*", new B.Service(new BServiceImpl(), new TBinaryProtocol.Factory()));
  }

  /**
   * A fake implementation of B's service interface for testing purposes.
   */
  private class BServiceImpl implements B.ServiceIface {

    @Override
    public Future<Integer> add(int a, int b) {
      return Future.value(0);
    }

    @Override
    public Future<Void> add_one(int a, int b) {
      return Future.Void();
    }

    @Override
    public Future<Integer> mergeable_add(List<Integer> alist) {
      return Future.value(0);
    }

    @Override
    public Future<SomeStruct> complex_return(String some_string) {
      return Future.value(new SomeStruct());
    }

    @Override
    public Future<Void> someway() {
      return Future.Void();
    }

    @Override
    public Future<String> show_me_your_dtab() {
      return Future.value("SomeDTab");
    }

    @Override
    public Future<Integer> show_me_your_dtab_size() {
      return Future.value(0);
    }

    @Override
    public Future<Integer> multiply(int a, int b) {
      return Future.value(0);
    }
  }
}
