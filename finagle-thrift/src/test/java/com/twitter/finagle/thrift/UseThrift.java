package com.twitter.finagle.thrift;

import scala.Tuple2;

import org.apache.thrift.protocol.TBinaryProtocol;

import com.twitter.finagle.Thrift;
import com.twitter.finagle.param.Label;
import com.twitter.finagle.param.Tracer;
import com.twitter.finagle.tracing.NullTracer;
import com.twitter.test.B;

// Compilation test. Not actually for running.
public class UseThrift {

  static {
    Thrift.newIface(":8000", B.ServiceIface.class);
    Thrift.serveIface(":8000", null);

    Thrift.client()
        .withProtocolFactory(null)
        .configured(Tuple2.apply(Label.apply("test"), Label.param()))
        .withClientId(ClientId.apply("id"))
        .configured(Tuple2.apply(Tracer.apply(new NullTracer()), Tracer.param()))
        .newIface(":8000", B.ServiceIface.class);

    Thrift.server()
        .withProtocolFactory(null)
        .configured(Tuple2.apply(Label.apply("test"), Label.param()))
        .withBufferedTransport()
        .configured(Tuple2.apply(Tracer.apply(new NullTracer()), Tracer.param()))
        .serve(":8000", new B.Service(null, new TBinaryProtocol.Factory()));
  }
}
