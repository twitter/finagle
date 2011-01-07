package com.twitter.finagle.javaapi;


import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.service.Service;
import com.twitter.finagle.thrift.*;
import com.twitter.silly.Silly;
import com.twitter.util.Future;

import java.net.InetSocketAddress;

public class ThriftServerTest {
  public static void runServer() {
    ThriftCallFactory<Silly.bleep_args, Silly.bleep_result> callFactory = new ThriftCallFactory<Silly.bleep_args, Silly.bleep_result>
      ("bleep", Silly.bleep_args.class, Silly.bleep_result.class);

    ThriftTypes.add(callFactory);

    Service<ThriftCall, ThriftReply> service =
      new Service<ThriftCall, ThriftReply>() {

      public Future<ThriftReply> apply(ThriftCall call) {
        if (call.getMethod().equals("bleep")) {
          Silly.bleep_result result = (Silly.bleep_result)call.newReply();
          result.setSuccess("bleepety bleep");
          Future<ThriftReply> future = Future.value(call.reply(result));
          return future;
        }

        throw new IllegalArgumentException("Method: " + call.getMethod() + " is unsupported!");
      }
    };

    ServerBuilder
      .get()
      .codec(new Thrift())
      .service(service)
      .bindTo(new InetSocketAddress("localhost", 10000))
      .build();
  }

  public static void main(String args[]) {
    try {
      runServer();
    } catch (Throwable e) {
      System.err.println("Caught top level exception: " + e);
      e.printStackTrace();
      System.exit(-1);
    }
  }
}