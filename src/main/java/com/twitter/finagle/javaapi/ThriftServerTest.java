package com.twitter.finagle.javaapi;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.*;
import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import com.twitter.finagle.service.*;
import com.twitter.finagle.builder.*;
import com.twitter.finagle.thrift.*;
import com.twitter.util.*;

import com.twitter.silly.Silly;

public class ThriftServerTest {
  public static void runServer() {
    ThriftTypes.add(new ThriftCallFactory<Silly.bleep_args, Silly.bleep_result>
                    ("bleep", Silly.bleep_args.class, Silly.bleep_result.class));

    Service<ThriftCall, ThriftReply> service =
      new Service<ThriftCall, ThriftReply>() {
      @Override
      public Future<ThriftReply> apply(ThriftCall call) {
        Promise<ThriftReply> future = new Promise<ThriftReply>();

        if (call.getMethod().equals("bleep")) {
          Silly.bleep_result result = (Silly.bleep_result)call.newReply();
          result.setSuccess("bleepety bleep");
          future.update(new Return<ThriftReply>(call.reply(result)));
        }

        return future;
      }
    };

    ServerBuilder
      .get()
      .codec(Codec4J.thrift())
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