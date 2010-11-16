package com.twitter.finagle.javaapi;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.*;
import org.jboss.netty.channel.*;

import com.twitter.finagle.stub.Stub;
import com.twitter.finagle.builder.*;
import com.twitter.finagle.thrift.*;
import com.twitter.util.*;

import com.twitter.silly.Silly;

public class ThriftClientTest {
  public static void main(String args[]) {

    Stub<ThriftCall<Silly.bleep_args, Silly.bleep_result>, Silly.bleep_result> client =
      ClientBuilder.get()
        .hosts("localhost:10000")
        .codec(Codec4J.thrift())
        .buildStub();

    Future<Silly.bleep_result> response =
      client.call(new ThriftCall("bleep", new Silly.bleep_args(), Silly.bleep_result.class));
    System.out.println("dispatched call");

    response.addEventListener(
      new FutureEventListener<Silly.bleep_result>() {
        public void onSuccess(Silly.bleep_result response) {
          System.out.println("received response: " + response);
          System.exit(0);
        }

        public void onFailure(Throwable cause) {
          System.out.println("failed with cause: " + cause);
          System.exit(1);
        }
      });
  }
}
