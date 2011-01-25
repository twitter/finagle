package com.twitter.test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.twitter.finagle.*;
import com.twitter.finagle.thrift.*;
import com.twitter.finagle.builder.*;
import com.twitter.util.Future;
import com.twitter.util.*;

public class EndToEndTest {
  private static void runServer() {
    class Processor implements B.ServiceIface {
      public Future<Integer> add(int a, int b) throws TException {
        return Future.value(0);
      }

      public Future<Void> add_one(int a, int b) throws TException {
        return Future.value(null);
      }

      public Future<SomeStruct> complex_return(String some_string) throws TException {
        return Future.value(new SomeStruct(123, "foobar"));
      }

      public Future<Integer> multiply(int a, int b) throws TException {
        return Future.value(123);
      }
    }

    ServerBuilder.get()
      .codec(new ThriftFramedTransportCodec())
      .bindTo(new InetSocketAddress("localhost", 10000))
      .build(new B.Service(new Processor(), new TBinaryProtocol.Factory()));
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