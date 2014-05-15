package com.twitter.finagle.example.java.thrift;

import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Thrift;
import com.twitter.finagle.example.thriftscala.Hello;
import com.twitter.util.Await;
import com.twitter.util.Future;
import com.twitter.util.TimeoutException;
import java.lang.InterruptedException;

public class ThriftServer {
  public static class HelloImpl implements Hello.FutureIface {
    public Future<String> hi() {
      return Future.value("hi");
    }
  }

  public static void main(String args[]) throws TimeoutException, InterruptedException {
    //#thriftserverapi
    Hello.FutureIface impl = new HelloImpl();
    ListeningServer server = Thrift.serveIface("localhost:8080", impl);
    Await.ready(server);
    //#thriftserverapi
  }
}
