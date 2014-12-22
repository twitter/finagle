package com.twitter.finagle.example.java.thrift;

import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Thrift;
import com.twitter.finagle.example.thriftscala.Hello;
import com.twitter.util.Await;
import com.twitter.util.Future;
import com.twitter.util.TimeoutException;

public final class ThriftServer {

  private ThriftServer() { }

  public static class HelloImpl implements Hello.FutureIface {
    public Future<String> hi() {
      return Future.value("hi");
    }
  }

  /**
   * Runs the example with given {@code args}.
   *
   * @param args the argument list
   */
  public static void main(String[] args) throws TimeoutException, InterruptedException {
    //#thriftserverapi
    Hello.FutureIface impl = new HelloImpl();
    ListeningServer server = Thrift.serveIface("localhost:8080", impl);
    Await.ready(server);
    //#thriftserverapi
  }
}
