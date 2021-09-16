package com.twitter.finagle.example.java.thrift;

import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Thrift;
import com.twitter.finagle.example.thriftscala.Hello;
import com.twitter.util.Await;
import com.twitter.util.Closable;
import com.twitter.util.Closables;
import com.twitter.util.Future;
import com.twitter.util.TimeoutException;

public final class ThriftServer {

  private ThriftServer() { }

  public static class HelloImpl implements Hello.MethodPerEndpoint {
    public Future<String> hi() {
      return Future.value("hi");
    }

    @Override
    public Closable asClosable() {
      return Closables.NOP;
    }
  }

  /**
   * Runs the example with given {@code args}.
   *
   * @param args the argument list
   */
  public static void main(String[] args) throws TimeoutException, InterruptedException {
    //#thriftserverapi
    Hello.MethodPerEndpoint impl = new HelloImpl();
    ListeningServer server = Thrift.server().serveIface("localhost:8080", impl);
    Await.ready(server);
    //#thriftserverapi
  }
}
