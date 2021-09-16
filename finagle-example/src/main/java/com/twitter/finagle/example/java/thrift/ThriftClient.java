package com.twitter.finagle.example.java.thrift;

import scala.runtime.BoxedUnit;

import com.twitter.finagle.Thrift;
import com.twitter.finagle.example.thriftscala.Hello;
import com.twitter.util.Await;
import com.twitter.util.Function;
import com.twitter.util.Future;

public final class ThriftClient {

  private ThriftClient() { }

  /**
   * Runs the example with given {@code args}.
   *
   * @param args the argument list
   */
  public static void main(String[] args) throws Exception {
    //#thriftclientapi
    Hello.MethodPerEndpoint client =
        Thrift.client().build("localhost:8080", Hello.MethodPerEndpoint.class);
    Future<String> response = client.hi().onSuccess(new Function<String, BoxedUnit>() {
      @Override
      public BoxedUnit apply(String response) {
        System.out.println("Received response: " + response);
        return null;
      }
    });

    Await.result(response);
    //#thriftclientapi
  }
}
