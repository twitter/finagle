package com.twitter.finagle.example.java.thrift;

import com.twitter.finagle.Thrift;
import com.twitter.finagle.example.thriftscala.Hello;
import com.twitter.util.Await;
import com.twitter.util.Function;
import com.twitter.util.Future;
import java.lang.Exception;
import scala.runtime.BoxedUnit;

public class ThriftClient {
  public static void main(String args[]) throws Exception {
    //#thriftclientapi
    Hello.FutureIface client = Thrift.newIface("localhost:8080", Hello.FutureIface.class);
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
