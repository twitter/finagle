package com.twitter.finagle;

import org.junit.Test;

import static org.junit.Assert.*;

import com.twitter.util.Function;
import com.twitter.util.Future;

public class ServiceCompilationTest {

  @Test
  public void testCompilation() {
    Service.constant(Future.value("hi"));

    Service<String, String> mkSvc =
      Service.mk(new Function<String, Future<String>>() {
        public Future<String> apply(String s) {
          return Future.value(s + s);
        }
      });

    Service.rescue(mkSvc);
  }

  @Test
  public void testServiceToString() {
    Service<Object, String> constSvc = Service.constant(Future.value("hi"));
    assertEquals(
        "com.twitter.finagle.service.ConstantService(ConstFuture(Return(hi)))",
        constSvc.toString());

    Service<String, String> mkSvc =
        Service.mk(new Function<String, Future<String>>() {
          public Future<String> apply(String s) {
            return Future.value(s + s);
          }
        });

     assertEquals("com.twitter.finagle.Service$$anon$2", mkSvc.toString());
  }

}
