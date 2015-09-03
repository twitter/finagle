package com.twitter.finagle;

import org.junit.Test;

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

}
