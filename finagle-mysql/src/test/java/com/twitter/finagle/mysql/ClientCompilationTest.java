package com.twitter.finagle.mysql;

import org.junit.Test;

import com.twitter.finagle.exp.Mysql;
import com.twitter.finagle.exp.Mysql.Client;
import com.twitter.finagle.param.Label;

public final class ClientCompilationTest {

  /**
   * Tests Java usage of the Mysql client. The client API should be as accessible in Java as it is
   * in Scala.
   */
  @Test
  public void testClientCompilation() {
    final Client client = Mysql.client()
        .withDatabase("random-db")
        .configured(new Label("test").mk())
        .withCredentials("user", "password");
  }
}
