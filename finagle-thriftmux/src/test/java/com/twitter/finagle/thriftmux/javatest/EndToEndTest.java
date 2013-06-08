package com.twitter.finagle.thriftmux.javatest;

import org.junit.Test;
import com.twitter.finagle.thriftmux.thriftscrooge3.TestService;
import static junit.framework.Assert.assertEquals;
import com.twitter.util.Future;
import com.twitter.finagle.*;

public class EndToEndTest {
  @Test
  public void test() {
    ListeningServer server = ThriftMux.serveIface(":*", new TestService.FutureIface() {
      public Future<String> query(String x) {
        return Future.value(x+x);
      }
    });

    TestService.FutureIface client = ThriftMux.newIface(server, TestService.FutureIface.class);
    assertEquals(client.query("ok").get(), "okok");
  }
}
