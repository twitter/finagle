package com.twitter.finagle.mux;

import org.junit.Test;

import com.twitter.io.Buf;
import com.twitter.io.Bufs;

public class RequestsCompilationTest {

  private Buf buf = Bufs.EMPTY;

  @Test
  public void makeWithPayload() {
    Request request = Requests.make(buf);
  }

}
