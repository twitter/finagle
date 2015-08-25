package com.twitter.finagle.memcached.java;

import com.twitter.io.Buf;

public class ResultWithCAS {
  public final Buf value;
  public final Buf casUnique;

  public ResultWithCAS(Buf value, Buf casUnique) {
    if (value == null) {
      throw new NullPointerException("value cannot be null");
    }

    if (casUnique == null) {
      throw new NullPointerException("casUnique cannot be null");
    }

    this.value = value;
    this.casUnique = casUnique;
  }
}
