package com.twitter.finagle.builder;

import com.twitter.finagle.Codec;

public class Codec4J {
  public static Codec Http = new Http(0);
  public static Codec HttpWithCompression = new Http(6);
}
