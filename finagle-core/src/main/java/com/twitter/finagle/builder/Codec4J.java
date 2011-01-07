package com.twitter.finagle.builder;

public class Codec4J {
  public static Codec Http = new Http(0);
  public static Codec HttpWithCompression = new Http(6);
}
