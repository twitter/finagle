package com.twitter.finagle.builder;

public class Codec4J {
  public static Codec Http = new Http(0);
  public static Codec HttpWithCompression = new Http(6);
  public static Codec Thrift = com.twitter.finagle.builder.Thrift.instance();
}
