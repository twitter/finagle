package com.twitter.finagle.builder;

import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

import com.twitter.finagle.Codec;

@Deprecated
public class Codec4J {
  public static Codec<HttpRequest, HttpResponse> Http = new Http(0);
  public static Codec<HttpRequest, HttpResponse> HttpWithCompression = new Http(6);
}
