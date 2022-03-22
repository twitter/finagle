package com.twitter.finagle.netty4.codec.compression;

public class MalformedInputException
    extends RuntimeException {
  private final long offset;

  public MalformedInputException(long offset) {
    this(offset, "Malformed input");
  }

  public MalformedInputException(long offset, String reason) {
    super(reason + ": offset=" + offset);
    this.offset = offset;
  }
}
