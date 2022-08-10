package com.twitter.finagle.netty4.codec.compression.zstd;

public final class ZstdConstants {

  private ZstdConstants() { }

  public static final int SIZE_OF_BYTE = 1;
  public static final int SIZE_OF_SHORT = 2;
  public static final int SIZE_OF_INT = 4;
  public static final int SIZE_OF_LONG = 8;

  public static final int MIN_WINDOW_LOG = 10;
  public static final int MAX_WINDOW_LOG = 31;

  public static final int SIZE_OF_MAGIC = SIZE_OF_INT;
  public static final int MAX_FRAME_HEADER_SIZE = 14;
  public static final int MAX_SIZE_OF_HEADERS = SIZE_OF_MAGIC + MAX_FRAME_HEADER_SIZE;

  /**
   * Default compression level
   */
  public static final int DEFAULT_COMPRESSION_LEVEL = 3;

  /**
   * Max compression level
   */
  public static final int MAX_COMPRESSION_LEVEL = 22;

  /**
   * Max block size
   */
  public static final int MAX_BLOCK_SIZE = 1 << 17; //  128kB

  /**
   * Default block size
   */
  public static final int DEFAULT_BLOCK_SIZE = 1 << 17; // 64kB

  /**
   * Default buffer size
   */
  public static final int BUFFER_SIZE = 1 << 19; // 512 KB

  /**
   * Default max block size
   */
  public static final int DEFAULT_MAX_ENCODE_SIZE = 1 << 23; // 8M

  /**
   * Max decompress size
   * When greater than max decompress size,
   * need to ensure that there is enough space for decompression
   */
  public static final int MAX_DECOMPRESS_SIZE = 1 << 16;
}
