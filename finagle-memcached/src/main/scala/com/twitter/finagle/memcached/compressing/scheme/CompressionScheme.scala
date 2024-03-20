package com.twitter.finagle.memcached.compressing.scheme

object CompressionScheme {

  /**
   * Number of bits to shift the flag setting for a compression scheme. The bits to indicate which
   * compression scheme is used for a blob are stored in bits 5-7 of the memcache flags value.
   */
  final val FlagShift = 4
  final val FlagMask = 0x7 << FlagShift

  /**
   * Set of all possible compression schemes. Used by tests to enumerate all possible
   * types for compatibility checking.
   */
  final val All = Set(
    Uncompressed,
    Lz4
  )

  /**
   * Given a raw flags value from memcache, determine the compression scheme used for the
   * blob.
   * @param flags the raw, unmodified flags value from the underlying memcache client
   * @return the compression scheme that was used
   */
  def compressionType(flags: Int): CompressionScheme = {
    val masked = (flags & FlagMask) >> FlagShift
    if (masked == Uncompressed.flagSettingId) {
      Uncompressed
    } else if (masked == Lz4.flagSettingId) {
      Lz4
    } else {
      throw new NotImplementedError("unknown compression scheme")
    }
  }

  /**
   * Given a compression scheme return the compression flags associated with the scheme
   *
   * @param flagSettingId the compression scheme flag settingId
   *
   * @return the compression flags associated
   */
  def compressionFlags(flagSettingId: Int): Int = {
    flagSettingId << CompressionScheme.FlagShift
  }

  /**
   * Mask off the compression bits from the provided flags, and then set them with
   * what we got back from the injection (which is the scheme that was actually applied).
   *
   * @param flags the raw, unmodified flags value from the underlying memcache client
   * @param compressionFlags the compression flags based on the compression scheme
   * @return the compression scheme that was used
   */
  def flagsWithCompression(flags: Int, compressionFlags: Int): Int = {
    (flags & ~FlagMask) | compressionFlags
  }
}

/**
 * Union type for compression schemes that are supported by the compressing memcache client.
 */
sealed trait CompressionScheme {

  /**
   * An enumeration, with a value from 0-7, representing the codec used to store the data.
   * 0 is used as the value for uncompressed data, for compatibility with existing blobs
   * in memcache.
   * @return the enumerated codec value to use for this compression scheme
   */
  def flagSettingId: Int
  def name: String
  def compressionFlags: Int
  // 3 bits reserved for compression flags
  require((flagSettingId >= 0) && (flagSettingId < 8))
}

/**
 * No compression applied. The flag setting for this scheme is 0, for backwards compatibility
 * with existing memcache blobs.
 */
object Uncompressed extends CompressionScheme {
  override final val flagSettingId = 0
  override final val name = "uncompressed"
  override final val compressionFlags = CompressionScheme.compressionFlags(flagSettingId)
}

/**
 * Lz4-based compression scheme. A 4 byte header is inserted in front of the blob, containing
 * the length of the uncompressed data. (This is an optimization for the decompression process,
 * so the correct amount of memory can be allocated up front for the decompressed value.)
 */
object Lz4 extends CompressionScheme {
  override final val flagSettingId = 1
  override final val name = "lz4"
  override final val compressionFlags = CompressionScheme.compressionFlags(flagSettingId)
}
