package com.twitter.finagle.memcached

import scala.collection.mutable
import _root_.java.nio.{ByteBuffer, ByteOrder}
import _root_.java.security.MessageDigest


/**
 * Hashes a memcache key into a 32-bit or 64-bit number (depending on the algorithm).
 * This is a purely optional trait, meant to allow NodeLocator implementations to share
 * hash functions.
 */
trait KeyHasher {
  def hashKey(key: Array[Byte]): Long
}


/**
 * Commonly used key hashing algorithms.
 */
object KeyHasher {
  /**
   * FNV fast hashing algorithm in 32 bits.
   * @see http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
   */
  val FNV1_32 = new KeyHasher {
    def hashKey(key: Array[Byte]): Long = {
      val PRIME: Int = 16777619
      var i = 0
      val len = key.length
      var rv: Long = 0x811c9dc5L
      while (i < len) {
        rv = (rv * PRIME) ^ (key(i) & 0xff)
        i += 1
      }
      rv & 0xffffffffL
    }

    override def toString() = "FNV1_32"
  }

  /**
   * FNV fast hashing algorithm in 32 bits, variant with operations reversed.
   * @see http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
   */
  val FNV1A_32 = new KeyHasher {
    def hashKey(key: Array[Byte]): Long = {
      val PRIME: Int = 16777619
      var i = 0
      val len = key.length
      var rv: Long = 0x811c9dc5L
      while (i < len) {
        rv = (rv ^ (key(i) & 0xff)) * PRIME
        i += 1
      }
      rv & 0xffffffffL
    }

    override def toString() = "FNV1A_32"
  }

  /**
   * FNV fast hashing algorithm in 64 bits.
   * @see http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
   */
  val FNV1_64 = new KeyHasher {
    def hashKey(key: Array[Byte]): Long = {
      val PRIME: Long = 1099511628211L
      var i = 0
      val len = key.length
      var rv: Long = 0xcbf29ce484222325L
      while (i < len) {
        rv = (rv * PRIME) ^ (key(i) & 0xff)
        i += 1
      }
      rv & 0xffffffffL
    }

    override def toString() = "FNV1_64"
  }

  /**
   * FNV fast hashing algorithm in 64 bits, variant with operations reversed.
   * @see http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
   */
  val FNV1A_64 = new KeyHasher {
    def hashKey(key: Array[Byte]): Long = {
      val PRIME: Long = 1099511628211L
      var i = 0
      val len = key.length
      var rv: Long = 0xcbf29ce484222325L
      while (i < len) {
        rv = (rv ^ (key(i) & 0xff)) * PRIME
        i += 1
      }
      rv & 0xffffffffL
    }

    override def toString() = "FNV1A_64"
  }

  /**
   * Ketama's default hash algorithm: the first 4 bytes of the MD5 as a little-endian int.
   * Wow, really? Who thought that was a good way to do it? :(
   */
  val KETAMA = new KeyHasher {
    def hashKey(key: Array[Byte]): Long = {
      val hasher = MessageDigest.getInstance("MD5")
      hasher.update(key)
      val buffer = ByteBuffer.wrap(hasher.digest)
      buffer.order(ByteOrder.LITTLE_ENDIAN)
      buffer.getInt.toLong & 0xffffffffL
    }

    override def toString() = "Ketama"
  }

  /**
   * The default memcache hash algorithm is the ITU-T variant of CRC-32.
   */
  val CRC32_ITU = new KeyHasher {
    def hashKey(key: Array[Byte]): Long = {
      var i = 0
      val len = key.length
      var rv: Long = 0xffffffffL
      while (i < len) {
        rv = rv ^ (key(i) & 0xff)
        var j = 0
        while (j < 8) {
          if ((rv & 1) != 0) {
            rv = (rv >> 1) ^ 0xedb88320L
          } else {
            rv >>= 1
          }
          j += 1
        }
        i += 1
      }
      (rv ^ 0xffffffffL) & 0xffffffffL
    }

    override def toString() = "CRC32_ITU"
  }

  private val hashes = new mutable.HashMap[String, KeyHasher]
  hashes += ("fnv" -> FNV1_32)
  hashes += ("fnv1" -> FNV1_32)
  hashes += ("fnv1-32" -> FNV1_32)
  hashes += ("fnv1a-32" -> FNV1A_32)
  hashes += ("fnv1-64" -> FNV1_64)
  hashes += ("fnv1a-64" -> FNV1A_64)
  hashes += ("ketama" -> KETAMA)
  hashes += ("crc32-itu" -> CRC32_ITU)

  /**
   * Register a hash function by name. If used before creating a memcache client from a
   * config file, this will let you create custom hash functions and specify them by name
   * in the config file. (It is not necessary to register a hash function unless you want it
   * to be identified in config files.)
   */
  def register(name: String, hasher: KeyHasher) = {
    hashes += (name -> hasher)
  }

  /**
   * Return one of the key hashing algorithms by name. This is used to configure a memcache
   * client from a config file.
   */
  def byName(name: String): KeyHasher = {
    hashes.get(name) match {
      case Some(h) => h
      case None => throw new IllegalArgumentException("unknown hash: " + name)
    }
  }
}
