package com.twitter.finagle.memcached

object Hash {

  val FNV1_32_PRIME = 16777619
  def fnv1_32(key: String) : Long = {
    var i = 0
    val len = key.length
    var rv: Long = 0x811c9dc5L
    val keyBytes = key.getBytes("UTF-8")
    while (i < len) {
      rv = (rv * FNV1_32_PRIME) ^ (keyBytes(i) & 0xff)
      i += 1
    }

    rv & 0xffffffffL
  }
}
