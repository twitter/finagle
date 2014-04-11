package com.twitter.finagle.serverset2.client

import com.twitter.io.Buf

package object apache {
  private[apache] def toByteArray(buf: Buf): Array[Byte] = buf match {
    case Buf.ByteArray(a, 0, len) if len == buf.length => a
    case b => {
      val bytes = new Array[Byte](b.length)
      b.write(bytes, 0)
      bytes
    }
  }
}
