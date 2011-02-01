package com.twitter.finagle.util

import java.io.{
  ByteArrayOutputStream, ByteArrayInputStream,
  DataOutputStream, DataInputStream}

object TracingHeader {
  def encode(txid: Long, body: Array[Byte]) = {
    val bos = new ByteArrayOutputStream(8)
    val dos = new DataOutputStream(bos)

    dos.writeLong(txid)
    dos.flush()

    bos.toByteArray ++ body
  }

  def decode(bytes: Array[Byte]) = {
    val bis  = new ByteArrayInputStream(bytes)
    val dis  = new DataInputStream(bis)
    val txid = dis.readLong()

    (bytes drop 8, txid)
  }
}
