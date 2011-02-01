package com.twitter.finagle.thrift

import java.util.concurrent.atomic.AtomicInteger

import java.io.{
  ByteArrayOutputStream, ByteArrayInputStream,
  DataOutputStream, DataInputStream}

import com.twitter.util.Local

object Tracing {
  private[this] val counter = new AtomicInteger(0)
  val txid = new Local[Int]

  def encode(bytes: Array[Byte]) = {
    val bos = new ByteArrayOutputStream(8)
    val dos = new DataOutputStream(bos)

    if (!txid().isDefined)
      txid() = counter.incrementAndGet()

    dos.writeInt(1)
    dos.writeInt(txid().get)
    dos.flush()

    bos.toByteArray ++ bytes
  }

  def decode(bytes: Array[Byte]) = {
    val bis = new ByteArrayInputStream(bytes)
    val dis = new DataInputStream(bis)

    require(dis.readInt() == 1)
    txid() = dis.readInt()

    bytes drop 8
  }
}

