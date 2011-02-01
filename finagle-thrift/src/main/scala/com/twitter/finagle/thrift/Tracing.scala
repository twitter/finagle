package com.twitter.finagle.thrift

import java.util.concurrent.atomic.AtomicInteger

import java.io.{
  ByteArrayOutputStream, ByteArrayInputStream,
  DataOutputStream, DataInputStream}

object Tracing {
  val CanTraceMethodName = "__can__twitter__trace__v1__"

  def encodeHeader(txid: Long, body: Array[Byte]) = {
    val bos = new ByteArrayOutputStream(8)
    val dos = new DataOutputStream(bos)

    dos.writeLong(txid)
    dos.flush()

    bos.toByteArray ++ body
  }

  def decodeHeader(bytes: Array[Byte]) = {
    val bis  = new ByteArrayInputStream(bytes)
    val dis  = new DataInputStream(bis)
    val txid = dis.readLong()

    (bytes drop 8, txid)
  }
}

