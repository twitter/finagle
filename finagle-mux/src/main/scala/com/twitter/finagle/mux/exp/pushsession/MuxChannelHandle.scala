package com.twitter.finagle.mux.exp.pushsession

import com.twitter.finagle.Mux.param.TurnOnTlsFn
import com.twitter.finagle.Stack
import com.twitter.finagle.exp.pushsession.{PushChannelHandle, PushChannelHandleProxy}
import com.twitter.io.{Buf, ByteReader}
import com.twitter.logging.Logger
import io.netty.channel.Channel
import java.util.concurrent.atomic.AtomicBoolean

/**
 * The only purpose of the MuxChannelHandle is to thread through the ability to
 * add the OpportunisticTls support. This is currently specialized to Netty4 since
 * we use the Netty4 TLS implementation.
 */
private[finagle] class MuxChannelHandle(
    underlying: PushChannelHandle[ByteReader, Buf],
    ch: Channel,
    params: Stack.Params)
  extends PushChannelHandleProxy[ByteReader, Buf](underlying) {

  private[this] val tlsGuard = new AtomicBoolean(false)

  /** Enable TLS support */
  def turnOnTls(): Unit = {
    if (tlsGuard.compareAndSet(false, true)) params[TurnOnTlsFn].fn(params, ch.pipeline)
    else MuxChannelHandle.log.warning("Attempted to turn on TLS multiple times")
  }

  /** A specialized method for sending data without going through the serial executor
   *
   * This lives only to avoid the potential race during opportunistic TLS negotiation
   * where we need to send our headers unencrypted to the peer, but immediately after
   * reconfigure the Netty pipeline for TLS, eg before receiving any more data (which
   * would be the TLS handshake). If we use the standard `sendAndForget`, we bounce
   * the write through the serial executor and there is no way to guarantee that the
   * pipeline refactor happens immediately after the headers have been encoded and made
   * it to the channels outbound byte buffer.
   */
  def sendNowAndForget(buf: Buf): Unit = {
    ch.writeAndFlush(buf, ch.voidPromise())
  }
}

private object MuxChannelHandle {
  private val log = Logger.get
}
