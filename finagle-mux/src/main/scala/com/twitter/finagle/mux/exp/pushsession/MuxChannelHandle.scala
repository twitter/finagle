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
private class MuxChannelHandle(
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
}

private object MuxChannelHandle {
  private val log = Logger.get
}
