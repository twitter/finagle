package com.twitter.finagle.thriftmux.pushsession

import com.twitter.finagle.context.{Contexts, RemoteInfo}
import com.twitter.finagle.{Dtab, Path, Service, Stack, Status, Thrift, mux, param}
import com.twitter.finagle.pushsession.{PushChannelHandle, PushSession}
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.mux.{ClientDiscardedRequestException, ServerProcessor}
import com.twitter.finagle.stats.Verbosity
import com.twitter.finagle.thrift.thrift.RequestHeader
import com.twitter.finagle.thrift.{ClientId, InputBuffer, RichRequestHeader}
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.transport.Transport
import com.twitter.io.{Buf, ByteReader}
import com.twitter.logging.{Level, Logger}
import com.twitter.util._
import java.util
import org.apache.thrift.protocol.TProtocolFactory
import scala.annotation.tailrec
import scala.collection.mutable

/**
 * Push based implementation of a standard Thrift dispatcher
 *
 * This exists because ThriftMux servers can downgrade to the standard (vanilla) Thrift
 * protocol. It is a pipelining dispatcher that lacks a control plane so it comes with
 * all the hazards associated with that.
 *
 * Draining is done by trying to wait for the session to become idle, eg no dispatches
 * flushing or waiting for a response from the service, and then hanging up. This is
 * intrinsically racy but the best we can do without a mechanism to signal to the client
 * that the session is going down.
 */
private final class VanillaThriftSession(
  handle: PushChannelHandle[ByteReader, Buf],
  ttwitterHeader: Option[Buf],
  params: Stack.Params,
  service: Service[mux.Request, mux.Response])
    extends PushSession[ByteReader, Buf](handle) {
  import VanillaThriftSession._

  private[this] def exec = handle.serialExecutor

  private[this] val isTTwitter = ttwitterHeader.isDefined
  private[this] val header: Buf = ttwitterHeader.getOrElse(Buf.Empty)
  private[this] val tProtocolFactory = params[Thrift.param.ProtocolFactory].protocolFactory
  private[this] val timer = params[param.Timer].timer

  @volatile
  private[this] var state: State = Running
  private[this] var pendingWrites: Int = 0
  private[this] val pending = new util.ArrayDeque[Future[Message]](1)

  private[this] val pendingGauge = params[param.Stats].statsReceiver
    .addGauge(Verbosity.Debug, "pending") { pending.size }

  private[this] val respond: Try[Message] => Unit = {
    // Since it's stateless we can reuse the Runnable!
    val runnable = new Runnable { def run = handleResponseComplete() }
    { _ => exec.execute(runnable) }
  }

  // These are the locals we need to have set on each dispatch
  private[this] val locals: Local.Context =
    Local.letClear {
      val peerCertLocal =
        Contexts.local.KeyValuePair(Transport.sslSessionInfoCtx, handle.sslSessionInfo)

      val remoteAddressLocal =
        Contexts.local.KeyValuePair(RemoteInfo.Upstream.AddressCtx, handle.remoteAddress)

      Trace.letTracer(params[param.Tracer].tracer) {
        Contexts.local.let(Seq(remoteAddressLocal, peerCertLocal)) {
          Local.save()
        }
      }
    }

  // Make sure we cleanup when we're finished
  handle.onClose.ensure(exec.execute(new Runnable {
    def run(): Unit = handleForceClose()
  }))

  def receive(message: ByteReader): Unit = {
    try safeReceive(message)
    finally message.close()
  }

  private[this] def safeReceive(reader: ByteReader): Unit = {
    if (state != Closed) {
      val f = Local.let(locals) {
        val dispatch = thriftToMux(isTTwitter, tProtocolFactory, reader.readAll())
        ServerProcessor(dispatch, service)
      }

      pending.offer(f)
      f.respond(respond)

    } else if (log.isLoggable(Level.DEBUG)) {
      log.debug(s"Discarding dispatch of size ${reader.remaining}")
    }
  }

  @tailrec
  private[this] def handleResponseComplete(): Unit = {
    if (!pending.isEmpty) {
      val head = pending.peek
      head.poll match {
        case Some(r) => // we have a complete response! What luck!
          pending
            .poll() // remove the promise from the queue (`r` is its value, so we don't need it)
          handleRenderResponse(r)
          handleResponseComplete() // now try it again since we may have more that can be written
        case None => // not finished, so we terminate the loop
      }
    }
  }

  private[this] def handleRenderResponse(result: Try[Message]): Unit = result match {
    case Return(Message.RdispatchOk(_, _, rep)) if isTTwitter =>
      sendBlob(header.concat(rep))

    case Return(Message.RdispatchOk(_, _, rep)) =>
      sendBlob(rep)

    case Return(Message.RdispatchNack(_, _)) =>
      // The only mechanism for negative acknowledgement afforded by non-Mux
      // clients is to tear down the connection.
      handle.close() // orphan the future

    case unexpected =>
      // we can't write this, so we signal failure to the remote
      // by tearing down the session.
      handleForceClose()
      // log here to surface the error
      if (log.isLoggable(Level.INFO)) {
        log.info(s"unable to write ${unexpected} to non-mux client")
      }
  }

  private[this] def sendBlob(blob: Buf): Unit = {
    pendingWrites += 1
    handle.send(blob) { _ =>
      pendingWrites -= 1
      handleCheckDrain()
    }
  }

  def status: Status = Status.worst(handle.status, state.status)

  def close(deadline: Time): Future[Unit] = {
    exec.execute(new Runnable { def run: Unit = handleClose(deadline) })
    handle.onClose
  }

  // For closing we attempt to field dispatches until we've received an Rdrain
  // and then subsequently have no more outstanding dispatches
  private[this] def handleClose(deadline: Time): Unit = {
    if (state == Running) {
      val task = timer.schedule(deadline) { handle.close() }
      state = Draining(task)
      handleCheckDrain()
    }
  }

  private[this] def handleCheckDrain(): Unit = state match {
    case Draining(_) if pendingWrites == 0 && pending.size == 0 => handleForceClose()
    case _ => // nop
  }

  // The designated shutdown method
  private[this] def handleForceClose(): Unit = {
    val old = state
    if (old != Closed) {
      state = Closed
      old match {
        case Draining(closeTask) => closeTask.cancel()
        case _ => // nop
      }

      if (!pending.isEmpty) {
        val cause = new ClientDiscardedRequestException(
          s"Session closed. Remote: ${handle.remoteAddress}"
        )
        while (!pending.isEmpty) {
          pending.poll().raise(cause)
        }
      }

      // Close the handle and the service
      handle.close()
      service.close()
      pendingGauge.remove()
    }
  }
}

private object VanillaThriftSession {
  private val log = Logger.get(classOf[VanillaThriftSession])

  private sealed trait State {
    final def status: Status = this match {
      case Running => Status.Open
      case Draining(_) => Status.Busy
      case Closed => Status.Closed
    }
  }

  private object Running extends State
  private case class Draining(forceCloseTask: TimerTask) extends State
  private object Closed extends State

  /**
   * Returns a `Mux.Tdispatch` from a thrift dispatch message.
   */
  private def thriftToMux(
    ttwitter: Boolean,
    protocolFactory: TProtocolFactory,
    buf: Buf
  ): Message.Tdispatch = {
    // It's okay to use a static tag since we serialize messages into
    // the dispatcher so we are ensured no tag conflicts.
    val tag = Message.Tags.MinTag
    if (!ttwitter) {
      Message.Tdispatch(tag, Nil, Path.empty, Dtab.empty, buf)
    } else {
      val header = new RequestHeader
      val request = InputBuffer.peelMessage(
        Buf.ByteArray.Owned.extract(buf),
        header,
        protocolFactory
      )
      val richHeader = new RichRequestHeader(header)
      val contextBuf =
        new mutable.ArrayBuffer[(Buf, Buf)](
          2 + (if (header.contexts == null) 0 else header.contexts.size)
        )

      contextBuf += (Trace.TraceIdContext.marshalId -> Trace.TraceIdContext.marshal(
        richHeader.traceId))

      richHeader.clientId match {
        case Some(clientId) =>
          val clientIdBuf = ClientId.clientIdCtx.marshal(Some(clientId))
          contextBuf += ClientId.clientIdCtx.marshalId -> clientIdBuf
        case None =>
      }

      if (header.contexts != null) {
        val iter = header.contexts.iterator()
        while (iter.hasNext) {
          val c = iter.next()
          contextBuf += (
            Buf.ByteArray.Owned(c.getKey) -> Buf.ByteArray.Owned(c.getValue)
          )
        }
      }

      val requestBuf = Buf.ByteArray.Owned(request)
      Message.Tdispatch(tag, contextBuf.toSeq, richHeader.dest, richHeader.dtab, requestBuf)
    }
  }
}
