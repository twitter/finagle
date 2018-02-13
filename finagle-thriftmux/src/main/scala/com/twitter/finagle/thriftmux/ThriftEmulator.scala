package com.twitter.finagle.thriftmux

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.mux.transport.{BadMessageException, Message}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.thrift._
import com.twitter.finagle.thrift.thrift.{RequestHeader, ResponseHeader, UpgradeReply}
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.transport.{Transport, TransportProxy, UpdatableContext}
import com.twitter.finagle.{Failure, Path, Dtab, Status}
import com.twitter.io.Buf
import com.twitter.logging.Level
import com.twitter.util.{Future, Try, Return, Promise, Throw, Updatable, Time}
import java.net.SocketAddress
import java.security.cert.Certificate
import java.util.logging.Logger
import org.apache.thrift.protocol.{TProtocolFactory, TMessage, TMessageType}
import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * A [[com.twitter.finagle.transport.Transport]] that manages the downgrading
 * of mux server sessions to plain thrift or twitter thrift. Because this is used in the
 * context of the mux server dispatcher, it's important that when we downgrade we
 * faithfully emulate the mux protocol.
 */
private[finagle] object ThriftEmulator {
  private[this] val log = Logger.getLogger(getClass.getName)

  /**
   * A thread-safe swappable Transport reference.
   */
  private class TransportRef[In, Out](init: Transport[In, Out])
      extends Transport[In, Out]
      with Updatable[Transport[In, Out]] {
    @volatile private[this] var cur: Transport[In, Out] = init

    type Context = UpdatableContext

    def write(in: In): Future[Unit] = cur.write(in)
    def read(): Future[Out] = cur.read()

    def update(trans: Transport[In, Out]): Unit = {
      cur = trans
      context() = trans.context
    }
    def status: Status = cur.status
    def onClose: Future[Throwable] = cur.onClose
    def localAddress: SocketAddress = cur.localAddress
    def remoteAddress: SocketAddress = cur.remoteAddress
    def peerCertificate: Option[Certificate] = cur.peerCertificate
    def close(deadline: Time): Future[Unit] = cur.close(deadline)
    val context: UpdatableContext = new UpdatableContext(init.context)
    override def toString: String = cur.toString
  }

  /**
   * Creates a transport that is capable of wrapping a Mux server transport
   * and can support vanilla Thrift and TTwitterThrift clients.
   */
  def apply(
    underlying: Transport[Buf, Buf],
    protocolFactory: TProtocolFactory,
    sr: StatsReceiver
  ): Transport[Buf, Buf] = {
    val transportP = new Promise[Transport[Buf, Buf]]
    // The swap on complete structure allows us to reduce the
    // footprint in the common case where we don't need to
    // downgrade to thrift.
    val init = new Init(underlying, transportP, protocolFactory, sr)
    val ref = new TransportRef[Buf, Buf](init)
    transportP.onSuccess(ref.update(_))
    ref
  }

  private class Init(
    underlying: Transport[Buf, Buf],
    transportP: Promise[Transport[Buf, Buf]],
    protocolFactory: TProtocolFactory,
    sr: StatsReceiver
  ) extends TransportProxy[Buf, Buf](underlying) {
    private[this] val thriftmuxConnects = sr.counter("connects")
    private[this] val downgradedConnects = sr.counter("downgraded_connects")

    // queues writes while we determine the type of session.
    private[this] val writeq = new AsyncQueue[Buf]

    // drain `writeq` into the the transport sequentially
    private[this] def drain(): Future[Unit] =
      if (writeq.size == 0) Future.Done
      else
        writeq
          .poll()
          .flatMap(underlying.write)
          .before { drain() }

    // initiate drain when the new transport is set.
    transportP.ensure { drain() }

    def write(buf: Buf): Future[Unit] = {
      if (writeq.offer(buf)) Future.Done
      else Future.exception(Failure("unable to enqueue write"))
    }

    def read(): Future[Buf] =
      underlying.read().flatMap { buf =>
        Try { Message.decode(buf) } match {
          // We assume that a bad message decode indicates a thrift
          // session. Due to Mux message numbering, a binary-encoded
          // thrift frame corresponds to an Rerr message with tag
          // 65537. Note that in this context, an R-message is never
          // valid.
          //
          // Binary-encoded thrift messages have the format
          //
          //     header:4 n:4 method:n seqid:4
          //
          // The header is
          //
          //     0x80010000 | type
          //
          // where the type of CALL is 1; the type of ONEWAY is 4. This makes
          // the first four bytes of a CALL message 0x80010001.
          //
          // Mux messages begin with
          //
          //     Type:1 tag:3
          //
          // Rerr is type 0x80, so we see the above thrift header
          // Rerr corresponds to (tag=0x010001).
          //
          // The hazards of protocol multiplexing.
          case Throw(Failure(Some(_: BadMessageException))) | Return(Message.Rerr(65537, _)) |
              Return(Message.Rerr(65540, _)) =>
            downgradedConnects.incr()
            val trans = new Emulator(underlying, protocolFactory, buf)
            transportP.setValue(trans)
            trans.read()

          // We have a valid mux session, return the original
          // transport untouched.
          case Return(r) =>
            thriftmuxConnects.incr()
            transportP.setValue(underlying)
            Future.value(buf)

          case Throw(exc) =>
            val msg = s"Unable to determine the protocol: $exc"
            log.log(Level.DEBUG, msg)
            transportP.setValue(underlying)
            close().before {
              Future.exception(Failure(msg).withLogLevel(Level.DEBUG))
            }

        }
      }
  }

  private class Emulator(
    underlying: Transport[Buf, Buf],
    protocolFactory: TProtocolFactory,
    init: Buf
  ) extends TransportProxy[Buf, Buf](underlying) {
    // A boolean which indicates if we are speaking twitter upgraded thrift.
    private[this] val ttwitter: Boolean = {
      try {
        val buffer = new InputBuffer(Buf.ByteArray.Owned.extract(init), protocolFactory)
        val msg = buffer().readMessageBegin()
        msg.`type` == TMessageType.CALL &&
        msg.name == ThriftTracing.CanTraceMethodName
      } catch {
        case NonFatal(_) => false
      }
    }

    // An encoded header message for TTwitter thrift using `protocolFactory`.
    private[this] val ttwitterHeader =
      Buf.ByteArray.Owned(OutputBuffer.messageToArray(new ResponseHeader, protocolFactory))

    // An encoded ack message for TTwitter thrift using `protocolFactory`.
    private[this] val ttwitterAck: Buf = {
      val buffer = new OutputBuffer(protocolFactory)
      buffer().writeMessageBegin(
        new TMessage(ThriftTracing.CanTraceMethodName, TMessageType.REPLY, 0)
      )
      val upgradeReply = new UpgradeReply
      upgradeReply.write(buffer())
      buffer().writeMessageEnd()
      Buf.ByteArray.Shared(buffer.toArray)
    }

    // We proxy reads via a queue so that we can synthesize incoming messages.
    private[this] val readq = new AsyncQueue[Buf]
    private[this] def readLoop(): Future[Unit] =
      underlying.read().flatMap(processRead)
    private[this] val processRead: Buf => Future[Unit] =
      buf => {
        readq.offer(Message.encode(thriftToMux(ttwitter, protocolFactory, buf)))
        readLoop()
      }

    if (ttwitter) {
      // write the TTwitter ack
      underlying.write(ttwitterAck)
    } else {
      // we are speaking vanilla thrift, encode `init` as a mux dispatch.
      readq.offer(Message.encode(thriftToMux(ttwitter, protocolFactory, init)))
    }

    // kick off readLoop and propagate failure.
    readLoop().onFailure { exc =>
      readq.fail(exc)
    }

    def write(buf: Buf): Future[Unit] = writeMuxToThrift(buf)
    def read(): Future[Buf] = readq.poll()

    /**
     * Lowers a mux message into a Thrift message where possible and writes
     * the result to `underlying`.
     */
    private[this] def writeMuxToThrift(buf: Buf): Future[Unit] =
      Message.decode(buf) match {
        case Message.RdispatchOk(_, _, rep) if ttwitter =>
          underlying.write(ttwitterHeader.concat(rep))

        case Message.RdispatchOk(_, _, rep) =>
          underlying.write(rep)

        case Message.RdispatchNack(_, _) =>
          // The only mechanism for negative acknowledgement afforded by non-Mux
          // clients is to tear down the connection.
          close()
          Future.Done

        case Message.Tdrain(tag) =>
          // Although downgraded connections don't understand Tdrains,
          // we synthesize an Rdrain so the server dispatcher enters draining
          // mode.
          readq.offer(Message.encode(Message.Rdrain(tag)))
          Future.Done

        case unexpected =>
          // we can't write this, so we signal failure to the remote
          // by tearing down the session.
          close()
          // log here to surface the error
          val msg = s"unable to write ${unexpected.getClass.getName} to non-mux client"
          log.log(Level.DEBUG, msg)
          // return a failure to the level above us.
          Future.exception(Failure(msg).withLogLevel(Level.DEBUG))
      }
  }

  /**
   * Returns a Mux.Tdispatch from a thrift dispatch message.
   */
  def thriftToMux(ttwitter: Boolean, protocolFactory: TProtocolFactory, buf: Buf): Message.Tdispatch = {
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

      contextBuf += (Trace.idCtx.marshalId -> Trace.idCtx.marshal(richHeader.traceId))

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
