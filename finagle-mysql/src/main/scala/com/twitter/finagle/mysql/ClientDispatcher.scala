package com.twitter.finagle.exp.mysql

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.exp.mysql.transport.{Buffer, BufferReader, Packet}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{CancelledRequestException, Service, WriteException}
import com.twitter.util.{Future, Promise, Time, Try, Throw, Return}
import java.util.logging.Logger

case class ServerError(code: Short, sqlState: String, message: String)
  extends Exception(message)

case class LostSyncException(underlying: Throwable)
  extends RuntimeException(underlying) {
    override def getMessage = underlying.getMessage
    override def getStackTrace = underlying.getStackTrace
  }

object ClientDispatcher {
  private val cancelledRequestExc = new CancelledRequestException
  private val lostSyncExc = new LostSyncException(new Throwable)
  private val emptyTx = (Nil, EOF(0: Short, 0: Short))
  private val wrapWriteException: PartialFunction[Throwable, Future[Nothing]] = {
    case exc: Throwable => Future.exception(WriteException(exc))
  }

  /**
   * Wrap a Try[T] into a Future[T]. This is useful for
   * transforming decoded results into futures. Any Throw
   * is assumed to be a failure to decode and thus a synchronization
   * error (or corrupt data) between the client and server.
   */
  private def const[T](result: Try[T]): Future[T] =
    Future.const(result rescue { case exc => Throw(LostSyncException(exc)) })
}

/**
 * A ClientDispatcher that implements the mysql client/server protocol.
 * For a detailed exposition of the client/server protocol refer to:
 * [[http://dev.mysql.com/doc/internals/en/client-server-protocol.html]]
 *
 * Note, the mysql protocol does not support any form of multiplexing so
 * requests are dispatched serially and concurrent requests are queued.
 *
 * @param trans A transport that reads a writes logical mysql packets.
 * @param handshake A function that is responsible for facilitating
 * the connection phase given a HandshakeInit.
 */
class ClientDispatcher(
  trans: Transport[Packet, Packet],
  handshake: HandshakeInit => Try[HandshakeResponse]
) extends Service[Request, Result] {
  import ClientDispatcher._
  private[this] val dispatchq = new AsyncQueue[(Request, Promise[Result])]

  override def apply(req: Request): Future[Result] = {
    val rep = new Promise[Result]
    dispatchq.offer((req, rep))
    rep
  }

  /**
   * Dispatches queued requests sequentially and satisfies the
   * response promise.
   */
  private[this] def loop(): Future[Unit] = {
    dispatchq.poll() flatMap { case (req, rep) =>
      rep.isInterrupted match {
        case Some(_) =>
          rep.setException(cancelledRequestExc)
          loop()
        case None =>
          val signal = new Promise[Unit]
          val result = connPhase.unit before dispatch(req, signal)
          result respond rep.updateIfEmpty
          signal ensure loop()
      }
    }
  }
  loop()

  /**
   * Performs the connection phase. The phase
   * should only be performed once before any
   * other exchange between the client/server.
   * [[http://dev.mysql.com/doc/internals/en/connection-phase.html]]
   */
  private[this] val connPhase: Future[Result] =
    trans.read() flatMap { packet =>
      const(HandshakeInit(packet)) flatMap { init =>
        const(handshake(init)) flatMap { req =>
          dispatch(req, new Promise[Unit])
        }
      }
    }

  /**
   * Returns a Future that represents the result of an exchange
   * between the client and server. An exchange does not necessarily entail
   * a single write and read operation. Thus, the result promise
   * is decoupled from the promise that signals a complete exchange.
   * This leaves room for implementing streaming results.
   */
  private[this] def dispatch(req: Request, signal: Promise[Unit]): Future[Result] =
    trans.write(req.toPacket) rescue {
      wrapWriteException
    } before {
      // synthesize COM_STMT_CLOSE response
      if (req.cmd == Command.COM_STMT_CLOSE) {
        signal.setDone()
        Future.value(CloseStatementOK)
      } else trans.read() flatMap { packet =>
        decodePacket(packet, req.cmd, signal)
      }
    }

  /**
   * Returns a Future[Result] representing the decoded
   * packet. Some packets represent the start of a longer
   * transmission. These packets are distinguished by
   * the command used to generate the transmission.
   *
   * @param packet The first packet in the result.
   * @param cmd The command byte used to generate the packet.
   * @param signal A future used to signal completion. When this
   * future is satisfied, subsequent requests can be dispatched.
   */
  private[this] def decodePacket(
    packet: Packet,
    cmd: Byte,
    signal: Promise[Unit]
  ): Future[Result] = packet.body.headOption match {
    case Some(Packet.OkByte) if cmd == Command.COM_STMT_PREPARE =>
      // decode PrepareOk Result: A header packet potentially followed
      // by two transmissions that contain parameter and column
      // information, respectively.
      val result = for {
        ok <- const(PrepareOK(packet))
        (seq1, _) <- readTx(ok.numOfParams)
        (seq2, _) <- readTx(ok.numOfCols)
        ps <- Future.collect(seq1 map { p => const(Field(p)) })
        cs <- Future.collect(seq2 map { p => const(Field(p)) })
      } yield ok.copy(params = ps, columns = cs)

      result ensure signal.setDone()

    // decode OK Result
    case Some(Packet.OkByte)  =>
      signal.setDone()
      const(OK(packet))

    // decode Error result
    case Some(Packet.ErrorByte) =>
      signal.setDone()
      const(Error(packet)) flatMap { err =>
        val Error(code, state, msg) = err
        Future.exception(ServerError(code, state, msg))
      }

    // decode ResultSet
    case Some(byte) =>
      val isBinaryEncoded = cmd != Command.COM_QUERY
      val numCols = Try {
        val br = BufferReader(packet.body)
        br.readLengthCodedBinary().toInt
      }

      val result = for {
        cnt <- const(numCols)
        (fields, _) <- readTx(cnt)
        (rows, _) <- readTx()
        res <- const(ResultSet(isBinaryEncoded)(packet, fields, rows))
      } yield res

      // TODO: When streaming is implemented the
      // done signal should dependent on the
      // completion of the stream.
      result ensure signal.setDone()

    case _ =>
      signal.setDone()
      Future.exception(lostSyncExc)
  }

  /**
   * Reads a transmission from the transport that is terminated by
   * an EOF packet.
   *
   * TODO: This result should be streaming via some construct
   * that allows the consumer to exert backpressure.
   *
   * @param limit An upper bound on the number of reads. If the
   * number of reads exceeds the limit before an EOF packet is reached
   * a Future encoded LostSyncException is returned.
   */
  private[this] def readTx(limit: Int = Int.MaxValue): Future[(Seq[Packet], EOF)] = {
    def aux(numRead: Int, xs: List[Packet]): Future[(List[Packet], EOF)] = {
      if (numRead > limit) Future.exception(lostSyncExc)
      else trans.read() flatMap { packet =>
        packet.body.headOption match {
          case Some(Packet.EofByte) =>
            const(EOF(packet)) map { eof =>
              (xs.reverse, eof)
            }
          case Some(Packet.ErrorByte) =>
            const(Error(packet)) flatMap { err =>
              val Error(code, state, msg) = err
              Future.exception(ServerError(code, state, msg))
            }
          case Some(_) => aux(numRead + 1, packet :: xs)
          case None => Future.exception(lostSyncExc)
        }
      }
    }

    if (limit <= 0) Future.value(emptyTx)
    else aux(0, Nil)
  }

  override def isAvailable() = trans.isOpen
  override def close(deadline: Time) = trans.close()
}