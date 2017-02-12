package com.twitter.finagle.mysql

import com.github.benmanes.caffeine.cache.{Caffeine, RemovalCause, RemovalListener}
import com.twitter.cache.caffeine.CaffeineCache
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.mysql.transport.{MysqlBufReader, MysqlBuf, Packet}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{CancelledRequestException, Service, ServiceProxy, WriteException}
import com.twitter.util.{Closable, Future, Promise, Return, Throw, Time, Try}

/**
 * A catch-all exception class for errors returned from the upstream
 * MySQL server.
 */
case class ServerError(code: Short, sqlState: String, message: String)
  extends Exception(message)

case class LostSyncException(underlying: Throwable)
  extends RuntimeException(underlying) {
    override def getMessage = underlying.toString
    override def getStackTrace = underlying.getStackTrace
  }

/**
 * Caches statements that have been successfully prepared over the connection
 * managed by the underlying service (a ClientDispatcher). This decreases
 * the chances of leaking prepared statements and can simplify the
 * implementation of prepared statements in the presence of a connection pool.
 */
private[mysql] class PrepareCache(
  svc: Service[Request, Result],
  cache: Caffeine[Object, Object]
) extends ServiceProxy[Request, Result](svc) {

  def closable(num: Int): Closable = Closable.make { deadline: Time =>
    svc(CloseRequest(num)).unit
  }

  private[this] val fn = {
    val listener = new RemovalListener[Request, Future[Result]] {
      // make sure prepared futures get removed eventually
      def onRemoval(request: Request, response: Future[Result], cause: RemovalCause): Unit = {
        response.onSuccess {
          case r: PrepareOK => Closable.closeOnCollect(closable(r.id), r)
          case _ => // nop
        }
      }
    }

    val underlying = cache
      .removalListener(listener)
      .build[Request, Future[Result]]()

    CaffeineCache.fromCache(Service.mk { req: Request => svc(req) }, underlying)
  }

  /**
   * Populate cache with unique prepare requests identified by their
   * sql queries.
   */
  override def apply(req: Request): Future[Result] = req match {
    case _: PrepareRequest => fn(req)
    case _ => super.apply(req)
  }
}

object ClientDispatcher {
  private val cancelledRequestExc = new CancelledRequestException
  private val lostSyncExc = new LostSyncException(new Throwable)
  private val emptyTx = (Nil, EOF(0: Short, ServerStatus(0)))
  private val wrapWriteException: PartialFunction[Throwable, Future[Nothing]] = {
    case exc: Throwable => Future.exception(WriteException(exc))
  }

  /**
   * Creates a mysql client dispatcher with write-through caches for optimization.
   * @param trans A transport that reads a writes logical mysql packets.
   * @param handshake A function that is responsible for facilitating
   * the connection phase given a HandshakeInit.
   * @param maxConcurrentPrepareStatements The maximum number of prepare
   * statements that the cache will keep track of.
   */
  def apply(
    trans: Transport[Packet, Packet],
    handshake: HandshakeInit => Try[HandshakeResponse],
    maxConcurrentPrepareStatements: Int
  ): Service[Request, Result] = {
    new PrepareCache(
      new ClientDispatcher(trans, handshake),
      Caffeine.newBuilder().maximumSize(maxConcurrentPrepareStatements)
    )
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
 */
class ClientDispatcher(
  trans: Transport[Packet, Packet],
  handshake: HandshakeInit => Try[HandshakeResponse]
) extends GenSerialClientDispatcher[Request, Result, Packet, Packet](trans) {
  import ClientDispatcher._

  override def apply(req: Request): Future[Result] =
    connPhase flatMap { _ =>
      super.apply(req)
    } onFailure {
      // a LostSyncException represents a fatal state between
      // the client / server. The error is unrecoverable
      // so we close the service.
      case e@LostSyncException(_) => close()
      case _ =>
    }

  override def close(deadline: Time): Future[Unit] =
    trans.write(QuitRequest.toPacket)
      .by(DefaultTimer.twitter, deadline).ensure(super.close(deadline))

  /**
   * Performs the connection phase. The phase should only be performed
   * once before any other exchange between the client/server. A failure
   * to handshake renders this service unusable.
   * [[http://dev.mysql.com/doc/internals/en/connection-phase.html]]
   */
  private[this] val connPhase: Future[Result] =
    trans.read() flatMap { packet =>
      const(HandshakeInit(packet)) flatMap { init =>
        const(handshake(init)) flatMap { req =>
          val rep = new Promise[Result]
          dispatch(req, rep)
          rep
        }
      }
    } onFailure { _ => close() }

  /**
   * Returns a Future that represents the result of an exchange
   * between the client and server. An exchange does not necessarily entail
   * a single write and read operation. Thus, the result promise
   * is decoupled from the promise that signals a complete exchange.
   * This leaves room for implementing streaming results.
   */
  protected def dispatch(req: Request, rep: Promise[Result]): Future[Unit] =
    trans.write(req.toPacket) rescue {
      wrapWriteException
    } before {
      val signal = new Promise[Unit]
      if (req.cmd == Command.COM_STMT_CLOSE) {
        // synthesize COM_STMT_CLOSE response
        signal.setDone()
        rep.updateIfEmpty(Return(CloseStatementOK))
        signal
      } else trans.read() flatMap { packet =>
        rep.become(decodePacket(packet, req.cmd, signal))
        signal
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
  ): Future[Result] = {
    MysqlBuf.peek(packet.body) match {
      case Some(Packet.OkByte) if cmd == Command.COM_STMT_FETCH =>
        // Not really an OK packet; 00 is the header for a row as well
        readTx().flatMap {
          case (rowPackets, eof) =>
            const(FetchResult(packet +: rowPackets, eof))
        } ensure signal.setDone()

      case Some(Packet.EofByte) if cmd == Command.COM_STMT_FETCH =>
        // synthesize an empty FetchResult
        signal.setDone()

        const(EOF(packet) flatMap (FetchResult(Seq(), _)))

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

    case Some(byte) =>
      val isBinaryEncoded = cmd != Command.COM_QUERY
      val numCols = Try {
        val br = new MysqlBufReader(packet.body)
        br.readVariableLong().toInt
      }

      val result = const(numCols).flatMap { cnt =>
        readTx(cnt).flatMap {
          case (fields, eof) =>
            if (eof.serverStatus.has(ServerStatus.CursorExists)) {
              const(ResultSet(isBinaryEncoded)(packet, fields, Seq()))
            } else {
              readTx().flatMap {
                case (rows, _) =>
                  const(ResultSet(isBinaryEncoded)(packet, fields, rows))
              }
            }
        }
      }

      // TODO: When streaming is implemented the
      // done signal should dependent on the
      // completion of the stream.
      result ensure signal.setDone()

      case _ =>
        signal.setDone()
        Future.exception(lostSyncExc)
    }
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
        MysqlBuf.peek(packet.body) match {
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
}
