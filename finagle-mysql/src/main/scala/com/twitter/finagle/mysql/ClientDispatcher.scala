package com.twitter.finagle.mysql

import com.github.benmanes.caffeine.cache.{Caffeine, RemovalCause, RemovalListener}
import com.twitter.cache.caffeine.CaffeineCache
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.dispatch.ClientDispatcher.wrapWriteException
import com.twitter.finagle.mysql.LostSyncException.const
import com.twitter.finagle.mysql.param.{MaxConcurrentPrepareStatements, UnsignedColumns}
import com.twitter.finagle.mysql.transport.{MysqlBuf, MysqlBufReader, Packet}
import com.twitter.finagle.param.Stats
import com.twitter.finagle.stats.{Counter, LazyStatsReceiver, StatsReceiver}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Service, ServiceProxy, Stack}
import com.twitter.util._

/**
 * A catch-all exception class for errors returned from the upstream
 * MySQL server.
 */
case class ServerError(code: Short, sqlState: String, message: String) extends Exception(message)

/**
 * Caches statements that have been successfully prepared over the connection
 * managed by the underlying service (a ClientDispatcher). This decreases
 * the chances of leaking prepared statements and can simplify the
 * implementation of prepared statements in the presence of a connection pool.
 */
private[mysql] class PrepareCache(
  svc: Service[Request, Result],
  cache: Caffeine[Object, Object],
  statsReceiver: StatsReceiver)
    extends ServiceProxy[Request, Result](svc) {

  private[this] val scopedStatsReceiver = new LazyStatsReceiver(
    statsReceiver.scope("pstmt-cache")
  )

  private[this] val evictionCounters = {
    val counters = new Array[Counter](RemovalCause.values().length)
    for (value <- RemovalCause.values()) {
      counters(value.ordinal()) =
        scopedStatsReceiver.counter(s"evicted_${value.name().toLowerCase}")
    }
    counters
  }
  private[this] val callCounter = scopedStatsReceiver.counter("calls")
  private[this] val missCounter = scopedStatsReceiver.counter("misses")

  private[this] val fn = {
    val listener = new RemovalListener[Request, Future[Result]] {
      // make sure prepared futures get removed eventually
      def onRemoval(request: Request, response: Future[Result], cause: RemovalCause): Unit = {
        evictionCounters(cause.ordinal()).incr()
        response.respond {
          case Return(r: PrepareOK) =>
            svc(CloseRequest(r.id)).unit
          case _ =>
        }
      }
    }

    val underlying = cache
      .removalListener(listener)
      .build[Request, Future[Result]]()

    CaffeineCache.fromCache(
      fn = { req: Request =>
        missCounter.incr()
        svc(req)
      },
      cache = underlying
    )
  }

  /**
   * Populate cache with unique prepare requests identified by their
   * sql queries.
   */
  override def apply(req: Request): Future[Result] = req match {
    case _: PrepareRequest =>
      callCounter.incr()
      fn(req)
    case _ => super.apply(req)
  }
}

private[finagle] object ClientDispatcher {
  private val emptyTx = (Nil, EOF(0: Short, ServerStatus(0)))

  /**
   * Creates a mysql client dispatcher with write-through caches for optimization.
   * @param trans A transport that reads a writes logical mysql packets.
   * @param params A collection of `Stack.Params` useful for configuring a mysql client.
   */
  def apply(trans: Transport[Packet, Packet], params: Stack.Params): Service[Request, Result] = {
    val maxConcurrentPrepareStatements = params[MaxConcurrentPrepareStatements].num
    new PrepareCache(
      svc = new ClientDispatcher(trans, params),
      cache = Caffeine.newBuilder().maximumSize(maxConcurrentPrepareStatements),
      statsReceiver = params[Stats].statsReceiver
    )
  }

}

/**
 * A ClientDispatcher that implements the mysql client/server protocol.
 * For a detailed exposition of the client/server protocol refer to:
 * [[https://dev.mysql.com/doc/internals/en/client-server-protocol.html]]
 *
 * Note, the mysql protocol does not support any form of multiplexing so
 * requests are dispatched serially and concurrent requests are queued.
 */
private[finagle] final class ClientDispatcher(
  trans: Transport[Packet, Packet],
  params: Stack.Params)
    extends GenSerialClientDispatcher[Request, Result, Packet, Packet](
      trans,
      params[Stats].statsReceiver) {
  import ClientDispatcher._

  private[this] val supportUnsigned: Boolean = params[UnsignedColumns].supported

  override def apply(req: Request): Future[Result] =
    super.apply(req).onFailure {
      // a LostSyncException represents a fatal state between
      // the client / server. The error is unrecoverable
      // so we close the service.
      case e @ LostSyncException(_) => close()
      case _ =>
    }

  override def close(deadline: Time): Future[Unit] = trans.close()

  /**
   * Returns a Future that represents the result of an exchange
   * between the client and server. An exchange does not necessarily entail
   * a single write and read operation. Thus, the result promise
   * is decoupled from the promise that signals a complete exchange.
   * This leaves room for implementing streaming results.
   */
  protected def dispatch(req: Request, rep: Promise[Result]): Future[Unit] =
    trans.write(req.toPacket).transform {
      case Throw(exc) => wrapWriteException(exc)
      case Return(_) =>
        val signal = new Promise[Unit]
        if (req.cmd == Command.COM_STMT_CLOSE) {
          // synthesize COM_STMT_CLOSE response
          signal.setDone()
          rep.updateIfEmpty(Return(CloseStatementOK))
          signal
        } else
          trans.read().flatMap { packet =>
            rep.become(decodePacket(req, packet, req.cmd, signal))
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
    req: Request,
    packet: Packet,
    cmd: Byte,
    signal: Promise[Unit]
  ): Future[Result] = {
    MysqlBuf.peek(packet.body) match {
      case Some(Packet.OkByte) if cmd == Command.COM_STMT_FETCH =>
        // Not really an OK packet; 00 is the header for a row as well
        readTx(req)
          .flatMap {
            case (rowPackets, eof) =>
              const(FetchResult(packet +: rowPackets, eof))
          }.ensure {
            signal.setDone()
          }

      case Some(Packet.EofByte) if cmd == Command.COM_STMT_FETCH =>
        // synthesize an empty FetchResult
        signal.setDone()

        const(EOF(packet).flatMap { eof => FetchResult(Nil, eof) })

      case Some(Packet.OkByte) if cmd == Command.COM_STMT_PREPARE =>
        // decode PrepareOk Result: A header packet potentially followed
        // by two transmissions that contain parameter and column
        // information, respectively.
        val result = for {
          ok <- const(PrepareOK(packet))
          (seq1, _) <- readTx(req, ok.numOfParams)
          (seq2, _) <- readTx(req, ok.numOfCols)
          ps <- Future.collect(seq1.map { p => const(Field(p)) })
          cs <- Future.collect(seq2.map { p => const(Field(p)) })
        } yield {
          ok.copy(params = ps, columns = cs)
        }
        result.ensure { signal.setDone() }

      // decode OK Result
      case Some(Packet.OkByte) =>
        signal.setDone()
        const(OK(packet))

      // decode Error result
      case Some(Packet.ErrorByte) =>
        signal.setDone()
        const(Error(packet)).flatMap { err => Future.exception(errorToServerError(req, err)) }

      case Some(byte) =>
        val isBinaryEncoded = cmd != Command.COM_QUERY
        val numCols = Try {
          val br = MysqlBufReader(packet.body)
          br.readVariableLong().toInt
        }

        val result = const(numCols).flatMap { cnt =>
          readTx(req, cnt).flatMap {
            case (fields, eof) =>
              if (eof.serverStatus.has(ServerStatus.CursorExists)) {
                const(ResultSetBuilder(isBinaryEncoded, supportUnsigned)(packet, fields, Seq()))
              } else {
                readTx(req).flatMap {
                  case (rows, _) =>
                    const(ResultSetBuilder(isBinaryEncoded, supportUnsigned)(packet, fields, rows))
                }
              }
          }
        }

        // TODO: When streaming is implemented the done signal should be
        // dependent on the completion of the stream.
        result.ensure {
          signal.setDone()
        }

      case _ =>
        signal.setDone()
        LostSyncException.AsFuture
    }
  }

  private[this] def errorToServerError(req: Request, err: Error): ServerError = {
    val msg = req match {
      case ws: WithSql => s"${err.message}, Executed SQL='${ws.sql}'"
      case _ => err.message
    }
    ServerError(err.code, err.sqlState, msg)
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
  private[this] def readTx(req: Request, limit: Int = Int.MaxValue): Future[(Seq[Packet], EOF)] = {
    def aux(numRead: Int, xs: List[Packet]): Future[(List[Packet], EOF)] = {
      if (numRead > limit) LostSyncException.AsFuture
      else
        trans.read().flatMap { packet =>
          MysqlBuf.peek(packet.body) match {
            case Some(Packet.EofByte) =>
              const(EOF(packet)).map { eof => (xs.reverse, eof) }
            case Some(Packet.ErrorByte) =>
              const(Error(packet)).flatMap { err => Future.exception(errorToServerError(req, err)) }
            case Some(_) => aux(numRead + 1, packet :: xs)
            case None => LostSyncException.AsFuture
          }
        }
    }

    if (limit <= 0) Future.value(emptyTx)
    else aux(0, Nil)
  }
}
