package com.twitter.finagle.mysql

import com.twitter.concurrent.AsyncStream
import com.twitter.finagle.Service
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.{Closable, Future, Time}

/**
 * A closable async stream of projected rows from a CursoredStatement.
 */
trait CursorResult[T] extends Closable {
  /**
   * Initiate the streaming result set.
   *
   * @note once `stream` has been called it is critical to either
   * consume the stream to the end or explicitly call `close()` to
   * avoid resource leaking.
   */
  def stream: AsyncStream[T]
}

/**
 * A CursoredStatement represents a parameterized
 * sql statement which when applied to parameters yields
 * a lazy stream of rows. Can be used concurrently.
 */
trait CursoredStatement {
  /**
   * Executes the cursored statement with the given `params` and lazily maps `f`
   * over the rows as they are streamed from the database.
   *
   * @note `rowsPerFetch` should be carefully picked to balance the minimum number of round
   * trips to the database, and the maximum amount of memory used by an individual fetch. For
   * example, consider estimating the whole result size with a `select count(...)` first,
   * and then setting `rowsPerFetch` to `Math.log(count)`.
   */
  def apply[T](rowsPerFetch: Int, params: Parameter*)(f: Row => T): Future[CursorResult[T]]
}

private object StdCursorResult {
  val logger = Logger(getClass.getName)
  val CursorClosedException = new Exception(
    "request attempted against already closed cursor")
}

private class StdCursorResult[T](
    stats: CursorStats,
    svc: Service[Request, Result],
    sql: String,
    rowsPerFetch: Int,
    params: Seq[Parameter],
    f: (Row) => T)
  extends CursorResult[T] { self =>
  import StdCursorResult._

  // We store the stream state outside of an AsyncStream instance to avoid storing the
  // head of the stream as a member field. That is, each operation on the `stream`
  // inside `StdCursorResult` will construct a new AsyncStream (e.g. no vals). This is
  // important to avoid OOMing during large or infinite streams.
  sealed trait StreamState
  case object Init extends StreamState
  case object Closed extends StreamState
  case class Preparing(s: AsyncStream[T]) extends StreamState
  case class Prepared(ok: PrepareOK) extends StreamState
  case class Fetching(fs: () => AsyncStream[T]) extends StreamState

  // Thread safety is provided by synchronization on `this`. The assumption is that it's
  // okay to use a coarse grained lock for to manage state since operations on the
  // stream should have no (or low) concurrency in the common case.
  private[this] var state: StreamState = Init

  private[this] val closeFn: Throwable => Unit = _ => close()
  private[this] def invoke(req: Request): Future[Result] = self.synchronized {
    state match {
      case Closed => Future.exception(CursorClosedException)
      case _ => svc(req).onFailure(closeFn)
    }
  }

  private[this] def prepare(): AsyncStream[Result] =
    AsyncStream.fromFuture(invoke(PrepareRequest(sql)))

  private[this] def execute(ok: PrepareOK): AsyncStream[Result] = {
    val execReq = new ExecuteRequest(
      stmtId = ok.id,
      params = params.toIndexedSeq,
      hasNewParams = true,
      flags = ExecuteRequest.FLAG_CURSOR_READ_ONLY)
    AsyncStream.fromFuture(invoke(execReq))
  }

  private[this] def fetch(ok: PrepareOK): () => AsyncStream[T] = {
    val columns = ok.columns.toIndexedSeq
    val indexMap = columns.map(_.id).zipWithIndex.toMap
    val fetchRequest = new FetchRequest(ok, rowsPerFetch)
    def go(): AsyncStream[T] = {
      stats.fetchStarted()
      AsyncStream.fromFuture(invoke(fetchRequest)).flatMap { result =>
        stats.fetchFinished()
        result match {
          case fetchResult: FetchResult =>
            // This is somewhat awkward reaching across the abstraction
            // of Results to touching Packets, but there are future
            // refactorings that can help clean this up.
            val rows = fetchResult.rowPackets.map { p =>
              new BinaryEncodedRow(p.body, columns, indexMap)
            }
            val asyncSeq = AsyncStream.fromSeq(rows.map(f))
            if (!fetchResult.containsLastRow) asyncSeq ++ go() else {
              AsyncStream.fromFuture(close()).flatMap(_ => asyncSeq)
            }
          case r => closeAndLog(s"unexpected reply $r when fetching an element.")
        }
      }
    }
    go
  }

  override def stream: AsyncStream[T] = self.synchronized {
    state match {
      case Preparing(s) => s
      case Fetching(fs) => fs()
      case Closed => AsyncStream.empty
      case Init =>
        val s = prepare().flatMap {
          case ok: PrepareOK => self.synchronized { state = Prepared(ok) }; stream
          case r => closeAndLog(s"unexpected reply $r when preparing stream.")
        }
        // Although unlikely, we want to make sure we don't race
        // with the closure on `prepare`.
        if (state == Init) state = Preparing(s)
        s
      case prepared: Prepared =>
        val s = execute(prepared.ok).flatMap {
          case _: ResultSet =>
            stats.streamStarted()
            val fs = fetch(prepared.ok)
            self.synchronized { state = Fetching(fs) }
            fs()
          case r => closeAndLog(s"unexpected reply $r when executing stream.")
        }
        // Although unlikely, we want to make sure we don't race
        // with the closure on `execute`.
        if (state == prepared) state = Preparing(s)
        s
    }
  }

  private[this] def closeAndLog(msg: String): AsyncStream[T] = {
    logger.error(msg)
    AsyncStream.fromFuture(close()).flatMap(_ => AsyncStream.empty)
  }

  override def close(deadline: Time): Future[Unit] = self.synchronized {
    state match {
      case Closed => Future.Unit
      case _ =>
        stats.streamFinished()
        state = Closed
        svc.close(deadline)
    }
  }
}

private class CursorStats(statsReceiver: StatsReceiver) {
  private[this] val sr = statsReceiver.scope("cursor")
  private[this] val timePerStreamMsStat = sr.stat("time_per_stream_ms")
  private[this] val timePerFetchMsStat = sr.stat("time_per_fetch_ms")
  private[this] val timeBetweenFetchMsStat = sr.stat("time_between_fetch_ms")
  private[this] val cursorsOpenedCounter = sr.counter("opened")
  private[this] val cursorsClosedCounter = sr.counter("closed")

  // used to export stats about the stream life-cycle
  @volatile private[this] var streamStartTime = Time.Bottom
  @volatile private[this] var fetchStartTime = Time.Bottom
  @volatile private[this] var lastFetchEndTime = Time.Bottom

  def streamStarted(): Unit = {
    cursorsOpenedCounter.incr()
    streamStartTime = Time.now
  }

  def streamFinished(): Unit = {
    cursorsClosedCounter.incr()
    timePerStreamMsStat.add((Time.now - streamStartTime).inMillis)
  }

  def fetchStarted(): Unit = {
    fetchStartTime = Time.now
  }

  def fetchFinished(): Time = {
    val fetchEndTime = Time.now
    timePerFetchMsStat.add((fetchEndTime - fetchStartTime).inMillis)
    if (lastFetchEndTime != Time.Bottom) {
      timeBetweenFetchMsStat.add((fetchStartTime - lastFetchEndTime).inMillis)
    }
    lastFetchEndTime = fetchEndTime
    fetchEndTime
  }
}