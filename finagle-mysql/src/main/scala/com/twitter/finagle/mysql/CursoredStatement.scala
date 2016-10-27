package com.twitter.finagle.mysql

import com.twitter.concurrent.AsyncStream
import com.twitter.finagle.Service
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{Closable, Future, Time}
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A closable async stream of projected rows from a
 * CursoredStatement.
 *
 * @note once `stream` has been called it is critical to either
 * consume the stream to the end or explicitly call `close()` to
 * avoid resource leaking
 */
trait CursorResult[T] extends Closable {
  /**
   * Initiate the streaming result set. See above note.
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

object CursorClosedException extends Exception("request attempted against already closed cursor")

private class StdCursorResult[T](
  stats: CursorStats,
  svc: Service[Request, Result],
  sql: String,
  rowsPerFetch: Int,
  params: Seq[Parameter],
  f: (Row) => T
) extends CursorResult[T] {

  @volatile
  private[this] var lastFetchEndTime = Time.Bottom
  private[this] val startTime = Time.now
  private[this] val cursorLive = new AtomicBoolean(true)

  override lazy val stream: AsyncStream[T] = {
    AsyncStream.fromFuture(invoke(PrepareRequest(sql))).flatMap {
      case prepareOk: PrepareOK =>
        val columns = prepareOk.columns.toIndexedSeq
        val indexMap = columns.map(_.id).zipWithIndex.toMap
        val fetchRequest = new FetchRequest(prepareOk, rowsPerFetch)

        val executeRequest = new ExecuteRequest(
          prepareOk.id,
          params.toIndexedSeq,
          flags = ExecuteRequest.FLAG_CURSOR_READ_ONLY,
          hasNewParams = true
        )

        AsyncStream.fromFuture(invoke(executeRequest)).flatMap { _ =>
          stats.streamStarted()

          streamFromFetches(fetchRequest, columns, indexMap, f)
        }
      case _ =>
        AsyncStream.fromFuture(close()).flatMap(_ => AsyncStream.empty)
    }
  }

  override def close(deadline: Time): Future[Unit] = {
    if (cursorLive.getAndSet(false)) {
      stats.streamFinished(startTime)
      svc.close(deadline)
    } else Future.Unit
  }

  private def streamFromFetches(
    fetchRequest: FetchRequest,
    columns: IndexedSeq[Field],
    indexMap: Map[String, Int],
    f: (Row) => T
  ): AsyncStream[T] = {
    def inner(): AsyncStream[T] = {
      val fetchStartTime = Time.now

      AsyncStream.fromFuture(invoke(fetchRequest)).flatMap { result =>
        lastFetchEndTime = stats.fetchFinished(fetchStartTime, lastFetchEndTime)

        result match {
          case fetchResult: FetchResult =>
            // This is somewhat awkward reaching across the abstraction
            // of Results to touching Packets, but there are future
            // refactorings that can help clean this up
            val rows = fetchResult.rowPackets.map { p =>
              new BinaryEncodedRow(p.body, columns, indexMap)
            }

            val asyncSeq = AsyncStream.fromSeq(rows.map(f))

            if (fetchResult.containsLastRow) {
              AsyncStream.fromFuture(close()).flatMap(_ => asyncSeq)
            }
            else
              asyncSeq ++ inner()

          case r =>
            AsyncStream.fromFuture(close()).flatMap(_ => AsyncStream.empty)

        }
      }
    }

    inner()
  }

  private def invoke(req: Request): Future[Result] = {
    if (cursorLive.get())
      svc(req).onFailure(_ => close())
    else
      Future.exception(LostSyncException(CursorClosedException))
  }
}

private class CursorStats(statsReceiver: StatsReceiver) {
  private[this] val timePerStreamMsStat = statsReceiver.scope("cursor").stat("time_per_stream_ms")
  private[this] val timePerFetchMsStat = statsReceiver.scope("cursor").stat("time_per_fetch_ms")
  private[this] val timeBetweenFetchMsStat = statsReceiver.scope("cursor").stat("time_between_fetch_ms")
  private[this] val cursorsOpenedCounter = statsReceiver.scope("cursor").counter("opened")
  private[this] val cursorsClosedCounter = statsReceiver.scope("cursor").counter("closed")

  def streamStarted() : Unit = cursorsOpenedCounter.incr()

  def streamFinished(startTime: Time) : Unit = {
    cursorsClosedCounter.incr()
    timePerStreamMsStat.add((Time.now - startTime).inMillis)
  }

  def fetchFinished(startTime: Time, lastFetchEndTime: Time) : Time = {
    val fetchEndTime = Time.now
    timePerFetchMsStat.add((fetchEndTime - startTime).inMillis)
    if (lastFetchEndTime != Time.Bottom) {
      timeBetweenFetchMsStat.add((startTime - lastFetchEndTime).inMillis)
    }
    fetchEndTime
  }
}
