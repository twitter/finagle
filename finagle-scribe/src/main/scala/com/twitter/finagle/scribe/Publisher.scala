package com.twitter.finagle.scribe

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.service._
import com.twitter.finagle.stats.{DefaultStatsReceiver, DenylistStatsReceiver, StatsReceiver}
import com.twitter.finagle.thrift.scribe.thriftscala.Scribe.Log
import com.twitter.finagle.thrift.scribe.thriftscala.{LogEntry, ResultCode, Scribe}
import com.twitter.finagle.tracing.{NullTracer, TracelessFilter}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{Filter, Service, Thrift}
import com.twitter.io.Buf
import com.twitter.util.{Closable, Future, Return, Throw, Time, Try}
import java.nio.charset.{StandardCharsets => JChar}
import java.util.concurrent.TimeUnit
import scala.util.control.NonFatal

object Publisher {
  private val DefaultDest = s"inet!localhost:1463"
  private val NewLineUtf8Buf: Buf = Buf.Utf8(System.getProperty("line.separator"))

  private def recordWithNewline(record: Array[Byte]): String = {
    val recordBuf = Buf.ByteArray.Shared(record)
    // add the trailing '\n'
    val concatenated = recordBuf.concat(NewLineUtf8Buf)
    Buf.decodeString(concatenated, JChar.UTF_8)
  }

  // only report these Finagle metrics (including counters for individual exceptions)
  private[this] def filteredStatsReceiver(statsReceiver: StatsReceiver): StatsReceiver = {
    new DenylistStatsReceiver(
      statsReceiver,
      {
        // MethodBuilder StatsFilter & RetryFilter
        case Seq(_, "logical", _*) => false
        case Seq(_, "retries", _*) => false

        // Basic Finagle stats
        case Seq(_, "requests") => false
        case Seq(_, "success") => false
        case Seq(_, "pending") => false
        case Seq(_, "failures", _*) => false

        case _ => true
      })
  }

  // We must use a ResponseClassifier as the Finagle Thrift client deals with raw
  // bytes and not the Scrooge generated types.
  private[finagle] val DefaultResponseClassifier: ResponseClassifier = {
    case ReqRep(_, Return(ResultCode.TryLater)) => ResponseClass.RetryableFailure
    case ReqRep(_, Return(_: ResultCode.EnumUnknownResultCode)) => ResponseClass.NonRetryableFailure
  }

  private[this] val ShouldRetry: PartialFunction[
    (Log.Args, Try[Log.SuccessType]),
    Boolean
  ] = {
    // We don't retry failures that the RequeueFilter will handle
    case (_, Throw(RequeueFilter.Requeueable(_))) => false
    case tup if DefaultResponseClassifier.isDefinedAt(ReqRep(tup._1, tup._2)) =>
      DefaultResponseClassifier(ReqRep(tup._1, tup._2)) match {
        case ResponseClass.Failed(retryable) => retryable
        case _ => false
      }
  }

  private[this] val DefaultRetryPolicy: RetryPolicy[(Log.Args, Try[Log.SuccessType])] =
    RetryPolicy.tries(
      numTries = 3, // MethodBuilder default
      shouldRetry = ShouldRetry
    )

  /** A [[Builder]] with all defaults */
  def builder: Builder = new Builder()

  /** Build a new [[Publisher]] with a default [[Builder]] */
  def build(category: String, label: String): Publisher =
    builder.build(category, label)

  /**
   * Builder for a Scribe [[Publisher]]
   * ==Usage==
   * {{{
   *   val publisher: Publisher = Publisher.build("category", "label")
   * }}}
   *
   * Or to apply configuration:
   *
   * {{{
   *   val publisher: Publisher =
   *     Publisher.builder
   *       .withRetryPolicy(customPolicy)
   *       .withResponseClassifier(customClassifier)
   *       .build("category", "label")
   * }}}
   *
   * @param dest Resolvable destination of the Scribe host to which to write entries.
   * @param statsReceiver [[StatsReceiver]] for collections of success/failure/error metrics.
   * @param retryPolicy the Finagle client [[RetryPolicy]] for retrying failures.
   * @param responseClassifier how Finagle should classify responses.
   * @param filter a user provided Filter chain which is applied directly before writing the entries to Scribe.
   */
  class Builder private[scribe] (
    dest: String = DefaultDest,
    statsReceiver: StatsReceiver = DefaultStatsReceiver,
    retryPolicy: RetryPolicy[(Log.Args, Try[Log.SuccessType])] = DefaultRetryPolicy,
    responseClassifier: ResponseClassifier = DefaultResponseClassifier,
    filter: Filter[Log.Args, Log.SuccessType, Log.Args, Log.SuccessType] = Filter.identity,
    logServiceOverride: Option[Service[Log.Args, Log.SuccessType]] = None) {

    def withDest(dest: String): Builder = {
      new Builder(
        dest = dest,
        this.statsReceiver,
        this.retryPolicy,
        this.responseClassifier,
        this.filter,
        this.logServiceOverride)
    }

    def withStatsReceiver(statsReceiver: StatsReceiver): Builder = {
      new Builder(
        this.dest,
        statsReceiver = statsReceiver,
        this.retryPolicy,
        this.responseClassifier,
        this.filter,
        this.logServiceOverride)
    }

    def withRetryPolicy(retryPolicy: RetryPolicy[(Log.Args, Try[Log.SuccessType])]): Builder = {
      new Builder(
        this.dest,
        this.statsReceiver,
        retryPolicy = retryPolicy,
        this.responseClassifier,
        this.filter,
        this.logServiceOverride)
    }

    def withResponseClassifier(responseClassifier: ResponseClassifier): Builder = {
      new Builder(
        this.dest,
        this.statsReceiver,
        this.retryPolicy,
        responseClassifier = responseClassifier,
        this.filter,
        this.logServiceOverride)
    }

    /** APPEND (not replace) the given Filter to the current Filter */
    def withFilter(
      filter: Filter[Log.Args, Log.SuccessType, Log.Args, Log.SuccessType]
    ): Builder = {
      new Builder(
        this.dest,
        this.statsReceiver,
        this.retryPolicy,
        this.responseClassifier,
        this.filter.andThen(filter),
        this.logServiceOverride)
    }

    /* exposed for testing */
    private[scribe] def withLogServiceOverride(
      logServiceOverride: Option[Service[Log.Args, Log.SuccessType]]
    ): Builder = {
      new Builder(
        this.dest,
        this.statsReceiver,
        this.retryPolicy,
        this.responseClassifier,
        this.filter,
        logServiceOverride = logServiceOverride
      )
    }

    /**
     * Build a new Scribe [[Publisher]] from the given category and label.
     * @param category the Scribe category to which to publish entries.
     * @param label the client label used in metrics
     * @return a new [[Publisher]] configured from the current state this [[Builder]].
     */
    def build(category: String, label: String): Publisher = {
      val stats = new ScribeStats(statsReceiver.scope(label))
      val client = newClient(label, stats, this.statsReceiver.scope("clnt"))
      new Publisher(
        category = category,
        stats = stats,
        client = client
      )
    }

    private def newClient(
      label: String,
      stats: ScribeStats, // captures per-request Scribe stats (not logical)
      clientStatsReceiver: StatsReceiver // Finagle client stats -- filtered & will be use for logical and per-request
    ): Scribe.MethodPerEndpoint = {
      val statsReceiver = filteredStatsReceiver(clientStatsReceiver)
      val scopedStatsReceiver = statsReceiver.scope(label)
      val logicalStatsReceiver = scopedStatsReceiver.scope("logical")

      // share the RetryBudget between the retry and requeue filters
      val retryBudget = RetryBudget()
      val retryFilter = new RetryFilter[Log.Args, ResultCode](
        retryPolicy = this.retryPolicy,
        retryBudget = retryBudget,
        timer = DefaultTimer,
        statsReceiver = scopedStatsReceiver
      )

      val statsFilter = StatsFilter
        .typeAgnostic(
          logicalStatsReceiver, // client stats receiver, filtered and scoped to the label + logical, e.g., clnt/label/logical
          this.responseClassifier,
          StatsFilter.DefaultExceptions,
          TimeUnit.MILLISECONDS
        ).toFilter[Log.Args, Log.SuccessType]

      val servicePerEndpoint: Scribe.ServicePerEndpoint = this.logServiceOverride match {
        case Some(svc) =>
          Scribe.ServicePerEndpoint(log = svc)
        case _ =>
          Thrift.client
            .withRetryBudget(retryBudget)
            .withSessionPool.maxSize(5)
            .withSessionPool.maxWaiters(10000)
            .withSessionQualifier.noFailFast
            .withSessionQualifier.noFailureAccrual
            // Client stats receiver, filtered, will be scoped to label by Finagle, e.g., clnt/label/
            .withStatsReceiver(statsReceiver)
            // We disable Tracing for this client to prevent creating a recursive tracing storm.
            .withTracer(NullTracer)
            .withRequestTimeout(1.second) // each retry will have this timeout
            .servicePerEndpoint[Scribe.ServicePerEndpoint](this.dest, label)
      }

      val tracelessFilter = new TracelessFilter
      val scribeMetricsFilter = new ScribeMetricsFilter(stats)
      val filteredServicePerEndpoint = servicePerEndpoint.withLog(
        log = tracelessFilter
          .andThen(statsFilter)
          .andThen(retryFilter)
          // this is placed after the retry filter so
          // that we update stats on retried requests
          .andThen(scribeMetricsFilter)
          // user provided filter
          .andThen(this.filter)
          .andThen(servicePerEndpoint.log)
      )
      Thrift.Client.methodPerEndpoint(filteredServicePerEndpoint)
    }
  }
}

/**
 * Publishes entries to Scribe. Metrics are collected per-request and logically.
 *
 * Logical metrics:
 * {{{
 *   clnt/label/logical/requests
 *   clnt/label/logical/success
 *   clnt/label/logical/pending
 *   clnt/label/logical/request_latency_ms
 *   clnt/label/logical/failures
 *   clnt/label/logical/failures/com.twitter.finagle.ChannelWriteException
 *
 * }}}
 *
 * Per-request metrics:
 * {{{
 *   clnt/label/retries
 *   clnt/label/retries/budget_exhausted
 *   label/scribe/try_later
 *   label/scribe/ok
 *   label/scribe/error/com.twitter.finagle.ChannelWriteException
 * }}}
 *
 * @param category the Scribe category to which to publish entries.
 * @param stats the [[ScribeStats]] to use for recording errors.
 * @param client the [[Scribe.MethodPerEndpoint]] Finagle client for sending messages to Scribe.
 */
final class Publisher private[finagle] (
  category: String,
  stats: ScribeStats,
  client: Scribe.MethodPerEndpoint)
    extends Closable {
  import Publisher._

  /**
   * Write the given array of bytes to scribe. Bytes are UTF-8 encoded and appended with the
   * system line separator.
   * @param record byte array to write to scribe
   */
  def write(record: Array[Byte]): Future[Unit] =
    write(toLogEntry(record))

  /**
   * Write the given list of [[LogEntry]] items to scribe.
   * @param entries list of entries to write to scribe
   */
  def write(entries: Seq[LogEntry]): Future[Unit] =
    if (entries.nonEmpty) {
      client
        .log(entries)
        .unit
    } else Future.Done

  /** Proxy to handle errors */
  def handleError(e: Throwable): Unit = stats.handleError(e)

  /**
   * Close the resource with the given deadline. This deadline is advisory,
   * giving the callee some leeway, for example to drain clients or finish
   * up other tasks.
   */
  override def close(deadline: Time): Future[Unit] =
    client.asClosable.close(deadline)

  private[this] def toLogEntry(record: Array[Byte]): Seq[LogEntry] = {
    try {
      Seq(LogEntry(category = category, message = recordWithNewline(record)))
    } catch {
      case NonFatal(e) =>
        stats.handleError(e)
        Seq.empty[LogEntry]
    }
  }
}
