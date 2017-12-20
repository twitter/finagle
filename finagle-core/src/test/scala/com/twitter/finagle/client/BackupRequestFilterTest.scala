package com.twitter.finagle.client

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.service.{ReqRep, ResponseClass, ResponseClassifier, RetryBudget}
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.finagle.util.WindowedPercentileHistogram
import com.twitter.util._
import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.scalatest.{FunSuite, Matchers, OneInstancePerTest}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.mockito.MockitoSugar
import scala.util.Random

class BackupRequestFilterTest extends FunSuite
  with OneInstancePerTest
  with MockitoSugar
  with Matchers
  with Eventually
  with IntegrationPatience {

  // Stores only the last added value
  class MockWindowedPercentileHistogram
    extends WindowedPercentileHistogram(0, Duration.Top, new MockTimer) {
    private[this] var _value: Int = 0

    var closed = false

    override def add(value: Int): Unit =
      _value = value

    override def percentile(percentile: Double): Int =
      _value

    override def close(deadline: Time): Future[Unit] = {
      closed = true
      Future.Done
    }
  }

  private[this] val wp = new MockWindowedPercentileHistogram()

  private[this] val timer = new MockTimer

  // The BRF has one perpetual timer task for updates to `sendBackupAfter`
  private[this] def numBackupTimerTasks: Int =
    timer.tasks.size - 1

  private[this] val underlying = mock[Service[String, String]]

  private[this] val fac = ServiceFactory.const(underlying)

  private[this] val statsReceiver = new InMemoryStatsReceiver

  private[this] val classifier: ResponseClassifier = {
    case ReqRep(_ , Return(rep)) if rep == "failure" => ResponseClass.RetryableFailure
    case ReqRep(_, Throw(_)) => ResponseClass.RetryableFailure
    case ReqRep(_ , Return(rep)) if rep == "ok" => ResponseClass.Success
  }

  private[this] val clientRetryBudget = RetryBudget(5.seconds, 10, 0.2, Stopwatch.timeMillis)
  private[this] val backupRequestRetryBudget = RetryBudget(5.seconds, 10, 0.01, Stopwatch.timeMillis)

  val maxDuration = 10.seconds

  private[this] def newBrf: BackupRequestFilter[String, String] =
    new BackupRequestFilter[String, String](
      0.5,
      true,
      classifier,
      clientRetryBudget,
      backupRequestRetryBudget,
      Stopwatch.timeMillis,
      statsReceiver,
      timer,
      () => wp)

  private[this] def newService(
    brf: BackupRequestFilter[String, String] = newBrf
  ): Service[String, String] =
    brf.andThen(underlying)

  private[this] val rng = new Random(123)

  private[this] val WarmupRequestLatency = 2.seconds

  private[this] def warmFilterForBackup(
    tc: TimeControl,
    service: Service[String, String],
    brf: BackupRequestFilter[String, String]
  ): Unit = {
    assert(numBackupTimerTasks == 0)
    (0 until 100).foreach { _ =>
      val p = new Promise[String]
      when(underlying("ok")).thenReturn(p)
      val f = service("ok")
      tc.advance(WarmupRequestLatency)
      p.setValue("ok")
    }

    // flush
    tc.advance(3.seconds)
    timer.tick()
    assert(numBackupTimerTasks == 0)
    assert(brf.sendBackupAfterDuration > Duration.Zero)
  }

  test("extra load must be non-negative") {
    intercept[IllegalArgumentException] {
      BackupRequestFilter.Configured(-5.0, false)
    }
    val mkBadFilter = () => new BackupRequestFilter[String, String](
      -5.0,
      false,
      ResponseClassifier.Default,
      RetryBudget.Infinite,
      RetryBudget.Infinite,
      Stopwatch.timeMillis,
      NullStatsReceiver,
      timer,
      () => wp
    )
    intercept[IllegalArgumentException] {
      new BackupRequestFactory[String, String](
        fac,
        mkBadFilter()
      )
    }
    intercept[IllegalArgumentException] {
      mkBadFilter()
    }
  }

  test("extra load must be <= 1.0") {
    intercept[IllegalArgumentException] {
      BackupRequestFilter.Configured(2.0, false)
    }
    val mkBadFilter = () => new BackupRequestFilter[String, String](
      2.0,
      false,
      ResponseClassifier.Default,
      RetryBudget.Infinite,
      RetryBudget.Infinite,
      Stopwatch.timeMillis,
      NullStatsReceiver,
      timer,
      () => wp
    )
    intercept[IllegalArgumentException] {
      new BackupRequestFactory[String, String](
        fac,
        mkBadFilter()
      )
    }
    intercept[IllegalArgumentException] {
      mkBadFilter()
    }
  }

  test("adds latency to windowedPercentile") {
    Time.withCurrentTimeFrozen { tc =>
      val service = newService()
      val p = new Promise[String]
      when(underlying("ok")).thenReturn(p)
      assert(wp.percentile(50.0) == 0)
      val rep = service("ok")
      tc.advance(100.millis)
      p.setValue("ok back")
      eventually {
        assert(wp.percentile(50.0) == 100)
      }
    }
  }

  test("stat for send_backup_after") {
    Time.withCurrentTimeFrozen { tc =>
      val service = newService()
      val p = new Promise[String]
      when(underlying("ok")).thenReturn(p)
      val rep = service("ok")
      tc.advance(100.millis)
      p.setValue("ok back")   // 100 ms latency added to WindowedPercentile
      tc.advance(3.seconds) // trigger update of send_backup_after
      timer.tick()
      eventually {
        assert(statsReceiver.stats(Seq("send_backup_after_ms")) == Seq(100))
      }
    }
  }

  test("Params get threaded through module") {
    val s: Stack[ServiceFactory[String, String]] =
      BackupRequestFilter
        .module[String, String]
        .toStack(Stack.Leaf(Stack.Role("Service"), fac))

    val ps: Stack.Params = Stack.Params.empty + param.Stats(statsReceiver) + param.Timer(timer)

    // Disabled by default
    assert(s.make(ps) == fac)

    // Disabled by explicitly making it Disabled
    assert(s.make(ps + BackupRequestFilter.Disabled) == fac)

    // Configured
    Time.withCurrentTimeFrozen { tc =>
      s.make(ps + BackupRequestFilter.Configured(0.5, false)).toService
      tc.advance(4.seconds)
      timer.tick()
      assert(statsReceiver.stats.contains(Seq("backups", "send_backup_after_ms")))
    }
  }

  test("Original request completes successfully before timer fires") {
    Time.withCurrentTimeFrozen { tc =>
      val brf = newBrf
      val service = newService(brf)
      warmFilterForBackup(tc, service, brf)

      val p = new Promise[String]
      when(underlying("b")).thenReturn(p)
      val f = service("b")
      verify(underlying).apply("b")
      assert(numBackupTimerTasks == 1)
      val task = timer.tasks(1)
      assert(!task.isCancelled)
      tc.advance(brf.sendBackupAfterDuration / 2)
      assert(timer.tasks.toSeq.tail == Seq(task))
      verify(underlying).apply("b")
      assert(!task.isCancelled)
      p.setValue("orig")
      assert(task.isCancelled)
      timer.tick()

      // ensure latency recorded
      assert(wp.percentile(50.0) == (brf.sendBackupAfterDuration / 2).inMillis)

      // ensure timer cancelled
      assert(numBackupTimerTasks == 0)

      // ensure result of original request returned
      assert(f.poll == Some(Return("orig")))

      assert(!statsReceiver.counters.contains(Seq("backups_sent")))
    }
  }

  test("Original request completes unsuccessfully before timer fires") {
    Time.withCurrentTimeFrozen { tc =>
      val brf = newBrf
      val service = newService(brf)
      val exc = new Exception("boom")
      warmFilterForBackup(tc, service, brf)

      val p = new Promise[String]
      when(underlying("b")).thenReturn(p)
      val f = service("b")
      verify(underlying).apply("b")
      assert(numBackupTimerTasks == 1)
      val task = timer.tasks(1)
      assert(!task.isCancelled)
      tc.advance(brf.sendBackupAfterDuration / 2)
      assert(timer.tasks.toSeq.tail == Seq(task))
      verify(underlying).apply("b")
      assert(!task.isCancelled)
      p.setException(exc)
      assert(task.isCancelled)
      timer.tick()

      // ensure latency *not* recorded (because request failed fast)
      assert(wp.percentile(50.0) == brf.sendBackupAfterDuration.inMillis)

      // ensure timer cancelled
      assert(numBackupTimerTasks == 0)

      // ensure result of original request is exception
      assert(f.poll == Some(Throw(exc)))

      assert(!statsReceiver.counters.contains(Seq("backups_sent")))
    }
  }

  def sendBackup(
    origPromise: Promise[String],
    backupPromise: Promise[String],
    tc: TimeControl,
    sendInterrupts: Boolean
  ): Future[String] = {

    val brf = (new BackupRequestFilter[String, String](
      0.5,
      sendInterrupts,
      classifier,
      clientRetryBudget,
      backupRequestRetryBudget,
      Stopwatch.timeMillis,
      statsReceiver,
      timer,
      () => wp))

    val service = brf.andThen(underlying)

    warmFilterForBackup(tc, service, brf)

    when(underlying("a")).thenReturn(origPromise)
    verify(underlying, times(0)).apply("a")

    val f = service("a")
    verify(underlying).apply("a")
    assert(!f.isDefined)
    assert(numBackupTimerTasks == 1)

    tc.advance(brf.sendBackupAfterDuration / 2)
    timer.tick()
    assert(numBackupTimerTasks == 1)
    verify(underlying).apply("a")
    when(underlying("a")).thenReturn(backupPromise)
    tc.advance(brf.sendBackupAfterDuration / 2)
    timer.tick()
    assert(numBackupTimerTasks == 0)
    verify(underlying, times(2)).apply("a")
    f
  }

  test(s"Backup request completes successfully first, sendInterrupts=true") {
    Time.withCurrentTimeFrozen { tc =>
      val origPromise, backupPromise = new Promise[String]
      val f = sendBackup(origPromise, backupPromise, tc, sendInterrupts = true)

      assert(wp.percentile(50.0) == WarmupRequestLatency.inMillis)

      tc.advance(1.second)
      timer.tick()

      backupPromise.setValue("backup")

      // ensure latency recorded. Backup was sent at `WarmupRequestLatency`, and 1 second
      // has elapsed since then.
      assert(wp.percentile(50.0) == 1.second.inMillis)

      // ensure result of backup request returned
      assert(f.poll == Some(Return("backup")))

      // ensure backup not interrupted, but original is because sendInterrupts=true
      assert(backupPromise.isInterrupted == None)
      origPromise.isInterrupted match {
        case Some(f: Failure) if f.isFlagged(Failure.Ignorable) =>
          origPromise.setException(f)
        case None => fail("expected Failure flagged Failure.Ignorable")
      }

      // ensure latency for original recorded
      assert(wp.percentile(50.0) == (WarmupRequestLatency + 1.second).inMillis)

      assert(statsReceiver.counters(Seq("backups_sent")) == 1)
      assert(statsReceiver.counters(Seq("backups_won")) == 1)
    }
  }

  test(s"Backup request completes successfully first, sendInterrupts=false") {
    Time.withCurrentTimeFrozen { tc =>
      val origPromise, backupPromise = new Promise[String]
      val f = sendBackup(origPromise, backupPromise, tc, sendInterrupts = false)

      assert(wp.percentile(50.0) == WarmupRequestLatency.inMillis)

      tc.advance(1.second)
      timer.tick()

      backupPromise.setValue("backup")

      // ensure latency recorded. Backup was sent at `WarmupRequestLatency`, and 1 second
      // has elapsed since then.
      assert(wp.percentile(50.0) == 1.second.inMillis)

      // ensure result of backup request returned
      assert(f.poll == Some(Return("backup")))

      // ensure backup and original not interrupted because sendInterrupts=false
      assert(backupPromise.isInterrupted == None)
      assert(origPromise.isInterrupted == None)

      origPromise.setValue("done")

      // ensure latency for original recorded
      assert(wp.percentile(50.0) == (WarmupRequestLatency + 1.second).inMillis)

      assert(statsReceiver.counters(Seq("backups_sent")) == 1)
      assert(statsReceiver.counters(Seq("backups_won")) == 1)
    }
  }

  test(s"Original request completes successfully first, sendInterrupts=true") {
    Time.withCurrentTimeFrozen { tc =>
      val origPromise, backupPromise = new Promise[String]
      val f = sendBackup(origPromise, backupPromise, tc, sendInterrupts = true)

      assert(wp.percentile(50.0) == WarmupRequestLatency.inMillis)

      tc.advance(1.second)
      timer.tick()

      origPromise.setValue("orig")

      // ensure latency recorded
      assert(wp.percentile(50.0) == (WarmupRequestLatency + 1.second).inMillis)

      // ensure result of backup request returned
      assert(f.poll == Some(Return("orig")))

      // ensure original not interrupted, but backup is because sendInterrupts=true
      assert(origPromise.isInterrupted == None)
      backupPromise.isInterrupted match {
        case Some(f: Failure) if f.isFlagged(Failure.Ignorable) =>
          backupPromise.setException(f)
        case None => fail("expected Failure flagged Failure.Ignorable")
      }

      // ensure latency for backup recorded
      assert(wp.percentile(50.0) == 1.second.inMillis)

      assert(statsReceiver.counters(Seq("backups_sent")) == 1)
      assert(!statsReceiver.counters.contains(Seq("backups_won")))
    }
  }

  test(s"Original request completes successfully first, sendInterrupts=false") {
    Time.withCurrentTimeFrozen { tc =>
      val origPromise, backupPromise = new Promise[String]
      val f = sendBackup(origPromise, backupPromise, tc, sendInterrupts = false)

      assert(wp.percentile(50.0) == WarmupRequestLatency.inMillis)

      tc.advance(1.second)
      timer.tick()

      origPromise.setValue("orig")

      // ensure latency recorded
      assert(wp.percentile(50.0) == (WarmupRequestLatency + 1.second).inMillis)

      // ensure result of orig request returned
      assert(f.poll == Some(Return("orig")))

      // ensure backup and original not interrupted because sendInterrupts=false
      assert(backupPromise.isInterrupted == None)
      assert(origPromise.isInterrupted == None)

      backupPromise.setValue("done")

      // ensure latency for backup recorded
      assert(wp.percentile(50.0) == 1.second.inMillis)

      assert(statsReceiver.counters(Seq("backups_sent")) == 1)
      assert(!statsReceiver.counters.contains(Seq("backups_won")))
    }
  }

  test(s"Original request completes unsuccessfully first") {
    Time.withCurrentTimeFrozen { tc =>
      val origPromise, backupPromise = new Promise[String]
      val f = sendBackup(origPromise, backupPromise, tc, sendInterrupts = false)

      assert(wp.percentile(50.0) == WarmupRequestLatency.inMillis)

      tc.advance(1.second)
      timer.tick()

      origPromise.setValue("failure")

      // ensure latency *not* recorded (because request failed fast)
      assert(wp.percentile(50.0) == WarmupRequestLatency.inMillis)

      // ensure result of orig request *not* returned
      assert(f.poll == None)

      backupPromise.setValue("backup")

      // ensure result of backup request returned
      assert(f.poll == Some(Return("backup")))

      assert(statsReceiver.counters(Seq("backups_sent")) == 1)
      assert(!statsReceiver.counters.contains(Seq("backups_won")))
    }
  }

  test(s"Backup request completes unsuccessfully first") {
    Time.withCurrentTimeFrozen { tc =>
      val origPromise, backupPromise = new Promise[String]
      val f = sendBackup(origPromise, backupPromise, tc, sendInterrupts = false)

      assert(wp.percentile(50.0) == WarmupRequestLatency.inMillis)

      tc.advance(1.second)
      timer.tick()

      backupPromise.setValue("failure")

      // ensure latency *not* recorded (because backup failed fast)
      assert(wp.percentile(50.0) == WarmupRequestLatency.inMillis)

      // ensure result of backup request *not* returned
      assert(f.poll == None)

      origPromise.setValue("orig")

      // ensure result of orig request returned
      assert(f.poll == Some(Return("orig")))

      // ensure latency for original recorded
      assert(wp.percentile(50.0) == (WarmupRequestLatency + 1.second).inMillis)

      assert(statsReceiver.counters(Seq("backups_sent")) == 1)
      assert(statsReceiver.counters(Seq("backups_won")) == 1)
    }
  }

  test("Deposits into local RetryBudget only") {
    Time.withCurrentTimeFrozen { tc =>
      val service = newService()
      assert(backupRequestRetryBudget.balance == 50)
      assert(clientRetryBudget.balance == 50)
      0.until(100).foreach { _ =>
        val p = new Promise[String]
        when(underlying("ok")).thenReturn(p)
        val f = service("ok")
        p.setValue("ok")
      }
      assert(backupRequestRetryBudget.balance == 51)
      assert(clientRetryBudget.balance == 50)
    }
  }

  test("Filter closes WindowedPercentileHistogram when closed") {
    val brf = newBrf
    Await.result(brf.close(), 2.seconds)
    assert(wp.closed)
  }

  test("Factory closes Filter and underlying when closed") {
    val underlying = mock[ServiceFactory[String, String]]
    when(underlying.close(any[Time]())).thenReturn(Future.Done)
    val filter = mock[BackupRequestFilter[String, String]]
    when(filter.close(any[Time]())).thenReturn(Future.Done)
    val factory = new BackupRequestFactory[String, String](
      underlying,
      filter)
    Await.result(factory.close(Duration.Top), 2.seconds)
    verify(underlying, times(1)).close(any[Time]())
    verify(filter, times(1)).close(any[Time]())
  }

  def testCannotSendBackupIfBudgetExhausted(retryBudget: RetryBudget, budgetName: String): Unit = {
    test(s"Don't send backup when backup timer fires if cannot withdraw from $budgetName") {
      Time.withCurrentTimeFrozen { tc =>
        val brf = newBrf
        val service = newService(brf)
        warmFilterForBackup(tc, service, brf)

        val p = new Promise[String]
        when(underlying("a")).thenReturn(p)
        verify(underlying, times(0)).apply("a")

        val f = service("a")
        verify(underlying).apply("a")
        assert(!f.isDefined)
        assert(numBackupTimerTasks == 1)

        (0.until(50)).foreach(_ => retryBudget.tryWithdraw())
        assert(!retryBudget.tryWithdraw())
        tc.advance(brf.sendBackupAfterDuration)
        timer.tick()
        verify(underlying, times(1)).apply("a")

        p.setValue("orig")
        assert(f.poll == Some(Return("orig")))
        assert(statsReceiver.counters(Seq("budget_exhausted")) == 1)
        assert(!statsReceiver.counters.contains(Seq("backups_sent")))
      }
    }
  }

  testCannotSendBackupIfBudgetExhausted(clientRetryBudget, "global retry budget")
  testCannotSendBackupIfBudgetExhausted(backupRequestRetryBudget, "backup request retry budget")

  test("record latency on timeout") {
    Time.withCurrentTimeFrozen { tc =>
      val origPromise, backupPromise = new Promise[String]
      val f = sendBackup(origPromise, backupPromise, tc, sendInterrupts = false)
      assert(wp.percentile(50.0) == WarmupRequestLatency.inMillis)
      tc.advance(1.second)
      timer.tick()
      origPromise.setException(new IndividualRequestTimeoutException(2.seconds))
      assert(wp.percentile(50.0) == (WarmupRequestLatency + 1.second).inMillis)
    }
  }

  test("filterService returns the passed-in service if BRF not configured") {
    val params = Stack.Params.empty
    assert(BackupRequestFilter.filterService(params, underlying) eq underlying)
  }

  test("Service returned by filterService closes the underlying service when closed") {
    val params = Stack.Params.empty + BackupRequestFilter.Configured(0.5, false)
    val svc = BackupRequestFilter.filterService(params, underlying)
    assert(svc ne underlying)

    when(underlying.close(any[Time]())).thenReturn(Future.Done)
    svc.close(Time.Top)
    verify(underlying, times(1)).close(any[Time]())
  }
}
