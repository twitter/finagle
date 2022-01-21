package com.twitter.finagle.client

import com.twitter.conversions.DurationOps._
import com.twitter.conversions.PercentOps._
import com.twitter.finagle._
import com.twitter.finagle.naming.BindingFactory
import com.twitter.finagle.service.{ReqRep, ResponseClass, ResponseClassifier, RetryBudget}
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.finagle.util.{MockWindowedPercentileHistogram, WindowedPercentileHistogram}
import com.twitter.util._
import com.twitter.util.registry.{Entry, GlobalRegistry, SimpleRegistry}
import com.twitter.util.tunable.Tunable
import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.scalatest.OneInstancePerTest
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import scala.util.Random

class BackupRequestFilterTest
    extends AnyFunSuite
    with OneInstancePerTest
    with MockitoSugar
    with Matchers
    with Eventually
    with IntegrationPatience {

  private[this] val wp = new MockWindowedPercentileHistogram()

  private[this] val timer = new MockTimer

  // The BRF has one perpetual timer task for updates to `sendBackupAfter`
  private[this] def numBackupTimerTasks: Int =
    timer.tasks.size - 1

  private[this] val underlying = mock[Service[String, String]]

  private[this] val fac = ServiceFactory.const(underlying)

  private[this] val statsReceiver = new InMemoryStatsReceiver

  private[this] val classifier: ResponseClassifier = {
    case ReqRep(_, Return(rep)) if rep == "failure" => ResponseClass.RetryableFailure
    case ReqRep(_, Throw(_)) => ResponseClass.RetryableFailure
    case ReqRep(_, Return(rep)) if rep == "ok" => ResponseClass.Success
  }

  private[this] val clientRetryBudget = RetryBudget(5.seconds, 10, 20.percent, Stopwatch.timeMillis)
  private[this] val backupRequestRetryBudget =
    RetryBudget(30.seconds, 10, 1.percent, Stopwatch.timeMillis)

  private[this] val newBackupRequestRetryBudget: (Double, () => Long) => RetryBudget =
    (_, _) => backupRequestRetryBudget

  private[this] val maxExtraLoadTunable: Tunable.Mutable[Double] =
    Tunable.mutable[Double]("brfTunable", 1.percent)

  private[this] def newBrf: BackupRequestFilter[String, String] =
    new BackupRequestFilter[String, String](
      maxExtraLoadTunable,
      true,
      1,
      classifier,
      newBackupRequestRetryBudget,
      clientRetryBudget,
      Stopwatch.timeMillis,
      statsReceiver,
      timer,
      () => wp
    )

  private[this] def newBrfWithSendBackup10ms: BackupRequestFilter[String, String] =
    new BackupRequestFilter[String, String](
      maxExtraLoadTunable,
      true,
      10,
      classifier,
      newBackupRequestRetryBudget,
      clientRetryBudget,
      Stopwatch.timeMillis,
      statsReceiver,
      timer,
      () => wp
    )

  private[this] def newService(
    brf: BackupRequestFilter[String, String] = newBrf
  ): Service[String, String] =
    brf.andThen(underlying)

  private[this] val rng = new Random(123)

  private[this] val WarmupRequestLatency = 1.second

  private[this] def warmFilterForBackup(
    tc: TimeControl,
    service: Service[String, String],
    brf: BackupRequestFilter[String, String],
    requestLatency: Duration
  ): Unit = {
    assert(numBackupTimerTasks == 0)
    (0 until 100).foreach { _ =>
      val p = new Promise[String]
      when(underlying("ok")).thenReturn(p)
      val f = service("ok")
      tc.advance(requestLatency)
      p.setValue("ok")
    }

    // flush
    tc.advance(3.seconds)
    timer.tick()
    assert(numBackupTimerTasks == 0)
    assert(statsReceiver.counters(Seq("backups_sent")) == 0)
    assert(brf.sendBackupAfterDuration > Duration.Zero)
  }

  def testRetryBudgetEmpty(maxExtraLoad: Tunable[Double]): Unit = {
    var currentRetryBudget: RetryBudget = null
    var currentMaxExtraLoad: Double = -1.0 // sentinel to make sure it gets set to 0.0 below

    def newRetryBudget(maxExtraLoad: Double, nowMillis: () => Long): RetryBudget = {
      currentMaxExtraLoad = maxExtraLoad
      currentRetryBudget = BackupRequestFilter.newRetryBudget(maxExtraLoad, nowMillis)
      currentRetryBudget
    }

    val filter = new BackupRequestFilter[String, String](
      maxExtraLoad,
      false,
      1,
      ResponseClassifier.Default,
      newRetryBudget,
      RetryBudget.Infinite,
      Stopwatch.timeMillis,
      NullStatsReceiver,
      timer,
      () => wp
    )
    assert(currentRetryBudget eq RetryBudget.Empty)
    assert(currentMaxExtraLoad == 0.percent)

    // Now make sure it's ok if we change the maxExtraLoad from a valid value to this one after
    // the filter is created

    val tunable = Tunable.mutable("brfTunable", 50.percent)

    Time.withCurrentTimeFrozen { tc =>
      val filter = new BackupRequestFilter[String, String](
        tunable,
        false,
        1,
        ResponseClassifier.Default,
        newRetryBudget,
        RetryBudget.Infinite,
        Stopwatch.timeMillis,
        NullStatsReceiver,
        timer,
        () => wp
      )
      assert(currentRetryBudget ne RetryBudget.Empty)
      assert(currentMaxExtraLoad == 50.percent)
      if (maxExtraLoad eq Tunable.none) tunable.clear() else tunable.set(maxExtraLoad().get)
      tc.advance(3.seconds)
      timer.tick()
      assert(currentRetryBudget eq RetryBudget.Empty)
      assert(currentMaxExtraLoad == 0.percent)
    }
  }

  test("extra load must be non-negative") {
    intercept[IllegalArgumentException] {
      BackupRequestFilter.Configured(-500.percent, false)
    }
  }

  test("extra load must be <= 1.0") {
    intercept[IllegalArgumentException] {
      BackupRequestFilter.Configured(200.percent, false)
    }
  }

  test("Uses 0.0 for maxExtraLoad if Tunable is negative") {
    testRetryBudgetEmpty(Tunable.const("brfTunable", -500.percent))
  }

  test("Uses 0.0 for maxExtraLoad if Tunable is > 1.0") {
    testRetryBudgetEmpty(Tunable.const("brfTunable", 500.percent))
  }

  test("Uses 0.0 for maxExtraLoad if Tunable.apply is None") {
    testRetryBudgetEmpty(Tunable.none)
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
      p.setValue("ok back") // 100 ms latency added to WindowedPercentile
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
        .toStack(Stack.leaf(Stack.Role("Service"), fac))

    val ps: Stack.Params = Stack.Params.empty + param.Stats(statsReceiver) + param.Timer(timer)

    // Disabled by default
    assert(s.make(ps) == fac)

    // Disabled by explicitly making it Disabled
    assert(s.make(ps + BackupRequestFilter.Disabled) == fac)

    // Configured
    Time.withCurrentTimeFrozen { tc =>
      s.make(ps + BackupRequestFilter.Configured(50.percent, false)).toService
      tc.advance(4.seconds)
      timer.tick()
      assert(statsReceiver.stats.contains(Seq("backups", "send_backup_after_ms")))
    }
  }

  test("Original request completes successfully before timer fires") {
    Time.withCurrentTimeFrozen { tc =>
      val brf = newBrf
      val service = newService(brf)
      warmFilterForBackup(tc, service, brf, WarmupRequestLatency)

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
      assert(wp.percentile(50.percent) == (brf.sendBackupAfterDuration / 2).inMillis)

      // ensure timer cancelled
      assert(numBackupTimerTasks == 0)

      // ensure result of original request returned
      assert(f.poll == Some(Return("orig")))

      assert(statsReceiver.counters(Seq("backups_sent")) == 0)
    }
  }

  test("Original request completes unsuccessfully before timer fires") {
    Time.withCurrentTimeFrozen { tc =>
      val brf = newBrf
      val service = newService(brf)
      val exc = new Exception("boom")
      warmFilterForBackup(tc, service, brf, WarmupRequestLatency)

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

      assert(statsReceiver.counters(Seq("backups_sent")) == 0)
    }
  }

  def sendBackup(
    origPromise: Promise[String],
    backupPromise: Promise[String],
    tc: TimeControl,
    sendInterrupts: Boolean
  ): Future[String] = {

    val brf = (new BackupRequestFilter[String, String](
      Tunable.const("brfTunable", 50.percent),
      sendInterrupts,
      1,
      classifier,
      newBackupRequestRetryBudget,
      clientRetryBudget,
      Stopwatch.timeMillis,
      statsReceiver,
      timer,
      () => wp
    ))

    val service = brf.andThen(underlying)

    warmFilterForBackup(tc, service, brf, WarmupRequestLatency)

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

      assert(wp.percentile(50.percent) == WarmupRequestLatency.inMillis)

      tc.advance(1.second)
      timer.tick()

      backupPromise.setValue("backup")

      // ensure latency recorded. Backup was sent at `WarmupRequestLatency`, and 1 second
      // has elapsed since then.
      assert(wp.percentile(50.percent) == 1.second.inMillis)

      // ensure result of backup request returned
      assert(f.poll == Some(Return("backup")))

      // ensure backup not interrupted, but original is because sendInterrupts=true
      assert(backupPromise.isInterrupted == None)
      origPromise.isInterrupted match {
        case Some(f: Failure) if f.isFlagged(FailureFlags.Ignorable) =>
          origPromise.setException(f)
        case None => fail("expected Failure flagged FailureFlags.Ignorable")
      }

      // ensure latency for original recorded
      assert(wp.percentile(50.percent) == (WarmupRequestLatency + 1.second).inMillis)

      assert(statsReceiver.counters(Seq("backups_sent")) == 1)
      assert(statsReceiver.counters(Seq("backups_won")) == 1)
    }
  }

  test(s"Backup request completes successfully first, sendInterrupts=false") {
    Time.withCurrentTimeFrozen { tc =>
      val origPromise, backupPromise = new Promise[String]
      val f = sendBackup(origPromise, backupPromise, tc, sendInterrupts = false)

      assert(wp.percentile(50.percent) == WarmupRequestLatency.inMillis)

      tc.advance(1.second)
      timer.tick()

      backupPromise.setValue("backup")

      // ensure latency recorded. Backup was sent at `WarmupRequestLatency`, and 1 second
      // has elapsed since then.
      assert(wp.percentile(50.percent) == 1.second.inMillis)

      // ensure result of backup request returned
      assert(f.poll == Some(Return("backup")))

      // ensure backup and original not interrupted because sendInterrupts=false
      assert(backupPromise.isInterrupted == None)
      assert(origPromise.isInterrupted == None)

      origPromise.setValue("done")

      // ensure latency for original recorded
      assert(wp.percentile(50.percent) == (WarmupRequestLatency + 1.second).inMillis)

      assert(statsReceiver.counters(Seq("backups_sent")) == 1)
      assert(statsReceiver.counters(Seq("backups_won")) == 1)
    }
  }

  test(s"Original request completes successfully first, sendInterrupts=true") {
    Time.withCurrentTimeFrozen { tc =>
      val origPromise, backupPromise = new Promise[String]
      val f = sendBackup(origPromise, backupPromise, tc, sendInterrupts = true)

      assert(wp.percentile(50.percent) == WarmupRequestLatency.inMillis)

      tc.advance(1.second)
      timer.tick()

      origPromise.setValue("orig")

      // ensure latency recorded
      assert(wp.percentile(50.percent) == (WarmupRequestLatency + 1.second).inMillis)

      // ensure result of backup request returned
      assert(f.poll == Some(Return("orig")))

      // ensure original not interrupted, but backup is because sendInterrupts=true
      assert(origPromise.isInterrupted == None)
      backupPromise.isInterrupted match {
        case Some(f: Failure) if f.isFlagged(FailureFlags.Ignorable) =>
          backupPromise.setException(f)
        case None => fail("expected Failure flagged FailureFlags.Ignorable")
      }

      // ensure latency for backup recorded
      assert(wp.percentile(50.percent) == 1.second.inMillis)

      assert(statsReceiver.counters(Seq("backups_sent")) == 1)
      assert(statsReceiver.counters(Seq("backups_won")) == 0)
    }
  }

  test(s"Original request completes successfully first, sendInterrupts=false") {
    Time.withCurrentTimeFrozen { tc =>
      val origPromise, backupPromise = new Promise[String]
      val f = sendBackup(origPromise, backupPromise, tc, sendInterrupts = false)

      assert(wp.percentile(50.percent) == WarmupRequestLatency.inMillis)

      tc.advance(1.second)
      timer.tick()

      origPromise.setValue("orig")

      // ensure latency recorded
      assert(wp.percentile(50.percent) == (WarmupRequestLatency + 1.second).inMillis)

      // ensure result of orig request returned
      assert(f.poll == Some(Return("orig")))

      // ensure backup and original not interrupted because sendInterrupts=false
      assert(backupPromise.isInterrupted == None)
      assert(origPromise.isInterrupted == None)

      backupPromise.setValue("done")

      // ensure latency for backup recorded
      assert(wp.percentile(50.percent) == 1.second.inMillis)

      assert(statsReceiver.counters(Seq("backups_sent")) == 1)
      assert(statsReceiver.counters(Seq("backups_won")) == 0)
    }
  }

  test(s"Original request completes unsuccessfully first") {
    Time.withCurrentTimeFrozen { tc =>
      val origPromise, backupPromise = new Promise[String]
      val f = sendBackup(origPromise, backupPromise, tc, sendInterrupts = false)

      assert(wp.percentile(50.percent) == WarmupRequestLatency.inMillis)

      tc.advance(1.second)
      timer.tick()

      origPromise.setValue("failure")

      // ensure latency *not* recorded (because request failed fast)
      assert(wp.percentile(50.percent) == WarmupRequestLatency.inMillis)

      // ensure result of orig request *not* returned
      assert(f.poll == None)

      backupPromise.setValue("backup")

      // ensure result of backup request returned
      assert(f.poll == Some(Return("backup")))

      assert(statsReceiver.counters(Seq("backups_sent")) == 1)
      assert(statsReceiver.counters(Seq("backups_won")) == 0)
    }
  }

  test("Backup request completes unsuccessfully first") {
    Time.withCurrentTimeFrozen { tc =>
      val origPromise, backupPromise = new Promise[String]
      val f = sendBackup(origPromise, backupPromise, tc, sendInterrupts = false)

      assert(wp.percentile(50.percent) == WarmupRequestLatency.inMillis)

      tc.advance(1.second)
      timer.tick()

      backupPromise.setValue("failure")

      // ensure latency *not* recorded (because backup failed fast)
      assert(wp.percentile(50.percent) == WarmupRequestLatency.inMillis)

      // ensure result of backup request *not* returned
      assert(f.poll == None)

      origPromise.setValue("orig")

      // ensure result of orig request returned
      assert(f.poll == Some(Return("orig")))

      // ensure latency for original recorded
      assert(wp.percentile(50.percent) == (WarmupRequestLatency + 1.second).inMillis)

      assert(statsReceiver.counters(Seq("backups_sent")) == 1)
      assert(statsReceiver.counters(Seq("backups_won")) == 0)
    }
  }

  test("Deposits into local RetryBudget only") {
    Time.withCurrentTimeFrozen { tc =>
      val service = newService()
      assert(backupRequestRetryBudget.balance == 300)
      assert(clientRetryBudget.balance == 50)
      0.until(100).foreach { _ =>
        val p = new Promise[String]
        when(underlying("ok")).thenReturn(p)
        val f = service("ok")
        p.setValue("ok")
      }
      assert(backupRequestRetryBudget.balance == 301)
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
    val factory = new BackupRequestFactory[String, String](underlying, filter)
    Await.result(factory.close(Duration.Top), 2.seconds)
    verify(underlying, times(1)).close(any[Time]())
    verify(filter, times(1)).close(any[Time]())
  }

  def testCannotSendBackupIfBudgetExhausted(retryBudget: RetryBudget, budgetName: String): Unit = {
    test(s"Don't send backup when backup timer fires if cannot withdraw from $budgetName") {
      Time.withCurrentTimeFrozen { tc =>
        val brf = newBrf
        val service = newService(brf)
        warmFilterForBackup(tc, service, brf, WarmupRequestLatency)

        val p = new Promise[String]
        when(underlying("a")).thenReturn(p)
        verify(underlying, times(0)).apply("a")

        val f = service("a")
        verify(underlying).apply("a")
        assert(!f.isDefined)
        assert(numBackupTimerTasks == 1)

        (0.until(300)).foreach(_ => retryBudget.tryWithdraw())
        assert(!retryBudget.tryWithdraw())
        tc.advance(brf.sendBackupAfterDuration)
        timer.tick()
        verify(underlying, times(1)).apply("a")

        p.setValue("orig")
        assert(f.poll == Some(Return("orig")))
        assert(statsReceiver.counters(Seq("budget_exhausted")) == 1)
        assert(statsReceiver.counters(Seq("backups_sent")) == 0)
      }
    }
  }

  testCannotSendBackupIfBudgetExhausted(clientRetryBudget, "global retry budget")
  testCannotSendBackupIfBudgetExhausted(backupRequestRetryBudget, "backup request retry budget")

  test("record latency on timeout") {
    Time.withCurrentTimeFrozen { tc =>
      val origPromise, backupPromise = new Promise[String]
      val f = sendBackup(origPromise, backupPromise, tc, sendInterrupts = false)
      assert(wp.percentile(50.percent) == WarmupRequestLatency.inMillis)
      tc.advance(1.second)
      timer.tick()
      origPromise.setException(new IndividualRequestTimeoutException(2.seconds))
      assert(wp.percentile(50.percent) == (WarmupRequestLatency + 1.second).inMillis)
    }
  }

  test("RetryBudget is empty if `maxExtraLoad` Tunable is changed to 0.0 dynamically") {

    var currentRetryBudget: RetryBudget = null

    def newRetryBudget(maxExtraLoad: Double, nowMillis: () => Long): RetryBudget = {
      currentRetryBudget = BackupRequestFilter.newRetryBudget(maxExtraLoad, nowMillis)
      currentRetryBudget
    }

    Time.withCurrentTimeFrozen { tc =>
      val brf = new BackupRequestFilter[String, String](
        maxExtraLoadTunable,
        true,
        1,
        classifier,
        newRetryBudget,
        clientRetryBudget,
        Stopwatch.timeMillis,
        statsReceiver,
        timer,
        () => wp
      )
      val service = newService(brf)
      warmFilterForBackup(tc, service, brf, WarmupRequestLatency)
      assert(currentRetryBudget.balance == 100)

      // Set filter to send no backups; advance 3 seconds so we see the change
      maxExtraLoadTunable.set(0.percent)
      tc.advance(3.seconds)
      timer.tick()

      // ensure the budget is now empty
      assert(currentRetryBudget eq RetryBudget.Empty)

      // ensure we really can't sent a backup
      val p = new Promise[String]
      when(underlying("a")).thenReturn(p)

      val f = service("a")
      verify(underlying).apply("a")
      assert(!f.isDefined)

      tc.advance(brf.sendBackupAfterDuration)
      timer.tick()
      verify(underlying, times(1)).apply("a")

      p.setValue("orig")
      assert(f.poll == Some(Return("orig")))
      assert(statsReceiver.counters(Seq("budget_exhausted")) == 1)
      assert(statsReceiver.counters(Seq("backups_sent")) == 0)
    }
  }

  test("if maxExtraLoad remains at the same value, no new RetryBudget is created") {
    var newRetryBudgetCalls = 0
    def newRetryBudget(maxExtraLoad: Double, nowMillis: () => Long): RetryBudget = {
      newRetryBudgetCalls += 1
      BackupRequestFilter.newRetryBudget(maxExtraLoad, nowMillis)
    }

    Time.withCurrentTimeFrozen { tc =>
      val brf = new BackupRequestFilter[String, String](
        maxExtraLoadTunable,
        true,
        1,
        classifier,
        newRetryBudget,
        clientRetryBudget,
        Stopwatch.timeMillis,
        statsReceiver,
        timer,
        () => wp
      )
      val service = newService(brf)
      warmFilterForBackup(tc, service, brf, WarmupRequestLatency)
      assert(newRetryBudgetCalls == 1)
      maxExtraLoadTunable.set(1.percent)
      // we refresh the budget every 3 seconds if the Tunable value has changed
      tc.advance(3.seconds)
      timer.tick()
      // `newRetryBudget` is called asynchronously, but we'd expect this test to fail often if
      // `newRetryBudget` was called more than once
      assert(newRetryBudgetCalls == 1)
    }
  }

  test("sendBackupAfter percentile and RetryBudget changed when maxExtraLoad Tunable is changed") {
    var currentRetryBudget: RetryBudget = null

    def newRetryBudget(maxExtraLoad: Double, nowMillis: () => Long): RetryBudget = {
      currentRetryBudget = BackupRequestFilter.newRetryBudget(maxExtraLoad, nowMillis)
      currentRetryBudget
    }

    Time.withCurrentTimeFrozen { tc =>
      val brf = new BackupRequestFilter[String, String](
        maxExtraLoadTunable,
        true,
        1,
        classifier,
        newRetryBudget,
        clientRetryBudget,
        Stopwatch.timeMillis,
        statsReceiver,
        timer,
        () => new WindowedPercentileHistogram(5, 3.seconds, timer)
      )
      val service = newService(brf)
      assert(currentRetryBudget.balance == 100)
      (0 until 90).foreach { _ =>
        val p = new Promise[String]
        when(underlying("ok")).thenReturn(p)
        val f = service("ok")
        tc.advance(50.millis)
        p.setValue("ok")
      }
      (0 until 10).foreach { _ =>
        val p = new Promise[String]
        when(underlying("ok")).thenReturn(p)
        val f = service("ok")
        tc.advance(100.millis)
        p.setValue("ok")
      }
      tc.advance(3.seconds)
      timer.tick()
      assert(brf.sendBackupAfterDuration == 100.millis)
      assert(
        currentRetryBudget.toString ==
          "TokenRetryBudget(deposit=1000, withdraw=100000, balance=101)"
      )

      // Set filter to send 10% backups; advance the time to see the change
      maxExtraLoadTunable.set(10.percent)
      tc.advance(3.seconds)
      timer.tick()
      assert(brf.sendBackupAfterDuration == 50.millis)

      // note that new budget does not get balance from old budget.
      eventually {
        assert(
          currentRetryBudget.toString ==
            "TokenRetryBudget(deposit=1000, withdraw=10000, balance=100)"
        )
      }
    }
  }

  test("Minimum sendBackupAfter of 1ms") {
    Time.withCurrentTimeFrozen { tc =>
      val brf = newBrf
      val service = newService(brf)

      (0 until 100).foreach { _ =>
        val p = new Promise[String]
        when(underlying("ok")).thenReturn(p)
        val f = service("ok")
        p.setValue("ok")
      }

      // flush
      tc.advance(3.seconds)
      timer.tick()
      assert(numBackupTimerTasks == 0)
      assert(statsReceiver.counters(Seq("backups_sent")) == 0)
      assert(brf.sendBackupAfterDuration == 1.millisecond)
      assert(statsReceiver.stats(Seq("send_backup_after_ms")) == Seq(1))
    }
  }

  test("Minimum sendBackupAfter can be overwritten") {
    Time.withCurrentTimeFrozen { tc =>
      val brf = newBrfWithSendBackup10ms
      val service = newService(brf)

      (0 until 100).foreach { _ =>
        val p = new Promise[String]
        when(underlying("ok")).thenReturn(p)
        val f = service("ok")
        p.setValue("ok")
      }

      // flush
      tc.advance(3.seconds)
      timer.tick()
      assert(numBackupTimerTasks == 0)
      assert(statsReceiver.counters(Seq("backups_sent")) == 0)
      assert(brf.sendBackupAfterDuration == 10.millisecond)
      assert(statsReceiver.stats(Seq("send_backup_after_ms")) == Seq(10))
    }
  }

  test("filterService returns the passed-in service if BRF not configured") {
    val params = Stack.Params.empty
    assert(BackupRequestFilter.filterService(params, underlying) eq underlying)
  }

  test("Service returned by filterService closes the underlying service when closed") {
    val params = Stack.Params.empty + BackupRequestFilter.Configured(50.percent, false)
    val svc = BackupRequestFilter.filterService(params, underlying)
    assert(svc ne underlying)

    when(underlying.close(any[Time]())).thenReturn(Future.Done)
    svc.close(Time.Top)
    verify(underlying, times(1)).close(any[Time]())
  }

  test("SupersededRequestFailureToString hasn't changed") {
    val expected =
      "Failure(Request was superseded by another in BackupRequestFilter, flags=0x20) with NoSources"
    assert(BackupRequestFilter.SupersededRequestFailureToString == expected)
  }

  test("Percentile latency exceeds measurable latency") {
    Time.withCurrentTimeFrozen { tc =>
      val brf = newBrf
      val service = newService(brf)
      warmFilterForBackup(tc, service, brf, wp.highestTrackableValue.millis)

      val p = new Promise[String]
      when(underlying("b")).thenReturn(p)
      val f = service("b")
      verify(underlying).apply("b")
      assert(numBackupTimerTasks == 0)
      tc.advance(brf.sendBackupAfterDuration / 2)
      p.setValue("orig")
      timer.tick()

      // ensure latency recorded
      assert(wp.percentile(50.percent) == (brf.sendBackupAfterDuration / 2).inMillis)

      // ensure timer cancelled
      assert(numBackupTimerTasks == 0)

      // ensure result of original request returned
      assert(f.poll == Some(Return("orig")))

      assert(statsReceiver.counters(Seq("backups_sent")) == 0)
    }
  }

  test(
    "BackupRequestFilter is added to the Registry when protocolLibrary, label, and dest are present in the stack params") {
    val mockService = mock[Service[String, String]]
    val registry = new SimpleRegistry()
    GlobalRegistry.withRegistry(registry) {
      val params =
        Stack.Params.empty +
          param.ProtocolLibrary("thrift") +
          param.Label("test") +
          BindingFactory.Dest(Name.Path(Path.read("/$/inet/localhost/0"))) +
          BackupRequestFilter.Configured(50.percent, false)

      BackupRequestFilter.filterService(params, mockService)

      def key(name: String, suffix: String*): Seq[String] =
        Seq("client", name) ++ suffix

      def filteredRegistry: Set[Entry] =
        registry.filter { entry => entry.key.contains("BackupRequestFilter") }.toSet

      val registeredEntries = Set(
        Entry(
          key("thrift", "test", "/$/inet/localhost/0", "BackupRequestFilter"),
          "maxExtraLoad: Some(0.5), sendInterrupts: false")
      )

      filteredRegistry should contain theSameElementsAs registeredEntries
    }
  }

  test(
    "BackupRequestFilter is not added to the Registry when any of protocolLibrary, label, or dest is missing in the stack params") {
    val mockService = mock[Service[String, String]]
    val registry = new SimpleRegistry()
    GlobalRegistry.withRegistry(registry) {
      val paramsMissingProtocolLibrary =
        Stack.Params.empty +
          param.Label("test") +
          BindingFactory.Dest(Name.Path(Path.read("/$/inet/localhost/0"))) +
          BackupRequestFilter.Configured(50.percent, false)

      val paramsMissingLabel =
        Stack.Params.empty +
          param.ProtocolLibrary("thrift") +
          BindingFactory.Dest(Name.Path(Path.read("/$/inet/localhost/0"))) +
          BackupRequestFilter.Configured(50.percent, false)

      val paramsMissingDest =
        Stack.Params.empty +
          param.ProtocolLibrary("thrift") +
          param.Label("test") +
          BackupRequestFilter.Configured(50.percent, false)

      BackupRequestFilter.filterService(paramsMissingProtocolLibrary, mockService)
      BackupRequestFilter.filterService(paramsMissingLabel, mockService)
      BackupRequestFilter.filterService(paramsMissingDest, mockService)

      def filteredRegistry: Set[Entry] =
        registry.filter { entry => entry.key.contains("BackupRequestFilter") }.toSet

      filteredRegistry should contain theSameElementsAs Set.empty
    }
  }
}
