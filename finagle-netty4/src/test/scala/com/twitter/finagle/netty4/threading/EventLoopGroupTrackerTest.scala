package com.twitter.finagle.netty4.threading

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.Duration
import io.netty.channel.nio.NioEventLoopGroup
import com.twitter.logging.Logger
import java.util.concurrent.Executors
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.mockito.Mockito.atLeast
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchers.contains
import org.mockito.ArgumentMatchers.anyVararg
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.IntegrationPatience
import org.scalatestplus.mockito.MockitoSugar
import scala.collection.JavaConverters._
import org.scalatest.funsuite.AnyFunSuite

class EventLoopGroupTrackerTest
    extends AnyFunSuite
    with Eventually
    with IntegrationPatience
    with MockitoSugar {

  test(
    "EventLoopGroupTracker with thread dump disabled records stats but no threads created and no logging"
  ) {
    val statsReceiver = new InMemoryStatsReceiver

    val mockLogger = mock[Logger]

    val executor = Executors.newCachedThreadPool(
      new NamedPoolThreadFactory("finagle_thread_delay_tracking_test", makeDaemons = true)
    )

    val eventLoopGroup = new NioEventLoopGroup(1, executor)

    EventLoopGroupTracker.track(
      eventLoopGroup,
      Duration.fromMilliseconds(50),
      Duration.Zero,
      statsReceiver,
      "no_threads_expected",
      mockLogger
    )

    eventLoopGroup.execute(new Runnable {
      override def run(): Unit = {
        Thread.sleep(200)
      }
    })

    // Force ourselves to wait
    Thread.sleep(300)

    // we should have deviation, cpu time, and active sockets stats
    assert(statsReceiver.stats.get(Seq("workerpool", "deviation_ms")).isDefined)
    assert(
      statsReceiver.counters
        .get(Seq("finagle_thread_delay_tracking_test-1", "cpu_time_ms")).isDefined)
    assert(
      statsReceiver.stats
        .get(Seq("finagle_thread_delay_tracking_test-1", "all_sockets")).isDefined)

    // we should have no threads with the name no_threads_expected
    Thread.getAllStackTraces.keySet().asScala.foreach { thread: Thread =>
      assert(!thread.getName.contains("no_threads_expected"))
    }

    // validate no logging
    verify(mockLogger, never()).warning(anyString(), anyVararg())
  }

  test(
    "EventLoopGroupTracker with thread dump enabled records stats creates watch threads and logs dumps"
  ) {
    val statsReceiver = new InMemoryStatsReceiver

    val mockLogger = mock[Logger]

    val executor = Executors.newCachedThreadPool(
      new NamedPoolThreadFactory("finagle_thread_delay_tracking_test", makeDaemons = true)
    )

    val eventLoopGroup = new NioEventLoopGroup(1, executor)

    EventLoopGroupTracker.track(
      eventLoopGroup,
      Duration.fromMilliseconds(50),
      Duration.fromMilliseconds(10),
      statsReceiver,
      "execution_delay_test_pool",
      mockLogger
    )

    eventLoopGroup.execute(new Runnable {
      override def run(): Unit = {
        Thread.sleep(200)
      }
    })

    // force ourselves to wait
    Thread.sleep(300)

    // we should have deviation stats
    statsReceiver.stats.get(Seq("workerpool", "deviation_ms")).isDefined

    // we should have threads with the name no_threads_expected
    assert(
      Thread.getAllStackTraces
        .keySet().asScala
        .exists(thread => thread.getName.contains("execution_delay_test_pool"))
    )

    // we should have logged a thread dump and an actual delay time for the thread
    verify(mockLogger, atLeast(1))
      .warning(contains("EXECUTION DELAY exceeded configured dump"), anyVararg())
    verify(mockLogger, atLeast(1)).warning(contains("EXECUTION DELAY is greater than"), anyVararg())
  }

  test(
    "validate EventLoopGroupTracker track guards against multiple submissions of the same EventLoopGroup"
  ) {
    // clear our tracking set first as other tests added to the set
    EventLoopGroupTracker.trackedEventLoopGroups.clear()

    val statsReceiver = new InMemoryStatsReceiver

    val mockLogger = mock[Logger]

    val executor = Executors.newCachedThreadPool(
      new NamedPoolThreadFactory("finagle_thread_delay_tracking_test_2", makeDaemons = true)
    )

    val eventLoopGroup = new NioEventLoopGroup(1, executor)
    val eventLoopGroup2 = new NioEventLoopGroup(1, executor)

    EventLoopGroupTracker.track(
      eventLoopGroup,
      Duration.fromMilliseconds(50),
      Duration.Zero,
      statsReceiver,
      "execution_delay_test_pool",
      mockLogger
    )
    assert(EventLoopGroupTracker.trackedEventLoopGroups.size == 1)

    EventLoopGroupTracker.track(
      eventLoopGroup2,
      Duration.fromMilliseconds(50),
      Duration.Zero,
      statsReceiver,
      "execution_delay_test_pool",
      mockLogger
    )
    assert(EventLoopGroupTracker.trackedEventLoopGroups.size == 2)

    EventLoopGroupTracker.track(
      eventLoopGroup,
      Duration.fromMilliseconds(50),
      Duration.Zero,
      statsReceiver,
      "execution_delay_test_pool",
      mockLogger
    )
    assert(EventLoopGroupTracker.trackedEventLoopGroups.size == 2)

  }
}
