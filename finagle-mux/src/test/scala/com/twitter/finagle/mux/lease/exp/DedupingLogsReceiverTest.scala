package com.twitter.finagle.mux.lease.exp

import java.util.logging.Logger
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.ArgumentMatchers.anyString
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class DedupingLogsReceiverTest extends AnyFunSuite with MockitoSugar {
  test("DedupingLogsReceiver logs when recording and flushing") {
    val mockLog = mock[Logger]
    val lr = new DedupingLogsReceiver(mockLog)
    lr.record("log", "me")
    verify(mockLog, never).info(anyString)
    lr.flush()
    verify(mockLog).info("log=me")
  }

  test("DedupingLogsReceiver relogging obliterates the old") {
    val mockLog = mock[Logger]
    val lr = new DedupingLogsReceiver(mockLog)
    lr.record("log", "me")
    verify(mockLog, never).info(anyString)
    lr.record("log", "you")
    verify(mockLog, never).info(anyString)
    lr.flush()
    verify(mockLog).info("log=you")
  }

  test("DedupingLogsReceiver can log multiple values") {
    val mockLog = mock[Logger]
    val lr = new DedupingLogsReceiver(mockLog)
    lr.record("log", "me")
    verify(mockLog, never).info(anyString)
    lr.record("gol", "em")
    verify(mockLog, never).info(anyString)
    lr.flush()
    verify(mockLog).info("gol=em, log=me")

    // and in reverse order
    lr.record("gol", "em")
    lr.record("log", "me")
    lr.flush()
    verify(mockLog, times(2)).info("gol=em, log=me")
  }

  test("DedupingLogsReceiver record order doesn't matter") {
    val mockLog = mock[Logger]
    val lr = new DedupingLogsReceiver(mockLog)
    lr.record("log", "me")
    lr.record("gol", "em")
    lr.flush()
    verify(mockLog).info("gol=em, log=me")

    lr.record("gol", "em")
    lr.record("log", "me")
    lr.flush()
    verify(mockLog, times(2)).info("gol=em, log=me")
  }

  test("DedupingLogsReceiver flushes clears too") {
    val mockLog = mock[Logger]
    val lr = new DedupingLogsReceiver(mockLog)
    lr.record("log", "1")
    verify(mockLog, never).info("log=1")
    lr.flush()
    verify(mockLog).info("log=1")

    lr.record("log", "2")
    verify(mockLog, never).info("log=2")
    lr.flush()
    verify(mockLog).info("log=1")
    verify(mockLog).info("log=2")
  }
}
