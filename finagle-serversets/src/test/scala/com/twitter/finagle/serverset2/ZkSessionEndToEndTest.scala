package com.twitter.finagle.serverset2

import com.twitter.conversions.time._
import com.twitter.finagle.serverset2.client._
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.zookeeper.ZkInstance
import com.twitter.io.Buf
import com.twitter.util._
import org.junit.runner.RunWith
import org.scalatest.concurrent.Eventually._
import org.scalatest.junit.JUnitRunner
import org.scalatest.time._
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class ZkSessionEndToEndTest extends FunSuite with BeforeAndAfter {
  val zkTimeout = 100.milliseconds
  @volatile var inst: ZkInstance = _

  def toSpan(d: Duration): Span = Span(d.inNanoseconds, Nanoseconds)

  implicit val patienceConfig = PatienceConfig(
    timeout = toSpan(1.second),
    interval = toSpan(zkTimeout))

  /* This can be useful if you want to retain ZK logging output for debugging.
  val app = new org.apache.log4j.ConsoleAppender
  app.setTarget(org.apache.log4j.ConsoleAppender.SYSTEM_ERR)
  app.setLayout(new org.apache.log4j.SimpleLayout)
  app.activateOptions()
  org.apache.log4j.Logger.getRootLogger().addAppender(app)
   */

  before {
    inst = new ZkInstance
    inst.start()
  }

  after {
    inst.stop()
  }

  // COORD-339
  if (!sys.props.contains("SKIP_FLAKY")) test("Session expiration 2") {
    implicit val timer = new MockTimer
    val connected: (WatchState => Boolean) = {
      case WatchState.SessionState(SessionState.SyncConnected) => true
      case _ => false
    }
    val notConnected: (WatchState => Boolean) = w => !connected(w)
    val session1 = ZkSession.retrying(Backoff.constant(zkTimeout), () => ZkSession(inst.zookeeperConnectString, statsReceiver = NullStatsReceiver))

    @volatile var states = Seq.empty[SessionState]
    val state = session1 flatMap { session1 => session1.state }
    state.changes.register(Witness({ ws => ws match {
      case WatchState.SessionState(s) => states = s +: states
      case _ =>
    }}))

    Await.result(state.changes.filter(connected).toFuture())
    val cond = state.changes.filter(notConnected).toFuture()

    val session2 = {
      val z = Var.sample(session1)
      val p = new Array[Byte](z.sessionPasswd.length)
      z.sessionPasswd.write(p, 0)

      ClientBuilder()
        .hosts(inst.zookeeperConnectString)
        .sessionTimeout(zkTimeout)
        .sessionId(z.sessionId)
        .password(Buf.ByteArray.Owned(p))
        .reader()
    }
    Await.result(session2.state.changes.filter(connected).toFuture())
    session2.value.close()

    Await.result(cond)
    Await.result(state.changes.filter(connected).toFuture())

    assert(states == Seq(
      SessionState.SyncConnected, SessionState.Expired,
      SessionState.Disconnected, SessionState.SyncConnected))
  }

  // COORD-339
  if (!sys.props.contains("SKIP_FLAKY")) test("ZkSession.retrying") {
    implicit val timer = new MockTimer
    val watch = Stopwatch.start()
    val varZkSession = ZkSession.retrying(Backoff.constant(zkTimeout), () => ZkSession(inst.zookeeperConnectString, statsReceiver = NullStatsReceiver))
    val varZkState = varZkSession flatMap { _.state }

    @volatile var zkStates = Seq[(SessionState, Duration)]()
    varZkState.changes.register(Witness({ ws => ws match {
      case WatchState.SessionState(state) =>
        zkStates = (state, watch()) +: zkStates
      case _ =>
    }}))

    @volatile var sessions = Seq[ZkSession]()
    varZkSession.changes.register(Witness({ s =>
      sessions = s +: sessions
    }))

    // Wait for the initial connect.
    eventually {
      assert(Var.sample(varZkState) ==
        WatchState.SessionState(SessionState.SyncConnected))
      assert(sessions.size == 1)
    }

    val session1 = Var.sample(varZkSession)

    // Hijack the session by reusing its id and password.
    val session2 = {
      val p = new Array[Byte](session1.sessionPasswd.length)
      session1.sessionPasswd.write(p, 0)

      ClientBuilder()
        .hosts(inst.zookeeperConnectString)
        .sessionTimeout(zkTimeout)
        .sessionId(session1.sessionId)
        .password(Buf.ByteArray.Owned(p))
        .reader()
    }

    val connected = new Promise[Unit]
    val closed = new Promise[Unit]
    session2.state.changes.register(Witness({ ws => ws match {
      case WatchState.SessionState(SessionState.SyncConnected) =>
        connected.setDone()
      case WatchState.SessionState(SessionState.Disconnected) =>
        closed.setDone()
      case _ =>
    }}))

    Await.ready(connected)
    Await.ready(session2.value.close())

    // This will expire the session.
    val session1Expired = session1.state.changes.filter(
      _ == WatchState.SessionState(SessionState.Expired)).toFuture()
    val zkConnected = varZkState.changes.filter(
      _ == WatchState.SessionState(SessionState.SyncConnected)).toFuture()

    Await.ready(session1.getData("/sadfads"))
    Await.ready(session1Expired)
    Await.ready(zkConnected)

    eventually {
      assert((zkStates map { case (s, _) => s }).reverse ==
        Seq(SessionState.SyncConnected, SessionState.Disconnected,
          SessionState.Expired, SessionState.SyncConnected))
    }
    assert(sessions.size == 2)
  }
}
