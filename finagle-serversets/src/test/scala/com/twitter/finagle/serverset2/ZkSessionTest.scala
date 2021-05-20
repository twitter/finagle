package com.twitter.finagle.serverset2

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.serverset2.client._
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.io.Buf
import com.twitter.util._
import java.util.concurrent.atomic.AtomicReference
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import scala.collection.immutable
import org.scalatest.funsuite.AnyFunSuite

sealed private trait ZkOp { type Res; val res = new Promise[Res] }
private object ZkOp {
  case class Exists(path: String) extends ZkOp {
    type Res = Option[Data.Stat]
  }

  case class ExistsWatch(path: String) extends ZkOp {
    type Res = Watched[Option[Data.Stat]]
  }

  case class GetChildren(path: String) extends ZkOp {
    type Res = Node.Children
  }

  case class GetChildrenWatch(path: String) extends ZkOp {
    type Res = Watched[Node.Children]
  }

  case class GlobWatch(pat: String) extends ZkOp {
    type Res = Watched[Seq[String]]
  }

  case class GetData(path: String) extends ZkOp {
    type Res = Node.Data
  }

  case class GetDataWatch(path: String) extends ZkOp {
    type Res = Watched[Node.Data]
  }

  case class GetEphemerals() extends ZkOp {
    type Res = Seq[String]
  }

  case class Sync(path: String) extends ZkOp {
    type Res = Unit
  }

  case class Close(deadline: Time) extends ZkOp {
    type Res = Unit
  }

  case class AddAuthInfo(scheme: String, auth: Buf) extends ZkOp {
    type Res = Unit
  }
}

private class OpqueueZkReader(
  val sessionId: Long,
  val sessionPasswd: Buf,
  val sessionTimeout: Duration)
    extends ZooKeeperReader {

  import ZkOp._

  def this() = this(0, Buf.Empty, Duration.Zero)

  @volatile var opq: immutable.Queue[ZkOp] = immutable.Queue.empty

  private def enqueue(op: ZkOp): Future[op.Res] = synchronized {
    opq = opq enqueue op
    op.res
  }

  def exists(path: String) = enqueue(Exists(path))
  def existsWatch(path: String) = enqueue(ExistsWatch(path))

  def getChildren(path: String) = enqueue(GetChildren(path))
  def getChildrenWatch(path: String) = enqueue(GetChildrenWatch(path))

  def globPrefixWatch(pat: String) = enqueue(GlobWatch(pat))

  def getData(path: String) = enqueue(GetData(path))
  def getDataWatch(path: String) = enqueue(GetDataWatch(path))

  def getEphemerals() = enqueue(GetEphemerals())

  def sync(path: String) = enqueue(Sync(path))
  def close(deadline: Time) = enqueue(Close(deadline))

  def addAuthInfo(scheme: String, auth: Buf) =
    enqueue(AddAuthInfo(scheme, auth))

  def getACL(path: String): Future[Node.ACL] = Future.never
}

class ZkSessionTest extends AnyFunSuite with Eventually with IntegrationPatience {

  import ZkOp._

  val retryStream = RetryStream()

  test("ops retry safely") {
    Time.withCurrentTimeFrozen { tc =>
      implicit val timer = new MockTimer
      val watchedZk = Watched(new OpqueueZkReader(), Var(WatchState.Pending))
      val zk = new ZkSession(retryStream, watchedZk, NullStatsReceiver)

      val v = zk.existsOf("/foo/bar")
      // An unobserved Var makes no side effect.
      assert(watchedZk.value.opq.isEmpty)
      val ref = new AtomicReference[Activity.State[Option[Data.Stat]]]
      val o = v.states.register(Witness(ref))
      assert(watchedZk.value.opq == Seq(ExistsWatch("/foo/bar")))
      assert(ref.get == Activity.Pending)

      assert(timer.tasks.isEmpty)
      watchedZk.value.opq(0).res() = Throw(new KeeperException.ConnectionLoss(None))
      assert(timer.tasks.size == 1)
      tc.advance(20.milliseconds)
      timer.tick()
      assert(watchedZk.value.opq == Seq(ExistsWatch("/foo/bar"), ExistsWatch("/foo/bar")))
      assert(ref.get == Activity.Pending)

      watchedZk.value.opq(1).res() = Throw(new KeeperException.SessionExpired(None))
      assert(watchedZk.value.opq == Seq(ExistsWatch("/foo/bar"), ExistsWatch("/foo/bar")))
      val Activity.Failed(exc) = ref.get
      assert(exc.isInstanceOf[KeeperException.SessionExpired])
    }
  }

  test("ZkSession.globOf") {
    Time.withCurrentTimeFrozen { tc =>
      implicit val timer = new MockTimer
      val watchedZk = Watched(new OpqueueZkReader(), Var(WatchState.Pending))
      val zk = new ZkSession(retryStream, watchedZk, NullStatsReceiver)

      val v = zk.globOf("/foo/bar/")
      val ref = new AtomicReference[Activity.State[Set[String]]]
      v.states.register(Witness(ref))
      assert(ref.get == Activity.Pending)

      val Seq(ew @ ExistsWatch("/foo/bar")) = watchedZk.value.opq
      val ewwatchv = Var[WatchState](WatchState.Pending)
      ew.res() = Return(Watched(None, ewwatchv))
      assert(watchedZk.value.opq == Seq(ExistsWatch("/foo/bar")))
      assert(ref.get == Activity.Ok(Set.empty))

      ewwatchv() = WatchState.Determined(NodeEvent.ChildrenChanged)
      val Seq(`ew`, ew2 @ ExistsWatch("/foo/bar")) = watchedZk.value.opq
      assert(ref.get == Activity.Ok(Set.empty))
      val ew2watchv = Var[WatchState](WatchState.Pending)
      ew2.res() = Return(Watched(Some(Data.Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)), ew2watchv))
      val Seq(`ew`, `ew2`, gw @ GetChildrenWatch("/foo/bar")) = watchedZk.value.opq
      assert(ref.get == Activity.Pending)
      gw.res() =
        Return(Watched(Node.Children(Seq("a", "b", "c"), null), Var.value(WatchState.Pending)))
      assert(ref.get == Activity.Ok(Set("a", "b", "c")))
      assert(watchedZk.value.opq == Seq(ew, ew2, gw))

      ew2watchv() = WatchState.Determined(NodeEvent.ChildrenChanged)
      val Seq(`ew`, `ew2`, `gw`, ew3 @ ExistsWatch("/foo/bar")) = watchedZk.value.opq
      ew3.res() = Return(Watched(None, Var.value(WatchState.Pending)))
      assert(ref.get == Activity.Ok(Set.empty))
    }
  }

  test("factory retries when ZK session fails to initialize") {
    Time.withCurrentTimeFrozen { tc =>
      val identity = Identities.get().head
      val authInfo = "%s:%s".format(identity, identity)
      implicit val timer = new MockTimer

      // A normal `ZkSession` with updatable state.
      val zkState: Var[WatchState] with Updatable[WatchState] = Var(WatchState.Pending)
      val watchedZk = Watched(new OpqueueZkReader(), zkState)

      // A failed `ZkSession`.
      val failedZk = Watched(
        new OpqueueZkReader(),
        Var(WatchState.FailedToInitialize(new Exception("failed")))
      )

      // Return failed session on the first invocation.
      var failed = true
      val zk = ZkSession.retrying(
        retryStream,
        () => {
          if (failed) {
            failed = false
            new ZkSession(retryStream, failedZk, NullStatsReceiver)
          } else new ZkSession(retryStream, watchedZk, NullStatsReceiver)
        }
      )

      zk.changes.respond {
        case _ => ()
      }

      // The underlying session is in a failed state here. Ensure we haven't updated the `Var` yet.
      assert(zk.sample() == ZkSession.nil)

      // Advance the timer to allow `reconnect` to run.
      tc.advance(10.seconds)
      timer.tick()

      // The underlying `ZkSession` should be set to `watchedZk` now.
      // Update the session state to connected -- we should receive auth info.
      zkState() = WatchState.SessionState(SessionState.SyncConnected)
      eventually {
        assert(
          watchedZk.value.opq == Seq(AddAuthInfo("digest", Buf.Utf8(authInfo)))
        )
      }
    }
  }

  test("factory authenticates and closes on expiry") {
    Time.withCurrentTimeFrozen { tc =>
      val identity = Identities.get().head
      val authInfo = "%s:%s".format(identity, identity)
      implicit val timer = new MockTimer
      val zkState: Var[WatchState] with Updatable[WatchState] = Var(WatchState.Pending)
      val watchedZk = Watched(new OpqueueZkReader(), zkState)
      val zk = ZkSession.retrying(
        retryStream,
        () => new ZkSession(retryStream, watchedZk, NullStatsReceiver)
      )

      zk.changes.respond {
        case _ => ()
      }

      zkState() = WatchState.SessionState(SessionState.SyncConnected)
      eventually {
        assert(watchedZk.value.opq == Seq(AddAuthInfo("digest", Buf.Utf8(authInfo))))
      }

      zkState() = WatchState.SessionState(SessionState.Expired)
      tc.advance(10.seconds)
      timer.tick()
      eventually {
        assert(
          watchedZk.value.opq == Seq(
            AddAuthInfo("digest", Buf.Utf8(authInfo)),
            Close(Time.Bottom)
          )
        )
      }

      zkState() = WatchState.SessionState(SessionState.SyncConnected)
      eventually {
        assert(
          watchedZk.value.opq == Seq(
            AddAuthInfo("digest", Buf.Utf8(authInfo)),
            Close(Time.Bottom),
            AddAuthInfo("digest", Buf.Utf8(authInfo))
          )
        )
      }
    }
  }
}
