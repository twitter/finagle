package com.twitter.finagle.serverset2

import collection.immutable
import com.twitter.conversions.time._
import com.twitter.finagle.MockTimer
import com.twitter.io.Buf
import com.twitter.util.{Future, Promise, Duration, Var, Timer, Time, Throw, Return}
import java.util.concurrent.atomic.AtomicReference
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.data.Stat
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

sealed private trait ZkOp { type Res; val res = new Promise[Res] }
private object ZkOp {
  case class Exists(path: String) 
      extends { type Res = Option[Stat] }
      with ZkOp

  case class ExistsWatch(path: String)
      extends { type Res = Zk#Watched[Option[Stat]] } 
      with ZkOp

  case class GetChildren(path: String)
      extends { type Res = Seq[String] } 
      with ZkOp

  case class GetChildrenWatch(path: String)
      extends { type Res = Zk#Watched[Seq[String]] } 
      with ZkOp

  case class GetData(path: String)
      extends { type Res = (Stat, Buf) } 
      with ZkOp

  case class GetDataWatch(path: String)
      extends { type Res = Zk#Watched[(Stat, Buf)] } 
      with ZkOp

  case class Sync(path: String)
      extends { type Res = Unit } 
      with ZkOp

  case class Close()
    extends { type Res = Unit }
    with ZkOp
}

private class OpqueueZk(
    timerIn: Timer,
    val sessionId: Long, 
    val sessionPasswd: Buf,
    val sessionTimeout: Duration, 
    val state: Var[WatchState]) extends Zk {

  def this(timer: Timer) = this(
    timer, 0, Buf.Empty,
    Duration.Zero, Var.value(WatchState.Pending))

  import ZkOp._
  
  protected[serverset2] implicit val timer = timerIn

  @volatile var opq: immutable.Queue[ZkOp] = immutable.Queue.empty

  private def enqueue(op: ZkOp): Future[op.Res] = synchronized {
    opq = opq enqueue op
    op.res
  }

  def exists(path: String) = enqueue(Exists(path))
  def existsWatch(path: String) = enqueue(ExistsWatch(path))

  def getChildren(path: String) = enqueue(GetChildren(path))
  def getChildrenWatch(path: String) = enqueue(GetChildrenWatch(path))

  def getData(path: String) = enqueue(GetData(path))
  def getDataWatch(path: String) = enqueue(GetDataWatch(path))

  def sync(path: String) = enqueue(Sync(path))
  def close() = enqueue(Close())
}

@RunWith(classOf[JUnitRunner])
class ZkTest extends FunSuite {
  import ZkOp._

  test("ops retry safely") { Time.withCurrentTimeFrozen { tc =>
    val timer = new MockTimer
    val zk = new OpqueueZk(timer)

    val v = zk.existsOf("/foo/bar")
    // An unobserved Var makes no side effect.
    assert(zk.opq.isEmpty)
    val ref = new AtomicReference[Op[Option[Stat]]]
    val o = v.observeTo(ref)
    assert(zk.opq === Seq(ExistsWatch("/foo/bar")))
    assert(ref.get === Op.Pending)
    
    assert(timer.tasks.isEmpty)
    zk.opq(0).res() = Throw(new KeeperException.ConnectionLossException)
    assert(timer.tasks.size === 1)
    tc.advance(10.milliseconds)
    timer.tick()
    assert(zk.opq === Seq(ExistsWatch("/foo/bar"), ExistsWatch("/foo/bar")))
    assert(ref.get === Op.Pending)
    
    zk.opq(1).res() = Throw(new KeeperException.SessionExpiredException)
    assert(zk.opq === Seq(ExistsWatch("/foo/bar"), ExistsWatch("/foo/bar")))
    val Op.Fail(exc) = ref.get
    assert(exc.isInstanceOf[KeeperException.SessionExpiredException])
  }}
  
  test("Zk.childrenOf") { Time.withCurrentTimeFrozen { tc =>
    val timer = new MockTimer
    val zk = new OpqueueZk(timer)
    
    val v = zk.childrenOf("/foo/bar")
    val ref = new AtomicReference[Op[Set[String]]]
    v.observeTo(ref)
    assert(ref.get === Op.Pending)
    
    val Seq(ew@ExistsWatch("/foo/bar")) = zk.opq
    val ewwatchv = Var[WatchState](WatchState.Pending)
    ew.res() = Return((None, ewwatchv))
    assert(zk.opq === Seq(ExistsWatch("/foo/bar")))
    assert(ref.get === Op.Ok(Set.empty))

    ewwatchv() = WatchState.Determined
    val Seq(`ew`, ew2@ExistsWatch("/foo/bar")) = zk.opq
    assert(ref.get === Op.Ok(Set.empty))
    val ew2watchv = Var[WatchState](WatchState.Pending)
    ew2.res() = Return((Some(new Stat), ew2watchv))
    val Seq(`ew`, `ew2`, gcw@GetChildrenWatch("/foo/bar")) = zk.opq
    assert(ref.get === Op.Pending)
    gcw.res() = Return(Seq("a", "b", "c"), Var.value(WatchState.Pending))
    assert(ref.get === Op.Ok(Set("a", "b", "c")))
    assert(zk.opq === Seq(ew, ew2, gcw))

    ew2watchv() = WatchState.Determined
    val Seq(`ew`, `ew2`, `gcw`, ew3@ExistsWatch("/foo/bar")) = zk.opq
    ew3.res() = Return((None, Var.value(WatchState.Pending)))
    assert(ref.get === Op.Ok(Set.empty))
  }}
}
