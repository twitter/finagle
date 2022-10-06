package com.twitter.finagle.serverset2.client.apache

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.serverset2.client._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.io.Buf
import com.twitter.util.Return
import com.twitter.util.Await
import java.util.concurrent.ExecutionException
import org.apache.zookeeper
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{eq => meq}
import org.mockito.Mockito.doNothing
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.OneInstancePerTest
import scala.jdk.CollectionConverters._
import org.scalatest.funsuite.AnyFunSuite

class ApacheZooKeeperTest extends AnyFunSuite with MockitoSugar with OneInstancePerTest {
  val statsReceiver = new InMemoryStatsReceiver
  val mockZK = mock[org.apache.zookeeper.ZooKeeper]
  val watchedZK = {
    val apacheWatcher = new ApacheWatcher(statsReceiver)
    val zkImpl = new ApacheZooKeeper(mockZK)
    val zkStats = new StatsReader with StatsWriter {
      val underlying = zkImpl
      val stats = statsReceiver
    }
    Watched(zkStats, apacheWatcher.state)
  }
  val zk = watchedZK.value

  // Basic data values
  val _children = List("/foo/bar/baz", "/foo/bar/biz")
  val path = "/foo/bar"
  val _data = List[Byte](1, 2, 3, 4).toArray
  val version = 6

  // Dummy argument values
  val data = Buf.ByteArray.Owned(_data)
  val id = Data.Id("scheme", "a")
  val acl = Data.ACL(3, id)
  val acls = List(acl)
  val mode = CreateMode.Persistent
  val ephMode = CreateMode.Ephemeral
  val stat = Data.Stat(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
  val children = Node.Children(_children, stat)

  // Apache ZK Equivalents
  val apacheId = new zookeeper.data.Id("scheme", "a")
  val apacheACL = new zookeeper.data.ACL(3, apacheId)
  val apacheACLS = List(apacheACL).asJava
  val apacheMode = ApacheCreateMode.zk(mode)
  val apacheEphMode = ApacheCreateMode.zk(ephMode)
  val apacheStat = new zookeeper.data.Stat(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
  val apacheChildren = _children.asJava
  val apacheOk = zookeeper.KeeperException.Code.OK.intValue
  val apacheNoNode = zookeeper.KeeperException.Code.NONODE.intValue
  val apacheConnLoss = zookeeper.KeeperException.Code.CONNECTIONLOSS.intValue

  // Argument capture for callbacks in async (Apache) ZK calls
  val stringCB = ArgumentCaptor.forClass(classOf[org.apache.zookeeper.AsyncCallback.StringCallback])
  val dataCB = ArgumentCaptor.forClass(classOf[org.apache.zookeeper.AsyncCallback.DataCallback])
  val voidCB = ArgumentCaptor.forClass(classOf[org.apache.zookeeper.AsyncCallback.VoidCallback])
  val aclCB = ArgumentCaptor.forClass(classOf[org.apache.zookeeper.AsyncCallback.ACLCallback])
  val statCB = ArgumentCaptor.forClass(classOf[org.apache.zookeeper.AsyncCallback.StatCallback])
  val childrenCB =
    ArgumentCaptor.forClass(classOf[org.apache.zookeeper.AsyncCallback.Children2Callback])

  val watcher = ArgumentCaptor.forClass(classOf[ApacheWatcher])

  test("Zk sessionId returns a proper sessionId") {
    when(mockZK.getSessionId).thenReturn(42)
    assert(zk.sessionId == 42)
  }

  test("Zk sessionPasswd returns a proper sessionPasswd") {
    val pw = List[Byte](1, 2, 3, 4).toArray
    when(mockZK.getSessionPasswd).thenReturn(pw)
    assert(zk.sessionPasswd == Buf.ByteArray.Owned(pw))
  }

  test("Zk sessionTimeout returns a proper duration") {
    val timeout = 10.seconds
    when(mockZK.getSessionTimeout).thenReturn(timeout.inMilliseconds.toInt)
    assert(zk.sessionTimeout == timeout)
  }

  test("Zk addAuthInfo submits properly constructed auth") {
    val scheme = "digest"
    val auth = List[Byte](1, 2, 3, 4).toArray
    zk.addAuthInfo(scheme, data)
    verify(mockZK).addAuthInfo(scheme, auth)
  }

  test("Zk getEphemerals raises unimplemented exception") {
    intercept[KeeperException.Unimplemented] {
      Await.result(zk.getEphemerals(), 5.seconds)
    }
  }

  test("Zk close submits properly constructed close") {
    doNothing().when(mockZK).close()

    val closed = zk.close()

    assert(Await.result(closed.liftToTry, 1.second) == Return.Unit)
  }

  test("Zk close handles error conditions") {
    doThrow(new InterruptedException()).when(mockZK).close()

    val closed = zk.close()

    intercept[ExecutionException] {
      Await.result(closed, 5.seconds)
    }
  }

  test("Zk create submits properly constructed empty znode create") {
    val created = zk.create(path, None, List(acl), mode)

    verify(mockZK).create(
      meq(path),
      meq(null),
      meq(apacheACLS),
      meq(apacheMode),
      stringCB.capture,
      meq(null)
    )

    val expected = path + "_expected"
    stringCB.getValue.processResult(apacheOk, path, null, expected)
    assert(Await.result(created, 1.second) == expected)
    assert(statsReceiver.counter("write_successes")() == 1)
  }

  test("Zk create submits properly constructed ephemeral empty znode create") {
    val created = zk.create(path, None, List(acl), ephMode)

    verify(mockZK).create(
      meq(path),
      meq(null),
      meq(apacheACLS),
      meq(apacheEphMode),
      stringCB.capture,
      meq(null)
    )

    val expected = path + "_expected"
    stringCB.getValue.processResult(apacheOk, path, null, expected)
    assert(Await.result(created, 1.second) == expected)
    assert(statsReceiver.counter("ephemeral_successes")() == 1)
  }

  test("Zk create submits properly constructed create") {
    val created = zk.create(path, Some(data), List(acl), mode)

    verify(mockZK).create(
      meq(path),
      meq(_data),
      meq(apacheACLS),
      meq(apacheMode),
      stringCB.capture,
      meq(null)
    )

    val expected = path + "_expected"
    stringCB.getValue.processResult(apacheOk, path, null, expected)
    assert(Await.result(created, 1.second) == expected)
    assert(statsReceiver.counter("write_successes")() == 1)
  }

  test("Zk create handles Zk error") {
    val created = zk.create(path, Some(data), List(acl), mode)

    verify(mockZK).create(
      meq(path),
      meq(_data),
      meq(apacheACLS),
      meq(apacheMode),
      stringCB.capture,
      meq(null)
    )

    stringCB.getValue.processResult(apacheConnLoss, path, null, null)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(created, 5.seconds)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  test("Zk create handles synchronous error") {
    when(
      mockZK.create(
        meq(path),
        meq(_data),
        meq(apacheACLS),
        meq(apacheMode),
        stringCB.capture,
        meq(null)
      )
    ).thenThrow(new IllegalArgumentException)
    val created = zk.create(path, Some(data), List(acl), mode)

    verify(mockZK).create(
      meq(path),
      meq(_data),
      meq(apacheACLS),
      meq(apacheMode),
      stringCB.capture,
      meq(null)
    )

    intercept[IllegalArgumentException] {
      Await.result(created, 5.seconds)
    }
    assert(statsReceiver.counter("write_failures")() == 1)
  }

  test("Zk delete submits properly constructed versioned delete") {
    val deleted = zk.delete(path, Some(version))

    verify(mockZK).delete(meq(path), meq(version), voidCB.capture, meq(null))

    voidCB.getValue.processResult(apacheOk, path, null)
    assert(Await.result(deleted.liftToTry, 1.second) == Return.Unit)
    assert(statsReceiver.counter("write_successes")() == 1)
  }

  test("Zk delete submits properly constructed unversioned delete") {
    val deleted = zk.delete(path, None)

    verify(mockZK).delete(meq(path), meq(-1), voidCB.capture, meq(null))

    voidCB.getValue.processResult(apacheOk, path, null)
    assert(Await.result(deleted.liftToTry, 1.second) == Return.Unit)
    assert(statsReceiver.counter("write_successes")() == 1)
  }

  test("Zk delete handles Zk error") {
    val deleted = zk.delete(path, Some(version))

    verify(mockZK).delete(meq(path), meq(version), voidCB.capture, meq(null))

    voidCB.getValue.processResult(apacheConnLoss, path, null)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(deleted, 5.seconds)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  test("Zk exists submits properly constructed exists") {
    val existed = zk.exists(path)

    verify(mockZK).exists(meq(path), meq(null), statCB.capture, meq(null))

    statCB.getValue.processResult(apacheOk, path, null, apacheStat)
    assert(Await.result(existed, 1.second) == Some(stat))
    assert(statsReceiver.counter("read_successes")() == 1)
  }

  test("Zk exists handles missing node") {
    val existed = zk.exists(path)

    verify(mockZK).exists(meq(path), meq(null), statCB.capture, meq(null))

    statCB.getValue.processResult(apacheNoNode, path, null, null)
    assert(Await.result(existed, 1.second) == None)
    assert(statsReceiver.counter("read_successes")() == 1)
  }

  test("Zk exists handles Zk error") {
    val existed = zk.exists(path)

    verify(mockZK).exists(meq(path), meq(null), statCB.capture, meq(null))

    statCB.getValue.processResult(apacheConnLoss, path, null, null)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(existed, 5.seconds)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  test("Zk exists handles synchronous error") {
    when(mockZK.exists(meq(path), meq(null), statCB.capture, meq(null)))
      .thenThrow(new IllegalArgumentException)

    val existed = zk.exists(path)

    verify(mockZK).exists(meq(path), meq(null), statCB.capture, meq(null))

    intercept[IllegalArgumentException] {
      Await.result(existed, 5.seconds)
    }
    assert(statsReceiver.counter("read_failures")() == 1)
  }

  test("Zk existsWatch submits properly constructed exists") {
    val existed = zk.existsWatch(path)

    verify(mockZK).exists(meq(path), watcher.capture, statCB.capture, meq(null))

    statCB.getValue.processResult(apacheOk, path, null, apacheStat)
    assert(Await.result(existed, 1.second).value == Some(stat))
    assert(statsReceiver.counter("watch_successes")() == 1)
  }

  test("Zk existsWatch handles missing node") {
    val existed = zk.existsWatch(path)

    verify(mockZK).exists(meq(path), watcher.capture, statCB.capture, meq(null))

    statCB.getValue.processResult(apacheNoNode, path, null, apacheStat)
    assert(Await.result(existed, 1.second).value == None)
    assert(statsReceiver.counter("watch_successes")() == 1)
  }

  test("Zk existsWatch handles Zk error") {
    val existed = zk.existsWatch(path)

    verify(mockZK).exists(meq(path), watcher.capture, statCB.capture, meq(null))

    statCB.getValue.processResult(apacheConnLoss, path, null, null)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(existed, 5.seconds)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  test("Zk existsWatch handles synchronous error") {
    when(mockZK.exists(meq(path), watcher.capture, statCB.capture, meq(null)))
      .thenThrow(new IllegalArgumentException)
    val existed = zk.existsWatch(path)

    verify(mockZK).exists(meq(path), watcher.capture, statCB.capture, meq(null))

    intercept[IllegalArgumentException] {
      Await.result(existed, 5.seconds)
    }
    assert(statsReceiver.counter("watch_failures")() == 1)
  }

  test("Zk getData submits properly constructed getData") {
    val nodeData = zk.getData(path)

    verify(mockZK).getData(meq(path), meq(null), dataCB.capture, meq(null))

    dataCB.getValue.processResult(apacheOk, path, null, _data, apacheStat)
    assert(Await.result(nodeData, 1.second) == Node.Data(Some(data), stat))
    assert(statsReceiver.counter("read_successes")() == 1)
  }

  test("Zk getData handles empty znodes") {
    val nodeData = zk.getData(path)

    verify(mockZK).getData(meq(path), meq(null), dataCB.capture, meq(null))

    dataCB.getValue.processResult(apacheOk, path, null, null, apacheStat)
    assert(Await.result(nodeData, 1.second) == Node.Data(None, stat))
    assert(statsReceiver.counter("read_successes")() == 1)
  }

  test("Zk getData handles Zk error") {
    val nodeData = zk.getData(path)

    verify(mockZK).getData(meq(path), meq(null), dataCB.capture, meq(null))

    dataCB.getValue.processResult(apacheConnLoss, path, null, null, null)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(nodeData, 5.seconds)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  test("Zk getData handles synchronous error") {
    when(mockZK.getData(meq(path), meq(null), dataCB.capture, meq(null)))
      .thenThrow(new IllegalArgumentException)
    val nodeData = zk.getData(path)

    verify(mockZK).getData(meq(path), meq(null), dataCB.capture, meq(null))

    intercept[IllegalArgumentException] {
      Await.result(nodeData, 5.seconds)
    }
    assert(statsReceiver.counter("read_failures")() == 1)
  }

  test("Zk getDataWatch submits properly constructed getData") {
    val nodeDataWatch = zk.getDataWatch(path)

    verify(mockZK).getData(meq(path), watcher.capture, dataCB.capture, meq(null))

    dataCB.getValue.processResult(apacheOk, path, null, _data, apacheStat)
    assert(Await.result(nodeDataWatch, 1.second).value == Node.Data(Some(data), stat))
    assert(statsReceiver.counter("watch_successes")() == 1)
  }

  test("Zk getDataWatch handles empty znodes") {
    val nodeDataWatch = zk.getDataWatch(path)

    verify(mockZK).getData(meq(path), watcher.capture, dataCB.capture, meq(null))

    dataCB.getValue.processResult(apacheOk, path, null, null, apacheStat)
    assert(Await.result(nodeDataWatch, 1.second).value == Node.Data(None, stat))
    assert(statsReceiver.counter("watch_successes")() == 1)
  }

  test("Zk getDataWatch handles Zk error") {
    val nodeDataWatch = zk.getDataWatch(path)

    verify(mockZK).getData(meq(path), watcher.capture, dataCB.capture, meq(null))

    dataCB.getValue.processResult(apacheConnLoss, path, null, null, null)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(nodeDataWatch, 5.seconds)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  test("Zk getDataWatch handles synchronous error") {
    when(mockZK.getData(meq(path), watcher.capture, dataCB.capture, meq(null)))
      .thenThrow(new IllegalArgumentException)
    val nodeDataWatch = zk.getDataWatch(path)

    verify(mockZK).getData(meq(path), watcher.capture, dataCB.capture, meq(null))

    intercept[IllegalArgumentException] {
      Await.result(nodeDataWatch, 5.seconds)
    }
    assert(statsReceiver.counter("watch_failures")() == 1)
  }

  test("Zk setData submits properly constructed versioned setData") {
    val nodeStat = zk.setData(path, Some(data), Some(version))

    verify(mockZK).setData(meq(path), meq(_data), meq(version), statCB.capture, meq(null))

    statCB.getValue.processResult(apacheOk, path, null, apacheStat)
    assert(Await.result(nodeStat, 1.second) == stat)
    assert(statsReceiver.counter("write_successes")() == 1)
  }

  test("Zk setData submits properly constructed unversioned setData") {
    val nodeStat = zk.setData(path, Some(data), None)

    verify(mockZK).setData(meq(path), meq(_data), meq(-1), statCB.capture, meq(null))

    statCB.getValue.processResult(apacheOk, path, null, apacheStat)
    assert(Await.result(nodeStat, 1.second) == stat)
    assert(statsReceiver.counter("write_successes")() == 1)
  }

  test("Zk setData submits properly constructed unversioned empty znode setData") {
    val nodeStat = zk.setData(path, None, None)

    verify(mockZK).setData(meq(path), meq(null), meq(-1), statCB.capture, meq(null))

    statCB.getValue.processResult(apacheOk, path, null, apacheStat)
    assert(Await.result(nodeStat, 1.second) == stat)
    assert(statsReceiver.counter("write_successes")() == 1)
  }

  test("Zk setData handles ZK error") {
    val nodeStat = zk.setData(path, Some(data), Some(version))

    verify(mockZK).setData(meq(path), meq(_data), meq(version), statCB.capture, meq(null))

    statCB.getValue.processResult(apacheConnLoss, path, null, null)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(nodeStat, 5.seconds)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  test("Zk setData handles synchronous error") {
    when(mockZK.setData(meq(path), meq(_data), meq(version), statCB.capture, meq(null)))
      .thenThrow(new IllegalArgumentException)
    val nodeStat = zk.setData(path, Some(data), Some(version))

    verify(mockZK).setData(meq(path), meq(_data), meq(version), statCB.capture, meq(null))

    intercept[IllegalArgumentException] {
      Await.result(nodeStat, 5.seconds)
    }
    assert(statsReceiver.counter("write_failures")() == 1)
  }

  test("Zk getACL submits properly constructed getACL") {
    val nodeACL = zk.getACL(path)

    verify(mockZK).getACL(meq(path), meq(null), aclCB.capture, meq(null))

    aclCB.getValue.processResult(apacheOk, path, null, apacheACLS, apacheStat)
    assert(Await.result(nodeACL, 1.second) == Node.ACL(acls, stat))
    assert(statsReceiver.counter("read_successes")() == 1)
  }

  test("Zk getACL handles Zk error") {
    val nodeACL = zk.getACL(path)

    verify(mockZK).getACL(meq(path), meq(null), aclCB.capture, meq(null))

    aclCB.getValue.processResult(apacheConnLoss, path, null, null, null)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(nodeACL, 5.seconds)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  test("Zk getACL handles synchronous error") {
    when(mockZK.getACL(meq(path), meq(null), aclCB.capture, meq(null)))
      .thenThrow(new IllegalArgumentException)
    val nodeACL = zk.getACL(path)

    verify(mockZK).getACL(meq(path), meq(null), aclCB.capture, meq(null))

    intercept[IllegalArgumentException] {
      Await.result(nodeACL, 5.seconds)
    }
    assert(statsReceiver.counter("read_failures")() == 1)
  }

  test("Zk setACL submits properly constructed versioned setACL") {
    val nodeStat = zk.setACL(path, acls, Some(version))

    verify(mockZK).setACL(meq(path), meq(apacheACLS), meq(version), statCB.capture, meq(null))

    statCB.getValue.processResult(apacheOk, path, null, apacheStat)
    assert(Await.result(nodeStat, 1.second) == stat)
    assert(statsReceiver.counter("write_successes")() == 1)
  }

  test("Zk setACL submits properly constructed unversioned setACL") {
    val nodeStat = zk.setACL(path, acls, None)

    verify(mockZK).setACL(meq(path), meq(apacheACLS), meq(-1), statCB.capture, meq(null))

    statCB.getValue.processResult(apacheOk, path, null, apacheStat)
    assert(Await.result(nodeStat, 1.second) == stat)
    assert(statsReceiver.counter("write_successes")() == 1)
  }

  test("Zk setACL handles Zk error") {
    val nodeStat = zk.setACL(path, acls, Some(version))

    verify(mockZK).setACL(meq(path), meq(apacheACLS), meq(version), statCB.capture, meq(null))

    statCB.getValue.processResult(apacheConnLoss, path, null, apacheStat)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(nodeStat, 5.seconds)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  test("Zk setACL handles synchronous error") {
    when(mockZK.setACL(meq(path), meq(apacheACLS), meq(version), statCB.capture, meq(null)))
      .thenThrow(new IllegalArgumentException)
    val nodeStat = zk.setACL(path, acls, Some(version))

    verify(mockZK).setACL(meq(path), meq(apacheACLS), meq(version), statCB.capture, meq(null))

    intercept[IllegalArgumentException] {
      Await.result(nodeStat, 5.seconds)
    }
    assert(statsReceiver.counter("write_failures")() == 1)
  }

  test("Zk getChildren submits properly constructed getChildren") {
    val nodeChildren = zk.getChildren(path)

    verify(mockZK).getChildren(meq(path), meq(null), childrenCB.capture, meq(null))

    childrenCB.getValue.processResult(apacheOk, path, null, apacheChildren, apacheStat)
    assert(Await.result(nodeChildren, 1.second) == children)
    assert(statsReceiver.counter("read_successes")() == 1)
  }

  test("Zk getChildren handles Zk error") {
    val nodeChildren = zk.getChildren(path)

    verify(mockZK).getChildren(meq(path), meq(null), childrenCB.capture, meq(null))

    childrenCB.getValue.processResult(apacheConnLoss, path, null, apacheChildren, apacheStat)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(nodeChildren, 5.seconds)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  test("Zk getChildren handles synchronous error") {
    when(mockZK.getChildren(meq(path), meq(null), childrenCB.capture, meq(null)))
      .thenThrow(new IllegalArgumentException)
    val nodeChildren = zk.getChildren(path)

    verify(mockZK).getChildren(meq(path), meq(null), childrenCB.capture, meq(null))

    intercept[IllegalArgumentException] {
      Await.result(nodeChildren, 5.seconds)
    }
    assert(statsReceiver.counter("read_failures")() == 1)
  }

  test("Zk getChildrenWatch submits properly constructed getChildren") {
    val nodeChildren = zk.getChildrenWatch(path)

    verify(mockZK).getChildren(meq(path), watcher.capture, childrenCB.capture, meq(null))

    childrenCB.getValue.processResult(apacheOk, path, null, apacheChildren, apacheStat)
    assert(Await.result(nodeChildren, 1.second).value == children)
    assert(statsReceiver.counter("watch_successes")() == 1)
  }

  test("Zk getChildrenWatch handles Zk error") {
    val nodeChildren = zk.getChildrenWatch(path)

    verify(mockZK).getChildren(meq(path), watcher.capture, childrenCB.capture, meq(null))

    childrenCB.getValue.processResult(apacheConnLoss, path, null, apacheChildren, apacheStat)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(nodeChildren, 5.seconds)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  test("Zk getChildrenWatch handles synchronous error") {
    when(mockZK.getChildren(meq(path), watcher.capture, childrenCB.capture, meq(null)))
      .thenThrow(new IllegalArgumentException)
    val nodeChildren = zk.getChildrenWatch(path)

    verify(mockZK).getChildren(meq(path), watcher.capture, childrenCB.capture, meq(null))

    intercept[IllegalArgumentException] {
      Await.result(nodeChildren, 5.seconds)
    }
    assert(statsReceiver.counter("watch_failures")() == 1)
  }

  test("Zk sync submits properly constructed sync") {
    val synced = zk.sync(path)

    verify(mockZK).sync(
      meq(path),
      voidCB.capture,
      meq(null)
    )

    voidCB.getValue.processResult(apacheOk, path, null)
    assert(Await.result(synced.liftToTry, 1.second) == Return.Unit)
    assert(statsReceiver.counter("read_successes")() == 1)
  }

  test("Zk sync handles Zk error") {
    val synced = zk.sync(path)

    verify(mockZK).sync(
      meq(path),
      voidCB.capture,
      meq(null)
    )

    voidCB.getValue.processResult(apacheConnLoss, path, null)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(synced, 5.seconds)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  test("Zk sync handles synchronous error") {
    when(
      mockZK.sync(
        meq(path),
        voidCB.capture,
        meq(null)
      )
    ).thenThrow(new IllegalArgumentException)
    val synced = zk.sync(path)

    verify(mockZK).sync(
      meq(path),
      voidCB.capture,
      meq(null)
    )

    intercept[IllegalArgumentException] {
      Await.result(synced, 5.seconds)
    }
    assert(statsReceiver.counter("read_failures")() == 1)
  }
}
