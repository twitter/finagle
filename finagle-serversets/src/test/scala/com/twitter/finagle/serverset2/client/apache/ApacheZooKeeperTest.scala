package com.twitter.finagle.serverset2.client.apache

import com.twitter.conversions.time._
import com.twitter.finagle.serverset2.client._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.io.Buf
import com.twitter.util.Await
import java.util.concurrent.ExecutionException
import org.apache.zookeeper
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.{eq => meq}
import org.mockito.Mockito.{doNothing, doThrow, verify, when}
import org.scalatest.{FlatSpec, OneInstancePerTest}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class ApacheZooKeeperTest extends FlatSpec with MockitoSugar with OneInstancePerTest {
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
  val data = Buf.ByteArray(_data)
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

  "sessionId" should "return proper sessionId" in {
    when(mockZK.getSessionId).thenReturn(42)
    assert(zk.sessionId == 42)
  }

  "sessionPasswd" should "return proper sessionPasswd" in {
    val pw = List[Byte](1, 2, 3, 4).toArray
    when(mockZK.getSessionPasswd).thenReturn(pw)
    assert(zk.sessionPasswd == Buf.ByteArray(pw))
  }

  "sessionTimeout" should "return proper duration" in {
    val timeout = 10.seconds
    when(mockZK.getSessionTimeout).thenReturn(timeout.inMilliseconds.toInt)
    assert(zk.sessionTimeout == timeout)
  }

  "addAuthInfo" should "submit properly constructed auth" in {
    val scheme = "digest"
    val auth = List[Byte](1, 2, 3, 4).toArray
    zk.addAuthInfo(scheme, data)
    verify(mockZK).addAuthInfo(scheme, auth)
  }

  "getEphemerals" should "raise unimplemented exception" in {
    intercept[KeeperException.Unimplemented] {
      Await.result(zk.getEphemerals())
    }
  }

  "close" should "submit properly constructed close" in {
    doNothing().when(mockZK).close()

    val closed = zk.close()

    assert(Await.result(closed) == ())
  }

  "close" should "handle error conditions" in {
    doThrow(new InterruptedException()).when(mockZK).close()

    val closed = zk.close()

    intercept[ExecutionException] {
      Await.result(closed, 5.seconds)
    }
  }

  "create" should "submit properly constructed empty znode create" in {
    val created = zk.create(path, None, List(acl), mode)

    verify(mockZK).create(
      meq(path),
      meq(null),
      meq(apacheACLS),
      meq(apacheMode),
      stringCB.capture,
      meq(null))

    val expected = path + "_expected"
    stringCB.getValue.processResult(apacheOk, path, null, expected)
    assert(Await.result(created) == expected)
    assert(statsReceiver.counter("write_successes")() == 1)
  }

  "create" should "submit properly constructed ephemeral empty znode create" in {
    val created = zk.create(path, None, List(acl), ephMode)

    verify(mockZK).create(
      meq(path),
      meq(null),
      meq(apacheACLS),
      meq(apacheEphMode),
      stringCB.capture,
      meq(null))

    val expected = path + "_expected"
    stringCB.getValue.processResult(apacheOk, path, null, expected)
    assert(Await.result(created) == expected)
    assert(statsReceiver.counter("ephemeral_successes")() == 1)
  }

  "create" should "submit properly constructed create" in {
    val created = zk.create(path, Some(data), List(acl), mode)

    verify(mockZK).create(
      meq(path),
      meq(_data),
      meq(apacheACLS),
      meq(apacheMode),
      stringCB.capture,
      meq(null))

    val expected = path + "_expected"
    stringCB.getValue.processResult(apacheOk, path, null, expected)
    assert(Await.result(created) == expected)
    assert(statsReceiver.counter("write_successes")() == 1)
  }

  "create" should "handle ZK error" in {
    val created = zk.create(path, Some(data), List(acl), mode)

    verify(mockZK).create(
      meq(path),
      meq(_data),
      meq(apacheACLS),
      meq(apacheMode),
      stringCB.capture,
      meq(null))

    stringCB.getValue.processResult(apacheConnLoss, path, null, null)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(created)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  "create" should "handle synchronous error" in {
    when(mockZK.create(
      meq(path),
      meq(_data),
      meq(apacheACLS),
      meq(apacheMode),
      stringCB.capture,
      meq(null))
    ).thenThrow(new IllegalArgumentException)
    val created = zk.create(path, Some(data), List(acl), mode)

    verify(mockZK).create(
      meq(path),
      meq(_data),
      meq(apacheACLS),
      meq(apacheMode),
      stringCB.capture,
      meq(null))

    intercept[IllegalArgumentException] {
      Await.result(created)
    }
    assert(statsReceiver.counter("write_failures")() == 1)
  }

  "delete" should "submit properly constructed versioned delete" in {
    val deleted = zk.delete(path, Some(version))

    verify(mockZK).delete(
      meq(path),
      meq(version),
      voidCB.capture,
      meq(null))

    voidCB.getValue.processResult(apacheOk, path, null)
    assert(Await.result(deleted) == ())
    assert(statsReceiver.counter("write_successes")() == 1)
  }

  "delete" should "submit properly constructed unversioned delete" in {
    val deleted = zk.delete(path, None)

    verify(mockZK).delete(
      meq(path),
      meq(-1),
      voidCB.capture,
      meq(null))

    voidCB.getValue.processResult(apacheOk, path, null)
    assert(Await.result(deleted) == ())
    assert(statsReceiver.counter("write_successes")() == 1)
  }

  "delete" should "handle ZK error" in {
    val deleted = zk.delete(path, Some(version))

    verify(mockZK).delete(
      meq(path),
      meq(version),
      voidCB.capture,
      meq(null))

    voidCB.getValue.processResult(apacheConnLoss, path, null)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(deleted)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  "exists" should "submit properly constructed exists" in {
    val existed = zk.exists(path)

    verify(mockZK).exists(
      meq(path),
      meq(null),
      statCB.capture,
      meq(null))

    statCB.getValue.processResult(apacheOk, path, null, apacheStat)
    assert(Await.result(existed) == Some(stat))
    assert(statsReceiver.counter("read_successes")() == 1)
  }

  "exists" should "handle missing node" in {
    val existed = zk.exists(path)

    verify(mockZK).exists(
      meq(path),
      meq(null),
      statCB.capture,
      meq(null))

    statCB.getValue.processResult(apacheNoNode, path, null, null)
    assert(Await.result(existed) == None)
    assert(statsReceiver.counter("read_successes")() == 1)
  }

  "exists" should "handle ZK error" in {
    val existed = zk.exists(path)

    verify(mockZK).exists(
      meq(path),
      meq(null),
      statCB.capture,
      meq(null))

    statCB.getValue.processResult(apacheConnLoss, path, null, null)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(existed)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  "exists" should "handle synchronous error" in {
    when(mockZK.exists(
      meq(path),
      meq(null),
      statCB.capture,
      meq(null))
    ).thenThrow(new IllegalArgumentException)

    val existed = zk.exists(path)

    verify(mockZK).exists(
      meq(path),
      meq(null),
      statCB.capture,
      meq(null))

    intercept[IllegalArgumentException] {
      Await.result(existed)
    }
    assert(statsReceiver.counter("read_failures")() == 1)
  }

  "existsWatch" should "submit properly constructed exists" in {
    val existed = zk.existsWatch(path)

    verify(mockZK).exists(
      meq(path),
      watcher.capture,
      statCB.capture,
      meq(null))

    statCB.getValue.processResult(apacheOk, path, null, apacheStat)
    assert(Await.result(existed).value == Some(stat))
    assert(statsReceiver.counter("watch_successes")() == 1)
  }

  "existsWatch" should "handle missing node" in {
    val existed = zk.existsWatch(path)

    verify(mockZK).exists(
      meq(path),
      watcher.capture,
      statCB.capture,
      meq(null))

    statCB.getValue.processResult(apacheNoNode, path, null, apacheStat)
    assert(Await.result(existed).value == None)
    assert(statsReceiver.counter("watch_successes")() == 1)
  }

  "existsWatch" should "handle ZK error" in {
    val existed = zk.existsWatch(path)

    verify(mockZK).exists(
      meq(path),
      watcher.capture,
      statCB.capture,
      meq(null))

    statCB.getValue.processResult(apacheConnLoss, path, null, null)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(existed)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  "existsWatch" should "handle synchronous error" in {
    when(mockZK.exists(
      meq(path),
      watcher.capture,
      statCB.capture,
      meq(null))
    ).thenThrow(new IllegalArgumentException)
    val existed = zk.existsWatch(path)

    verify(mockZK).exists(
      meq(path),
      watcher.capture,
      statCB.capture,
      meq(null))

    intercept[IllegalArgumentException] {
      Await.result(existed)
    }
    assert(statsReceiver.counter("watch_failures")() == 1)
  }

  "getData" should "submit properly constructed getData" in {
    val nodeData = zk.getData(path)

    verify(mockZK).getData(
      meq(path),
      meq(null),
      dataCB.capture,
      meq(null))

    dataCB.getValue.processResult(apacheOk, path, null, _data, apacheStat)
    assert(Await.result(nodeData) == Node.Data(Some(data), stat))
    assert(statsReceiver.counter("read_successes")() == 1)
  }

  "getData" should "handle empty znodes" in {
    val nodeData = zk.getData(path)

    verify(mockZK).getData(
      meq(path),
      meq(null),
      dataCB.capture,
      meq(null))

    dataCB.getValue.processResult(apacheOk, path, null, null, apacheStat)
    assert(Await.result(nodeData) == Node.Data(None, stat))
    assert(statsReceiver.counter("read_successes")() == 1)
  }

  "getData" should "handle ZK error" in {
    val nodeData = zk.getData(path)

    verify(mockZK).getData(
      meq(path),
      meq(null),
      dataCB.capture,
      meq(null))

    dataCB.getValue.processResult(apacheConnLoss, path, null, null, null)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(nodeData)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  "getData" should "handle synchronous error" in {
    when(mockZK.getData(
      meq(path),
      meq(null),
      dataCB.capture,
      meq(null))
    ).thenThrow(new IllegalArgumentException)
    val nodeData = zk.getData(path)

    verify(mockZK).getData(
      meq(path),
      meq(null),
      dataCB.capture,
      meq(null))

    intercept[IllegalArgumentException] {
      Await.result(nodeData)
    }
    assert(statsReceiver.counter("read_failures")() == 1)
  }

  "getDataWatch" should "submit properly constructed getData" in {
    val nodeDataWatch = zk.getDataWatch(path)

    verify(mockZK).getData(
      meq(path),
      watcher.capture,
      dataCB.capture,
      meq(null))

    dataCB.getValue.processResult(apacheOk, path, null, _data, apacheStat)
    assert(Await.result(nodeDataWatch).value == Node.Data(Some(data), stat))
    assert(statsReceiver.counter("watch_successes")() == 1)
  }

  "getDataWatch" should "handle empty znodes" in {
    val nodeDataWatch = zk.getDataWatch(path)

    verify(mockZK).getData(
      meq(path),
      watcher.capture,
      dataCB.capture,
      meq(null))

    dataCB.getValue.processResult(apacheOk, path, null, null, apacheStat)
    assert(Await.result(nodeDataWatch).value == Node.Data(None, stat))
    assert(statsReceiver.counter("watch_successes")() == 1)
  }

  "getDataWatch" should "handle ZK error" in {
    val nodeDataWatch = zk.getDataWatch(path)

    verify(mockZK).getData(
      meq(path),
      watcher.capture,
      dataCB.capture,
      meq(null))

    dataCB.getValue.processResult(apacheConnLoss, path, null, null, null)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(nodeDataWatch)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  "getDataWatch" should "handle synchronous error" in {
    when(mockZK.getData(
      meq(path),
      watcher.capture,
      dataCB.capture,
      meq(null))
    ).thenThrow(new IllegalArgumentException)
    val nodeDataWatch = zk.getDataWatch(path)

    verify(mockZK).getData(
      meq(path),
      watcher.capture,
      dataCB.capture,
      meq(null))

    intercept[IllegalArgumentException] {
      Await.result(nodeDataWatch)
    }
    assert(statsReceiver.counter("watch_failures")() == 1)
  }

  "setData" should "submit properly constructed versioned setData" in {
    val nodeStat = zk.setData(path, Some(data), Some(version))

    verify(mockZK).setData(
      meq(path),
      meq(_data),
      meq(version),
      statCB.capture,
      meq(null))

    statCB.getValue.processResult(apacheOk, path, null, apacheStat)
    assert(Await.result(nodeStat) == stat)
    assert(statsReceiver.counter("write_successes")() == 1)
  }

  "setData" should "submit properly constructed unversioned setData" in {
    val nodeStat = zk.setData(path, Some(data), None)

    verify(mockZK).setData(
      meq(path),
      meq(_data),
      meq(-1),
      statCB.capture,
      meq(null))

    statCB.getValue.processResult(apacheOk, path, null, apacheStat)
    assert(Await.result(nodeStat) == stat)
    assert(statsReceiver.counter("write_successes")() == 1)
  }

  "setData" should "submit properly constructed unversioned empty znode setData" in {
    val nodeStat = zk.setData(path, None, None)

    verify(mockZK).setData(
      meq(path),
      meq(null),
      meq(-1),
      statCB.capture,
      meq(null))

    statCB.getValue.processResult(apacheOk, path, null, apacheStat)
    assert(Await.result(nodeStat) == stat)
    assert(statsReceiver.counter("write_successes")() == 1)
  }

  "setData" should "handle ZK error" in {
    val nodeStat = zk.setData(path, Some(data), Some(version))

    verify(mockZK).setData(
      meq(path),
      meq(_data),
      meq(version),
      statCB.capture,
      meq(null))

    statCB.getValue.processResult(apacheConnLoss, path, null, null)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(nodeStat)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  "setData" should "handle synchronous error" in {
    when(mockZK.setData(
      meq(path),
      meq(_data),
      meq(version),
      statCB.capture,
      meq(null))
    ).thenThrow(new IllegalArgumentException)
    val nodeStat = zk.setData(path, Some(data), Some(version))

    verify(mockZK).setData(
      meq(path),
      meq(_data),
      meq(version),
      statCB.capture,
      meq(null))

    intercept[IllegalArgumentException] {
      Await.result(nodeStat)
    }
    assert(statsReceiver.counter("write_failures")() == 1)
  }

  "getACL" should "submit properly constructed getACL" in {
    val nodeACL = zk.getACL(path)

    verify(mockZK).getACL(
      meq(path),
      meq(null),
      aclCB.capture,
      meq(null))

    aclCB.getValue.processResult(apacheOk, path, null, apacheACLS, apacheStat)
    assert(Await.result(nodeACL) == Node.ACL(acls, stat))
    assert(statsReceiver.counter("read_successes")() == 1)
  }

  "getACL" should "handle ZK error" in {
    val nodeACL = zk.getACL(path)

    verify(mockZK).getACL(
      meq(path),
      meq(null),
      aclCB.capture,
      meq(null))

    aclCB.getValue.processResult(apacheConnLoss, path, null, null, null)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(nodeACL)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  "getACL" should "handle synchronous error" in {
    when(mockZK.getACL(
      meq(path),
      meq(null),
      aclCB.capture,
      meq(null))
    ).thenThrow(new IllegalArgumentException)
    val nodeACL = zk.getACL(path)

    verify(mockZK).getACL(
      meq(path),
      meq(null),
      aclCB.capture,
      meq(null))

    intercept[IllegalArgumentException] {
      Await.result(nodeACL)
    }
    assert(statsReceiver.counter("read_failures")() == 1)
  }

  "setACL" should "submit properly constructed versioned setACL" in {
    val nodeStat = zk.setACL(path, acls, Some(version))

    verify(mockZK).setACL(
      meq(path),
      meq(apacheACLS),
      meq(version),
      statCB.capture,
      meq(null))

    statCB.getValue.processResult(apacheOk, path, null, apacheStat)
    assert(Await.result(nodeStat) == stat)
    assert(statsReceiver.counter("write_successes")() == 1)
  }

  "setACL" should "submit properly constructed unversioned setACL" in {
    val nodeStat = zk.setACL(path, acls, None)

    verify(mockZK).setACL(
      meq(path),
      meq(apacheACLS),
      meq(-1),
      statCB.capture,
      meq(null))

    statCB.getValue.processResult(apacheOk, path, null, apacheStat)
    assert(Await.result(nodeStat) == stat)
    assert(statsReceiver.counter("write_successes")() == 1)
  }

  "setACL" should "handle ZK error" in {
    val nodeStat = zk.setACL(path, acls, Some(version))

    verify(mockZK).setACL(
      meq(path),
      meq(apacheACLS),
      meq(version),
      statCB.capture,
      meq(null))

    statCB.getValue.processResult(apacheConnLoss, path, null, apacheStat)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(nodeStat)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  "setACL" should "handle synchronous error" in {
    when(mockZK.setACL(
      meq(path),
      meq(apacheACLS),
      meq(version),
      statCB.capture,
      meq(null))
    ).thenThrow(new IllegalArgumentException)
    val nodeStat = zk.setACL(path, acls, Some(version))

    verify(mockZK).setACL(
      meq(path),
      meq(apacheACLS),
      meq(version),
      statCB.capture,
      meq(null))

    intercept[IllegalArgumentException] {
      Await.result(nodeStat)
    }
    assert(statsReceiver.counter("write_failures")() == 1)
  }

  "getChildren" should "submit properly constructed getChildren" in {
    val nodeChildren = zk.getChildren(path)

    verify(mockZK).getChildren(
      meq(path),
      meq(null),
      childrenCB.capture,
      meq(null))

    childrenCB.getValue.processResult(apacheOk, path, null, apacheChildren, apacheStat)
    assert(Await.result(nodeChildren) == children)
    assert(statsReceiver.counter("read_successes")() == 1)
  }

  "getChildren" should "handle ZK error" in {
    val nodeChildren = zk.getChildren(path)

    verify(mockZK).getChildren(
      meq(path),
      meq(null),
      childrenCB.capture,
      meq(null))

    childrenCB.getValue.processResult(apacheConnLoss, path, null, apacheChildren, apacheStat)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(nodeChildren)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  "getChildren" should "handle synchronous error" in {
    when(mockZK.getChildren(
      meq(path),
      meq(null),
      childrenCB.capture,
      meq(null))
    ).thenThrow(new IllegalArgumentException)
    val nodeChildren = zk.getChildren(path)

    verify(mockZK).getChildren(
      meq(path),
      meq(null),
      childrenCB.capture,
      meq(null))

    intercept[IllegalArgumentException] {
      Await.result(nodeChildren)
    }
    assert(statsReceiver.counter("read_failures")() == 1)
  }

  "getChildrenWatch" should "submit properly constructed getChildren" in {
    val nodeChildren = zk.getChildrenWatch(path)

    verify(mockZK).getChildren(
      meq(path),
      watcher.capture,
      childrenCB.capture,
      meq(null))

    childrenCB.getValue.processResult(apacheOk, path, null, apacheChildren, apacheStat)
    assert(Await.result(nodeChildren).value == children)
    assert(statsReceiver.counter("watch_successes")() == 1)
  }

  "getChildrenWatch" should "handle ZK error" in {
    val nodeChildren = zk.getChildrenWatch(path)

    verify(mockZK).getChildren(
      meq(path),
      watcher.capture,
      childrenCB.capture,
      meq(null))

    childrenCB.getValue.processResult(apacheConnLoss, path, null, apacheChildren, apacheStat)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(nodeChildren)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  "getChildrenWatch" should "handle synchronous error" in {
    when(mockZK.getChildren(
      meq(path),
      watcher.capture,
      childrenCB.capture,
      meq(null))
    ).thenThrow(new IllegalArgumentException)
    val nodeChildren = zk.getChildrenWatch(path)

    verify(mockZK).getChildren(
      meq(path),
      watcher.capture,
      childrenCB.capture,
      meq(null))

    intercept[IllegalArgumentException] {
      Await.result(nodeChildren)
    }
    assert(statsReceiver.counter("watch_failures")() == 1)
  }

  "sync" should "submit properly constructed sync" in {
    val synced = zk.sync(path)

    verify(mockZK).sync(
      meq(path),
      voidCB.capture,
      meq(null)
    )

    voidCB.getValue.processResult(apacheOk, path, null)
    assert(Await.result(synced) == ())
    assert(statsReceiver.counter("read_successes")() == 1)
  }

  "sync" should "handle ZK error" in {
    val synced = zk.sync(path)

    verify(mockZK).sync(
      meq(path),
      voidCB.capture,
      meq(null)
    )

    voidCB.getValue.processResult(apacheConnLoss, path, null)
    intercept[KeeperException.ConnectionLoss] {
      Await.result(synced)
    }
    assert(statsReceiver.counter("connection_loss")() == 1)
  }

  "sync" should "handle synchronous error" in {
    when(mockZK.sync(
      meq(path),
      voidCB.capture,
      meq(null)
    )).thenThrow(new IllegalArgumentException)
    val synced = zk.sync(path)

    verify(mockZK).sync(
      meq(path),
      voidCB.capture,
      meq(null)
    )

    intercept[IllegalArgumentException] {
      Await.result(synced)
    }
    assert(statsReceiver.counter("read_failures")() == 1)
  }
}
