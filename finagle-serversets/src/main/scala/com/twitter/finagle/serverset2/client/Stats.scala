package com.twitter.finagle.serverset2.client

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.io.Buf
import com.twitter.util.{Stopwatch, Duration, Future, Time}

private[serverset2] trait StatsClient extends ZooKeeperClient {
  sealed trait StatFilter {
    val name: String
    lazy val failure = stats.counter("%s_failures".format(name))
    lazy val latency = stats.stat("%s_latency_ms".format(name))
    lazy val success = stats.counter("%s_successes".format(name))

    def apply[T](result: Future[T]): Future[T] = {
      val elapsed = Stopwatch.start()
      result respond { rv =>
        if (rv.isReturn) success.incr()
        else failure.incr()
        latency.add(elapsed().inMilliseconds)
      }
      result
    }
  }

  protected val EphemeralFilter = new StatFilter {
    val name = "ephemeral"
  }

  protected val MultiFilter = new StatFilter {
    val name = "multi"
  }

  protected val ReadFilter = new StatFilter {
    val name = "read"
  }

  protected val WatchFilter = new StatFilter {
    val name = "watch"
  }

  protected val WriteFilter = new StatFilter {
    val name = "write"
  }

  protected val underlying: ZooKeeperClient
  protected val stats: StatsReceiver

  def addAuthInfo(scheme: String, auth: Buf): Future[Unit] = underlying.addAuthInfo(scheme, auth)
  def close(deadline: Time): Future[Unit] = underlying.close()
  def sessionId: Long = underlying.sessionId
  def sessionPasswd: Buf = underlying.sessionPasswd
  def sessionTimeout: Duration = underlying.sessionTimeout
}

private[serverset2] trait StatsReader extends StatsClient with ZooKeeperReader {
  protected val underlying: ZooKeeperReader

  def exists(path: String): Future[Option[Data.Stat]] = ReadFilter(underlying.exists(path))
  def existsWatch(path: String): Future[Watched[Option[Data.Stat]]] = WatchFilter(underlying.existsWatch(path))
  def getData(path: String): Future[Node.Data] = ReadFilter(underlying.getData(path))
  def getDataWatch(path: String): Future[Watched[Node.Data]] = WatchFilter(underlying.getDataWatch(path))
  def getACL(path: String): Future[Node.ACL] = ReadFilter(underlying.getACL(path))
  def getChildren(path: String): Future[Node.Children] = ReadFilter(underlying.getChildren(path))
  def getChildrenWatch(path: String): Future[Watched[Node.Children]] = WatchFilter(underlying.getChildrenWatch(path))
  def globPrefixWatch(pat: String): Future[Watched[Seq[String]]] = WatchFilter(underlying.globPrefixWatch(pat))

  def sync(path: String): Future[Unit] = ReadFilter(underlying.sync(path))
}

private[serverset2] trait StatsWriter extends StatsClient with ZooKeeperWriter {
  protected val underlying: ZooKeeperWriter

  def create(
      path: String, data: Option[Buf], acl: Seq[Data.ACL], createMode: CreateMode): Future[String] = createMode match {
    case CreateMode.Ephemeral => EphemeralFilter(underlying.create(path, data, acl, createMode))
    case CreateMode.EphemeralSequential => EphemeralFilter(underlying.create(path, data, acl, createMode))
    case _ => WriteFilter(underlying.create(path, data, acl, createMode))
  }

  def delete(path: String, version: Option[Int]): Future[Unit] = WriteFilter(underlying.delete(path, version))

  def setACL(path: String, acl: Seq[Data.ACL], version: Option[Int]): Future[Data.Stat] =
    WriteFilter(underlying.setACL(path, acl, version))

  def setData(path: String, data: Option[Buf], version: Option[Int]): Future[Data.Stat] =
    WriteFilter(underlying.setData(path, data, version))
}

private[serverset2] trait StatsMulti extends StatsClient with ZooKeeperMulti {
  protected val underlying: ZooKeeperMulti

  def multi(ops: Seq[Op]): Future[Seq[OpResult]] = MultiFilter(underlying.multi(ops))
}

private[serverset2] trait StatsRW extends ZooKeeperRW with StatsReader with StatsWriter {
  protected val underlying: ZooKeeperRW
}

private[serverset2] trait StatsRWMulti extends ZooKeeperRWMulti with StatsReader with StatsWriter with StatsMulti {
  protected val underlying: ZooKeeperRWMulti
}

private[serverset2] trait SessionStats {
  protected val stats: StatsReceiver

  protected def SessionStateFilter(state: SessionState): SessionState = {
    state match {
      case SessionState.AuthFailed => stats.counter("session_auth_failures").incr()
      case SessionState.SyncConnected => stats.counter("session_connects").incr()
      case SessionState.ConnectedReadOnly => stats.counter("session_connects_readonly").incr()
      case SessionState.Disconnected => stats.counter("session_disconnects").incr()
      case SessionState.Expired => stats.counter("session_expirations").incr()
      case _ => Unit
    }
    state
  }
}
