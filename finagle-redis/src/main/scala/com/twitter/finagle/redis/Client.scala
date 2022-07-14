package com.twitter.finagle.redis

import com.twitter.finagle.Status
import com.twitter.finagle.redis.exp.RedisPool
import com.twitter.finagle.redis.exp.SubscribeCommands
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.ClientConnection
import com.twitter.finagle.Redis
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.ServiceProxy
import com.twitter.io.Buf
import com.twitter.util.Closable
import com.twitter.util.Future
import com.twitter.util.Time
import com.twitter.util.Timer

object Client {

  /**
   * Construct a client from a single host.
   * @param host a String of host:port combination.
   */
  def apply(host: String): Client =
    new Client(com.twitter.finagle.Redis.client.newClient(host))

  /**
   * Construct a client from a single Service.
   */
  def apply(raw: ServiceFactory[Command, Reply]): Client =
    new Client(raw)
}

class Client(
  override val factory: ServiceFactory[Command, Reply],
  private[redis] val timer: Timer = DefaultTimer)
    extends BaseClient(factory)
    with NormalCommands
    with SubscribeCommands
    with Transactions

trait NormalCommands
    extends KeyCommands
    with StringCommands
    with HashCommands
    with SortedSetCommands
    with ListCommands
    with SetCommands
    with BtreeSortedSetCommands
    with TopologyCommands
    with HyperLogLogCommands
    with GeoCommands
    with PubSubCommands
    with ServerCommands
    with ScriptCommands
    with ConnectionCommands
    with StreamCommands { self: BaseClient => }

trait Transactions { self: Client =>
  private[this] def singletonFactory(): ServiceFactory[Command, Reply] =
    new ServiceFactory[Command, Reply] {
      val svc: Future[Service[Command, Reply]] = RedisPool.forTransaction(factory)
      // Because the `singleton` is used in the context of a `FactoryToService` we override
      // `Service#close` to ensure that we can control the checkout lifetime of the `Service`.
      val proxiedService: Future[ServiceProxy[Command, Reply]] =
        svc.map { underlying =>
          new ServiceProxy(underlying) {
            override def close(deadline: Time) = Future.Done
          }
        }

      def apply(conn: ClientConnection) = proxiedService
      def close(deadline: Time): Future[Unit] = svc.map(_.close(deadline))
      def status: Status = factory.status
    }

  def transaction[T](cmds: Seq[Command]): Future[Seq[Reply]] =
    transactionSupport(_.transaction(cmds))

  def transaction[T](f: NormalCommands => Future[_]): Future[Seq[Reply]] =
    transactionSupport(_.transaction(f))

  def transactionSupport[T](f: TransactionalClient => Future[T]): Future[T] = {
    val singleton = singletonFactory()
    val client = new TransactionalClient(singleton)
    f(client).ensure {
      client.reset().ensure(singleton.close())
    }
  }
}

/**
 * Connects to a single Redis host
 * @param factory: Finagle service factory object built with the Redis codec
 */
abstract class BaseClient(protected val factory: ServiceFactory[Command, Reply]) extends Closable {

  /**
   * Releases underlying service factory object
   */
  def close(deadline: Time): Future[Unit] = factory.close(deadline)

  /**
   * Helper function for passing a command to the service
   */
  private[redis] def doRequest[T](
    cmd: Command
  )(
    handler: PartialFunction[Reply, Future[T]]
  ): Future[T] = {
    factory.toService
      .apply(cmd)
      .flatMap(handler orElse {
        case ErrorReply(message) => Future.exception(new ServerError(message))
        case StatusReply("QUEUED") => Future.Done.asInstanceOf[Future[Nothing]]
        case _ => Future.exception(new IllegalStateException)
      })
  }

  /**
   * Helper function to convert a Redis multi-bulk reply into a map of pairs
   */
  private[redis] def returnPairs[A](messages: Seq[A]) = {
    assert(messages.length % 2 == 0, "Odd number of items in response")
    messages.grouped(2).toSeq.flatMap {
      case Seq(a, b) => Some((a, b))
      case _ => None
    }
  }
}

/**
 * A redis client that uses a consistent hash ring to do key-based routing between multiple
 * backend nodes. Use [[com.twitter.finagle.redis.PartitionedClient.apply]] to create a new instance
 */
class PartitionedClient private[redis] (factory: ServiceFactory[Command, Reply])
    extends BaseClient(factory)
    with NormalCommands

object PartitionedClient {

  /**
   * Create a new partitioned redis client with the given `dest`. A partitioned client uses
   * a consistent hash ring to distribute keys to backend nodes in the cluster.
   *
   * @param dest the location to connect to, e.g. "host:port". See the
   *             [[https://twitter.github.io/finagle/guide/Names.html user guide]]
   *             for details on destination names.
   * @see https://en.wikipedia.org/wiki/Consistent_hashing
   */
  def apply(dest: String): PartitionedClient =
    apply(Redis.partitionedClient.newClient(dest))

  /**
   * Creates a new partitioned redis client using the given [[com.twitter.finagle.ServiceFactory]] instance.
   * This method must *only* be called with a ServiceFactory that's been generated by
   * [[Redis.partitionedClient]]
   */
  def apply(raw: ServiceFactory[Command, Reply]): PartitionedClient =
    new PartitionedClient(raw)
}

object TransactionalClient {

  /**
   * Construct a client from a single host with transaction commands
   * @param host a String of host:port combination.
   */
  def apply(host: String): TransactionalClient =
    new TransactionalClient(com.twitter.finagle.Redis.client.newClient(host))

  /**
   * Construct a client from a service factory
   * @param raw ServiceFactory
   */
  def apply(raw: ServiceFactory[Command, Reply]): TransactionalClient =
    new TransactionalClient(raw)
}

/**
 * Client connected over a single connection to a
 * single redis instance, supporting transactions
 */
class TransactionalClient(factory: ServiceFactory[Command, Reply])
    extends BaseClient(factory)
    with NormalCommands {

  private[this] var _multi = false
  private[this] var _watch = false

  /**
   * Flushes all previously watched keys for a transaction
   */
  def unwatch(): Future[Unit] =
    doRequest(UnWatch) {
      case StatusReply(message) =>
        _watch = false
        Future.Unit
    }

  /**
   * Marks given keys to be watched for conditional execution of a transaction
   * @param keys to watch
   */
  def watches(keys: Seq[Buf]): Future[Unit] =
    doRequest(Watch(keys)) {
      case StatusReply(message) =>
        _watch = true
        Future.Unit
    }

  def multi(): Future[Unit] =
    doRequest(Multi) {
      case StatusReply(message) =>
        _multi = true
        Future.Unit
    }

  def exec(): Future[Seq[Reply]] =
    doRequest(Exec) {
      case MBulkReply(messages) =>
        _watch = false
        _multi = false
        Future.value(messages)
      case EmptyMBulkReply =>
        _watch = false
        _multi = false
        Future.Nil
      case NilMBulkReply =>
        _watch = false
        _multi = false
        Future.exception(new ServerError("One or more keys were modified before transaction"))
    }

  def discard(): Future[Unit] =
    doRequest(Discard) {
      case StatusReply(message) =>
        _multi = false
        _watch = false
        Future.Unit
    }

  def transaction[T](cmds: Seq[Command]): Future[Seq[Reply]] = {
    transaction {
      cmds.iterator
        .map(cmd => factory().flatMap(_(cmd)).unit)
        .reduce(_ before _)
    }
  }

  def transaction[T](f: => Future[_]): Future[Seq[Reply]] = {
    multi() before {
      f.unit before exec()
    } ensure {
      reset()
    }
  }

  private[redis] def transaction[T](f: NormalCommands => Future[_]): Future[Seq[Reply]] = {
    transaction(f(this))
  }

  private[redis] def reset(): Future[Unit] = {
    if (_multi) discard()
    else if (_watch) unwatch()
    else Future.Done
  }
}
