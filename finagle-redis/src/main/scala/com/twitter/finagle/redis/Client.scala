package com.twitter.finagle.redis

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.Future
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

object Client {

  /**
   * Construct a client from a single host.
   * @param host a String of host:port combination.
   */
  def apply(host: String): Client = Client(
    ClientBuilder()
      .hosts(host)
      .hostConnectionLimit(1)
      .codec(Redis())
      .daemon(true)
      .build())

  /**
   * Construct a client from a single Service.
   */
  def apply(raw: Service[Command, Reply]): Client =
    new Client(raw)

}

class Client(service: Service[Command, Reply])
  extends BaseClient(service)
  with Keys
  with Strings
  with Hashes
  with SortedSets
  with Lists
  with Sets
  with BtreeSortedSetCommands
  with HyperLogLogs

/**
 * Connects to a single Redis host
 * @param service: Finagle service object built with the Redis codec
 */
class BaseClient(service: Service[Command, Reply]) {

  /**
   * Authorizes to db
   * @param password
   */
  def auth(password: ChannelBuffer): Future[Unit] =
    doRequest(Auth(password)) {
      case StatusReply(message) => Future.Unit
    }

  /**
   * Returns information and statistics about the server
   * @param section Optional parameter can be used to select a specific section of information
   * @return ChannelBuffer with collection of \r\n terminated lines if server has info on section
   */
  def info(section: ChannelBuffer = ChannelBuffers.EMPTY_BUFFER): Future[Option[ChannelBuffer]] =
    doRequest(Info(section)) {
      case BulkReply(message) => Future.value(Some(message))
      case EmptyBulkReply() => Future.value(None)
    }

  /**
   * Deletes all keys in all databases
   */
  def flushAll(): Future[Unit] =
    doRequest(FlushAll) {
      case StatusReply(_) => Future.Unit
    }

  /**
   * Deletes all keys in current DB
   */
  def flushDB(): Future[Unit] =
    doRequest(FlushDB) {
      case StatusReply(message) => Future.Unit
    }

  /**
   * Closes connection to Redis instance
   */
  def quit(): Future[Unit] =
    doRequest(Quit) {
      case StatusReply(message) => Future.Unit
    }

  /**
   * Select DB with specified zero-based index
   * @param index
   */
  def select(index: Int): Future[Unit] =
    doRequest(Select(index)) {
      case StatusReply(message) => Future.Unit
    }

  /**
   * Releases underlying service object
   */
  def release() = service.close()

  /**
   * Helper function for passing a command to the service
   */
  private[redis] def doRequest[T](cmd: Command)(handler: PartialFunction[Reply, Future[T]]) =
    service(cmd) flatMap (handler orElse {
      case ErrorReply(message)  => Future.exception(new ServerError(message))
      case _                    => Future.exception(new IllegalStateException)
    })

  /**
   * Helper function to convert a Redis multi-bulk reply into a map of pairs
   */
  private[redis] def returnPairs(messages: Seq[ChannelBuffer]) = {
    assert(messages.length % 2 == 0, "Odd number of items in response")
    messages.grouped(2).toSeq flatMap { case Seq(a, b) => Some(a, b); case _ => None }
  }

}


/**
 * Client connected over a single connection to a
 * single redis instance, supporting transactions
 */
trait TransactionalClient extends Client {

  /**
   * Flushes all previously watched keys for a transaction
   */
  def unwatch(): Future[Unit] =
    doRequest(UnWatch) {
      case StatusReply(message)  => Future.Unit
    }

  /**
   * Marks given keys to be watched for conditional execution of a transaction
   * @param keys to watch
   */
  def watch(keys: Seq[ChannelBuffer]): Future[Unit] =
    doRequest(Watch(keys)) {
      case StatusReply(message)  => Future.Unit
    }

  /**
   * Executes given vector of commands as a Redis transaction
   * The order of the commands queued on the Redis server is not guaranteed
   * @param Sequence of commands to be executed
   * @return Results of each command in order
   */
  def transaction(cmds: Seq[Command]): Future[Seq[Reply]]

}

object TransactionalClient {

  /**
   * Construct a client from a single host with transaction commands
   * @param host a String of host:port combination.
   */
  def apply(host: String): TransactionalClient = TransactionalClient(
    ClientBuilder()
      .hosts(host)
      .hostConnectionLimit(1)
      .codec(Redis())
      .daemon(true)
      .buildFactory())

  /**
   * Construct a client from a service factory
   * @param raw ServiceFactory
   */
  def apply(raw: ServiceFactory[Command, Reply]): TransactionalClient =
    new ConnectedTransactionalClient(raw)

}

/**
 * Connects to a single Redis host supporting transactions
 */
private[redis] class ConnectedTransactionalClient(
  serviceFactory: ServiceFactory[Command, Reply]
) extends Client(serviceFactory.toService) with TransactionalClient {

  def transaction(cmds: Seq[Command]): Future[Seq[Reply]] = {
    serviceFactory() flatMap { svc =>
      multi(svc) before {
        val cmdQueue = cmds map { cmd => svc(cmd) }
        Future.collect(cmdQueue).unit before exec(svc)
      } rescue { case e =>
        svc(Discard).unit before {
          Future.exception(ClientError("Transaction failed: " + e.toString))
        }
      } ensure {
        svc.close()
      }
    }
  }

  private def multi(svc: Service[Command, Reply]): Future[Unit] =
    svc(Multi) flatMap {
      case StatusReply(message)  => Future.Unit
      case ErrorReply(message)   => Future.exception(new ServerError(message))
      case _                     => Future.exception(new IllegalStateException)
  }

  private def exec(svc: Service[Command, Reply]): Future[Seq[Reply]] =
    svc(Exec) flatMap {
      case MBulkReply(messages)  => Future.value(messages)
      case EmptyMBulkReply()     => Future.Nil
      case NilMBulkReply()       => Future.exception(
        new ServerError("One or more keys were modified before transaction"))
      case ErrorReply(message)   => Future.exception(new ServerError(message))
      case _                     => Future.exception(new IllegalStateException)
    }

}
