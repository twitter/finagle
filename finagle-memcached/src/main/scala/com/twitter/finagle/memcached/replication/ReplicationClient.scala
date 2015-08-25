package com.twitter.finagle.memcached.replication

import _root_.java.lang.{Boolean => JBoolean, Long => JLong}

import scala.collection.JavaConversions._
import scala.util.Random

import com.twitter.conversions.time._
import com.twitter.finagle.builder.{Cluster, ClientBuilder, ClientConfig}
import com.twitter.finagle.cacheresolver.CacheNode
import com.twitter.finagle.Group
import com.twitter.finagle.memcached._
import com.twitter.finagle.memcached.protocol.Value
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.io.Buf
import com.twitter.util._

sealed trait ReplicationStatus[T]

/**
 * Indicating a consistent state across all replicas, which comes with the agreed consistent result;
 */
case class ConsistentReplication[T](result: T) extends ReplicationStatus[T]

/**
 * Indicating an inconsistent state across all replicas, which comes with a sequence of result
 * from all replicas; each replica's result can be either Return[T] or Throw[T]
 */
case class InconsistentReplication[T](resultSeq: Seq[Try[T]]) extends ReplicationStatus[T]

/**
 * indicating a failed state from all replicas, which comes with a sequence of failures from
 * all replicas;
 */
case class FailedReplication[T](failureSeq: Seq[Throw[T]]) extends ReplicationStatus[T]

/**
 * Wrapping underlying replicas cas unique values for replication purpose.
 */
trait ReplicaCasUnique
case class RCasUnique(uniques: Seq[Buf]) extends ReplicaCasUnique
case class SCasUnique(casUnique: Buf) extends ReplicaCasUnique

/**
 * Replication client helper
 */
object ReplicationClient {
  def newBaseReplicationClient(
    pools: Seq[Cluster[CacheNode]],
    clientBuilder: Option[ClientBuilder[_, _, _, _, ClientConfig.Yes]] = None,
    hashName: Option[String] = None,
    failureAccrualParams: (Int, () => Duration) = (5, () => 30.seconds)
  ) = {
    val underlyingClients = pools map { pool =>
      Await.result(pool.ready)
      KetamaClientBuilder(Group.fromCluster(pool), hashName, clientBuilder, failureAccrualParams).build()
    }
    val repStatsReceiver =
      clientBuilder map { _.statsReceiver.scope("cache_replication") } getOrElse(NullStatsReceiver)
    new BaseReplicationClient(underlyingClients, repStatsReceiver)
  }

  def newSimpleReplicationClient(
    pools: Seq[Cluster[CacheNode]],
    clientBuilder: Option[ClientBuilder[_, _, _, _, ClientConfig.Yes]] = None,
    hashName: Option[String] = None,
    failureAccrualParams: (Int, () => Duration) = (5, () => 30.seconds)
  ) = {
    new SimpleReplicationClient(newBaseReplicationClient(pools, clientBuilder, hashName, failureAccrualParams))
  }
}

/**
 * Base replication client. This client manages a list of base memcached clients representing
 * cache replicas. All replication API returns ReplicationStatus object indicating the underlying
 * replicas consistency state.
 * @param clients list of memcached clients with each one representing to a single cache pool
 * @param statsReceiver
 */
class BaseReplicationClient(clients: Seq[Client], statsReceiver: StatsReceiver = NullStatsReceiver) {
  private[this] val inconsistentContentCounter = statsReceiver.counter("inconsistent_content_count")
  private[this] val failedCounter = statsReceiver.counter("failed_replication_count")

  assert(!clients.isEmpty)

  /**
   * Return GetResult object that aggregates all hits, misses and failures.
   * This method will send the requested keys to each underlying replicas in a fixed order or
   * random order, and will stop passing along a key if a replica has returned 'hit'.
   *
   * TODO: introducing BackupRequestFilter to shorten the waiting period for secondary requests
   */
  private[memcached] def getResult(keys: Iterable[String], useRandomOrder: Boolean): Future[GetResult] = {
    val clientsInOrder = if (useRandomOrder) Random.shuffle(clients) else clients

    def loopGet(clients: Seq[Client], currentRes: GetResult): Future[GetResult] = clients match {
      case _ if currentRes.misses.isEmpty &&
          currentRes.failures.isEmpty => Future.value(currentRes)
      case Seq() => Future.value(currentRes)
      case Seq(c, tail@_*) =>
        val missing = currentRes.misses ++ currentRes.failures.keySet
        c.getResult(missing) flatMap { case res =>
          val newRes = GetResult.merged(Seq(GetResult(currentRes.hits), res))
          loopGet(tail, newRes)
        }
    }

    loopGet(clientsInOrder, GetResult(Map.empty, keys.toSet))
  }

  /**
   * Get one value for the input keys from the underlying replicas.
   * For each input key, this operation searches all replicas in an order until it finds the
   * first hit result, or return the last replica's result.
   */
  def getOne(key: String, useRandomOrder: Boolean = false): Future[Option[Buf]] =
    getOne(Seq(key), useRandomOrder) map { _.values.headOption }

  def getOne(keys: Iterable[String], useRandomOrder: Boolean): Future[Map[String, Buf]] =
    getResult(keys, useRandomOrder) flatMap { result =>
      if (result.failures.nonEmpty)
        Future.exception(result.failures.values.head)
      else
        Future.value(result.values)
    }

  /**
   * Get replication status for the input keys from the underlying replicas.
   * For each input key, this operation returns the aggregated replication status after requesting
   * all replicas.
   */
  def getAll(key: String): Future[ReplicationStatus[Option[Buf]]] =
    getAll(Seq(key)) map { _.values.head }

  def getAll(keys: Iterable[String]): Future[Map[String, ReplicationStatus[Option[Buf]]]] = {
    val keySet = keys.toSet
    Future.collect(clients map { _.getResult(keySet) }) map {
      results: Seq[GetResult] =>
        keySet.map { k =>
          val replicasResult = results map {
            case r if (r.hits.contains(k)) => Return(Some(r.hits.get(k).get.value))
            case r if (r.misses.contains(k)) => Return(None)
            case r if (r.failures.contains(k)) => Throw(r.failures.get(k).get)
          }
          k -> toReplicationStatus(replicasResult)
        }.toMap
    }
  }

  /**
   * Get replication status for the input keys and their checksum. The aggregated results returned
   * can be either of these three cases:
   * - ConsistentReplication, indicating consistent value across all replicas, which comes with
   * an aggregated cas unique to be used for CAS;
   *
   * - InconsistentReplication, indicating inconsistent values across all replicas, which comes
   * with each replica's own cas unique id;
   *
   * - FailedReplication, indicating failures from all replicas;
   */
  def getsAll(key: String): Future[ReplicationStatus[Option[(Buf, ReplicaCasUnique)]]] =
    getsAll(Seq(key)) map { _.values.head }

  def getsAll(keys: Iterable[String]): Future[Map[String, ReplicationStatus[Option[(Buf, ReplicaCasUnique)]]]] = {
    val keySet = keys.toSet
    Future.collect(clients map { _.getsResult(keySet) }) map {
      results: Seq[GetsResult] =>
        keySet.map { k =>
          val replicasResult = results map {
            case r if (r.hits.contains(k)) => Return(Some(r.hits.get(k).get.value))
            case r if (r.misses.contains(k)) => Return(None)
            case r if (r.failures.contains(k)) => Throw(r.failures.get(k).get)
          }

          k -> attachCas(toReplicationStatus(replicasResult), results, k)
        }.toMap
    }
  }

  // attach replication cas unique to the result for clients to do following CAS;
  // if all replicas are consistent, a RCasUnique is attached,
  // otherwise individual SCasUnique is attached
  private[this] def attachCas(
    valueStatus: ReplicationStatus[Option[Buf]],
    underlyingResults: Seq[GetsResult],
    key: String
  ): ReplicationStatus[Option[(Buf, ReplicaCasUnique)]] =
    valueStatus match {
      case ConsistentReplication(Some(v)) =>
        val allReplicasCas = underlyingResults map {_.hits.get(key).get.casUnique.get}
        ConsistentReplication(Some((v, RCasUnique(allReplicasCas))))
      case ConsistentReplication(None) => ConsistentReplication(None)
      case InconsistentReplication(rs) =>
        val transformed = rs.zip(underlyingResults) map {
          case (Return(Some(v)), r: GetsResult) =>
            val singleReplicaCas = r.hits.get(key).get.casUnique.get
            Return(Some((v, SCasUnique(singleReplicaCas))))
          case (Return(None), _) => Return(None)
          case (Throw(e), _) => Throw(e)
        }
        InconsistentReplication(transformed)
      case FailedReplication(fs) =>
        FailedReplication(fs map { t => Throw(t.e)})
    }

  /**
   * Stores a key in all replicas and returns the aggregated replication status.
   */
  def set(key: String, value: Buf): Future[ReplicationStatus[Unit]] =
    set(key, 0, Time.epoch, value)

  def set(key: String, flags: Int, expiry: Time, value: Buf): Future[ReplicationStatus[Unit]] =
    collectAndResolve[Unit](_.set(key, flags, expiry, value))

  /**
   * Attempts to perform a CAS operation on all replicas, and returns the aggregated replication status.
   */
  def cas(key: String, value: Buf, casUniques: Seq[Buf]): Future[ReplicationStatus[JBoolean]] =
    cas(key, 0, Time.epoch, value, casUniques)

  def cas(key: String, flags: Int, expiry: Time, value: Buf, casUniques: Seq[Buf]): Future[ReplicationStatus[JBoolean]] = {
    assert(clients.size == casUniques.size)

    // cannot use collectAndResolve helper here as this is the only case where there's no common op
    Future.collect((clients zip casUniques) map {
      case (c, u) =>
        c.cas(key, flags, expiry, value, u).transform(Future.value)
    }) map { toReplicationStatus }
  }

  /**
   * Remove a key and returns the aggregated replication status.
   */
  def delete(key: String): Future[ReplicationStatus[JBoolean]] =
    collectAndResolve[JBoolean](_.delete(key))

  /**
   * Store a key in all replicas but only if it doesn't already exist on the server, and returns
   * the aggregated replication status.
   */
  def add(key: String, value: Buf): Future[ReplicationStatus[JBoolean]] =
    add(key, 0, Time.epoch, value)

  def add(key: String, flags: Int, expiry: Time, value: Buf): Future[ReplicationStatus[JBoolean]] =
    collectAndResolve[JBoolean](_.add(key, flags, expiry, value))

  /**
   * Replace existing key in all replicas, and returns the aggregated replication status.
   */
  def replace(key: String, value: Buf): Future[ReplicationStatus[JBoolean]] =
    replace(key, 0, Time.epoch, value)

  def replace(key: String, flags: Int, expiry: Time, value: Buf): Future[ReplicationStatus[JBoolean]] =
    collectAndResolve[JBoolean](_.replace(key, flags, expiry, value))

  /**
   * Increment a key and returns the aggregated replication status.
   */
  def incr(key: String): Future[ReplicationStatus[Option[JLong]]] = incr(key, 1L)

  def incr(key: String, delta: Long): Future[ReplicationStatus[Option[JLong]]] =
    collectAndResolve[Option[JLong]](_.incr(key, delta))

  /**
   * Decrement a key and returns the aggregated replication status.
   */
  def decr(key: String): Future[ReplicationStatus[Option[JLong]]] = decr(key, 1L)

  def decr(key: String, delta: Long): Future[ReplicationStatus[Option[JLong]]] =
    collectAndResolve[Option[JLong]](_.decr(key, delta))

  /**
   * Unsupported operation yet
   */
  def append(key: String, value: Buf): Future[ReplicationStatus[JBoolean]] =
    append(key, 0, Time.epoch, value)
  def append(key: String, flags: Int, expiry: Time, value: Buf): Future[ReplicationStatus[JBoolean]] =
    throw new UnsupportedOperationException("append is not supported for cache replication client.")

  /**
   * Unsupported operation yet
   */
  def prepend(key: String, value: Buf): Future[ReplicationStatus[JBoolean]] =
    prepend(key, 0, Time.epoch, value)
  def prepend(key: String, flags: Int, expiry: Time, value: Buf): Future[ReplicationStatus[JBoolean]] =
    throw new UnsupportedOperationException("prepend is not supported for cache replication client.")

  /**
   * Unsupported operation yet
   */
  def stats(args: Option[String]): Future[Seq[String]] =
    throw new UnsupportedOperationException("stats is not supported for cache replication client.")

  def release() {
    clients foreach { _.release() }
  }

  /**
   * Translating the results sequence from all replicas into aggregated results, which can be
   * either of these three sub-types of ReplicationStatus:
   *
   * - ConsistentReplication, indicating a consistent state across all replicas, which comes with
   * the agreed consistent result;
   *
   * - InconsistentReplication, indicating an inconsistent state across all replicas, which comes
   * with a sequence of result from all replicas;
   *
   * - FailedReplication, indicating a failed state from all replicas, which comes with a sequence
   * of failures from all replicas;
   */
  private[this] def toReplicationStatus[T](results: Seq[Try[T]]): ReplicationStatus[T] = {
    results match {
      case _ if (results.forall(_.isReturn)) && (results.distinct.size == 1) =>
        ConsistentReplication(results.head.get())
      case _ if (results.exists(_.isReturn)) =>
        inconsistentContentCounter.incr()
        InconsistentReplication(results)
      case _ =>
        failedCounter.incr()
        FailedReplication(results collect {case t@Throw(_) => t})
    }
  }

  /**
   * Private helper to collect all underlying clients result for a given operation
   * and resolve them to the ReplicationStatus to tell the consistency
   */
  private[this] def collectAndResolve[T](op: Client => Future[T]) =
    Future.collect(clients map {
      op(_).transform(Future.value)
    }) map { toReplicationStatus }
}

/**
 * Simple replication client wrapper that's compatible with base memcached client.
 * This simple replication client handles inconsistent state across underlying replicas in a
 * naive way:
 * - operation would succeed only if it succeeds on all replicas
 * - inconsistent data across replicas will be treated as key missing
 * - any replica's failure will make the operation throw
 */
case class SimpleReplicationFailure(msg: String) extends Throwable(msg)

class SimpleReplicationClient(underlying: BaseReplicationClient) extends Client {
  def this(clients: Seq[Client], statsReceiver: StatsReceiver = NullStatsReceiver) =
    this(new BaseReplicationClient(clients, statsReceiver))

  private[this] val underlyingClient = underlying

  /**
   * Returns the first result found within all replicas, or miss/failure depends on the last replica
   */
  def getResult(keys: Iterable[String]): Future[GetResult] =
    underlyingClient.getResult(keys, useRandomOrder = false)

  /**
   * Only returns the consistent result from all replicas; if the data is inconsistent, this client simply
   * returns nothing just like key missing; if any failure occurs, this method returns failure
   * as there's a great chance the check-and-set won't succeed.
   */
  def getsResult(keys: Iterable[String]): Future[GetsResult] =
    underlyingClient.getsAll(keys) map {
      resultsMap =>
        val getsResultSeq = resultsMap map {
          case (key, ConsistentReplication(Some((value, RCasUnique(uniques))))) =>
            val newCas = uniques map { case Buf.Utf8(s) => s } mkString("|")
            val newValue = Value(Buf.Utf8(key), value, Some(Buf.Utf8(newCas)))
            GetsResult(GetResult(hits = Map(key -> newValue)))
          case (key, ConsistentReplication(None)) =>
            GetsResult(GetResult(misses = Set(key)))
          case (key, InconsistentReplication(resultsSeq)) if resultsSeq.forall(_.isReturn) =>
            GetsResult(GetResult(misses = Set(key)))
          case (key, _) =>
            GetsResult(GetResult(failures = Map(key -> SimpleReplicationFailure("One or more underlying replica failed gets"))))
        }
        GetResult.merged(getsResultSeq.toSeq)
    }

  /**
   * Store a key in all replicas, succeed only if all replicas succeed.
   */
  def set(key: String, flags: Int, expiry: Time, value: Buf) =
    resolve[Unit]("set", _.set(key, flags, expiry, value), ())

  /**
   * Check and set a key, succeed only if all replicas succeed.
   */
  def cas(key: String, flags: Int, expiry: Time, value: Buf, casUnique: Buf): Future[JBoolean] =
  {
    val Buf.Utf8(casUniqueStr) = casUnique
    val casUniqueBufs = casUniqueStr.split('|') map { Buf.Utf8(_) }
    resolve[JBoolean]("cas", _.cas(key, flags, expiry, value, casUniqueBufs), false)
  }

  /**
   * Delete a key from all replicas, succeed only if all replicas succeed.
   */
  def delete(key: String) =
    resolve[JBoolean]("delete", _.delete(key), false)

  /**
   * Add a new key to all replicas, succeed only if all replicas succeed.
   */
  def add(key: String, flags: Int, expiry: Time, value: Buf) =
    resolve[JBoolean]("add", _.add(key, flags, expiry, value), false)

  /**
   * Replace an existing key in all replicas, succeed only if all replicas succeed.
   */
  def replace(key: String, flags: Int, expiry: Time, value: Buf) =
    resolve[JBoolean]("replace", _.replace(key, flags, expiry, value), false)

  /**
   * Increase an existing key in all replicas, succeed only if all replicas succeed.
   */
  def incr(key: String, delta: Long): Future[Option[JLong]] =
    resolve[Option[JLong]]("incr", _.incr(key, delta), None)

  /**
   * Decrease an existing key in all replicas, succeed only if all replicas succeed.
   */
  def decr(key: String, delta: Long): Future[Option[JLong]] =
    resolve[Option[JLong]]("decr", _.decr(key, delta), None)

  /**
   * Private helper to resolve a replication operation result from BaseReplicationClient to
   * a single value type, with using given default value in case of data inconsistency.
   */
  private[this] def resolve[T](name: String, op: BaseReplicationClient => Future[ReplicationStatus[T]], default: T): Future[T] =
    op(underlyingClient) flatMap {
      case ConsistentReplication(r) => Future.value(r)
      case InconsistentReplication(resultsSeq) if resultsSeq.forall(_.isReturn) => Future.value(default)
      case _ => Future.exception(SimpleReplicationFailure("One or more underlying replica failed op: " + name))
    }

  def append(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] =
    throw new UnsupportedOperationException("append is not supported for replication cache client yet.")

  def prepend(key: String, flags: Int, expiry: Time, value: Buf): Future[JBoolean] =
    throw new UnsupportedOperationException("prepend is not supported for replication cache client yet.")

  def stats(args: Option[String]): Future[Seq[String]] =
    throw new UnsupportedOperationException("No logical way to perform stats without a key")

  def release() {
    underlyingClient.release()
  }
}
