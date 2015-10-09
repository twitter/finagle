package com.twitter.finagle.memcached.migration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.twitter.common.zookeeper.ZooKeeperClient
import com.twitter.finagle.Memcached
import com.twitter.finagle.cacheresolver.ZookeeperStateMonitor
import com.twitter.finagle.memcached._
import com.twitter.finagle.stats.{ClientStatsReceiver, NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.zookeeper.DefaultZkClientFactory
import com.twitter.io.Buf
import com.twitter.util.{Future, Time}

/**
 * migration config data
 */
private[memcached] object MigrationConstants {
  case class MigrationConfig(
      state: String,
      readRepairBack: Boolean,
      readRepairFront: Boolean)

  val jsonMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  object MigrationState extends Enumeration {
    type t = Value
    val Pending, Warming, Verifying, Done = Value
  }

  object PoolNames extends Enumeration {
    type t = Value
    val OldPool, NewPool = Value
  }
}

/**
 * Migration client. This client manages a two cache clients representing source and
 * destination cache pool. Depending on the migration state, this client may send dark traffic
 * to destination pool to warm up the cache, or send light traffic to destination pool and fall
 * back to original pool for cache misses. The state transitioning is controlled by opeartor
 * by setting corresponding metadata in zookeeper.
 */
class MigrationClient(
    oldClient: Client,
    newClient: Client,
    protected val zkPath: String,
    protected val zkClient: ZooKeeperClient,
    protected val statsReceiver: StatsReceiver = NullStatsReceiver
) extends ProxyClient with ZookeeperStateMonitor {

  import MigrationConstants._

  // private type def of FrontendClient to be actually a proxy client
  class FrontendClient(client: Client) extends ProxyClient {
    override def proxyClient = client
  }

  @volatile var proxyClient = new FrontendClient(oldClient)

  override def applyZKData(data: Array[Byte]): Unit = synchronized {
    val config = jsonMapper.readValue(data, classOf[MigrationConfig])
    val migrationState = MigrationState.withName(config.state)

    migrationState match {
      case MigrationState.Pending =>
        proxyClient = new FrontendClient(oldClient)
      case MigrationState.Warming if (config.readRepairBack) =>
        proxyClient = new FrontendClient(oldClient) with DarkRead with ReadWarmup with DarkWrite {
          val backendClient = newClient
        }
      case MigrationState.Warming =>
        proxyClient = new FrontendClient(oldClient) with DarkRead with DarkWrite {
          val backendClient = newClient
        }
      case MigrationState.Verifying if (config.readRepairFront) =>
        proxyClient = new FrontendClient(newClient) with FallbackRead with ReadRepair {
          val backendClient = oldClient
        }
      case MigrationState.Verifying =>
        proxyClient = new FrontendClient(newClient) with FallbackRead {
          val backendClient = oldClient
        }
      case MigrationState.Done =>
        proxyClient = new FrontendClient(newClient)
    }
  }
}

/**
 * DarkRead client.
 * requires backendClient;
 * override Get and Gets to send dark read to backend pool;
 * backend read results are not blocking or exposed.
 */
trait DarkRead extends Client {
  protected val backendClient: Client

  abstract override def getResult(keys: Iterable[String]) = {
    val frontResult = super.getResult(keys)
    val backResult = backendClient.getResult(keys)

    chooseGetResult(frontResult, backResult)
  }

  // DarkRead always choose the front result and ignore the backend one
  protected def chooseGetResult(frontResult: Future[GetResult], backResult: Future[GetResult]): Future[GetResult] = {
    frontResult
  }

  abstract override def getsResult(keys: Iterable[String]) = {
    val frontResult = super.getsResult(keys)
    val backResult = backendClient.getsResult(keys)

    chooseGetsResult(frontResult, backResult)
  }

  // DarkRead always choose the front result and ignore the backend one
  protected def chooseGetsResult(frontResult: Future[GetsResult], backResult: Future[GetsResult]): Future[GetsResult] = {
    frontResult
  }

  abstract override def release() {
    super.release()
    backendClient.release()
  }
}

/**
 * ReadWarmup client.
 * can only be mixed into DarkRead client;
 * before returning frontend result, use frontend result to warm up backend missed key;
 * backend warming up is not not blocking or exposed.
 */
trait ReadWarmup { self: DarkRead =>
  override protected def chooseGetResult(frontResult: Future[GetResult], backResult: Future[GetResult]): Future[GetResult] = {
    // when readRepairDark, hit on front should repair miss on back in the background
    Future.join(frontResult, backResult) onSuccess {
      case (frontR, backR) => backR.misses foreach {
        case key if frontR.hits.contains(key) => backendClient.set(key, frontR.hits.get(key).get.value)
        case _ =>
      }
    }

    frontResult
  }

  override protected def chooseGetsResult(frontResult: Future[GetsResult], backResult: Future[GetsResult]): Future[GetsResult] = {
    Future.join(frontResult, backResult) onSuccess {
      case (frontR, backR) => backR.misses foreach {
        case key if frontR.hits.contains(key) => backendClient.set(key, frontR.hits.get(key).get.value)
        case _ =>
      }
    }

    frontResult
  }
}

/**
 * DarkWrite client.
 * requires backendClient;
 * override all write operation (except cas) to send dark write to backend pool;
 * backend write results are not blocking or exposed.
 */
trait DarkWrite extends Client {
  protected val backendClient: Client

  abstract override def set(key: String, flags: Int, expiry: Time, value: Buf) = {
    val result = super.set(key, flags, expiry, value)
    backendClient.set(key, flags, expiry, value)
    result
  }

  abstract override def add(key: String, flags: Int, expiry: Time, value: Buf) = {
    val result = super.add(key, flags, expiry, value)
    backendClient.add(key, flags, expiry, value)
    result
  }

  abstract override def append(key: String, flags: Int, expiry: Time, value: Buf) = {
    val result = super.append(key, flags, expiry, value)
    backendClient.append(key, flags, expiry, value)
    result
  }

  abstract override def prepend(key: String, flags: Int, expiry: Time, value: Buf) = {
    val result = super.prepend(key, flags, expiry, value)
    backendClient.prepend(key, flags, expiry, value)
    result
  }

  abstract override def replace(key: String, flags: Int, expiry: Time, value: Buf) = {
    val result = super.replace(key, flags, expiry, value)
    backendClient.replace(key, flags, expiry, value)
    result
  }

  abstract override def incr(key: String, delta: Long) = {
    val result = super.incr(key, delta)
    backendClient.incr(key, delta)
    result
  }

  abstract override def decr(key: String, delta: Long) = {
    val result = super.decr(key, delta)
    backendClient.decr(key, delta)
    result
  }

  // cas operation does not migrate
  abstract override def cas(key: String, flags: Int, expiry: Time, value: Buf, casUnique: Buf) =
    super.cas(key, flags, expiry, value, casUnique)

  abstract override def delete(key: String) = {
    val result = super.delete(key)
    backendClient.delete(key)
    result
  }

  abstract override def release() {
    super.release()
    backendClient.release()
  }
}

/**
 * FallbackRead client.
 * requires backendClient;
 * override Get to read the backend pool if for frontend pool misses;
 * Gets remains the same, as fallback reading backend pool for cas_unique is useless.
 */
trait FallbackRead extends Client {
  protected val backendClient: Client

  abstract override def getResult(keys: Iterable[String]) = {
    val frontResult = super.getResult(keys)

    frontResult flatMap {
      case frontR if (frontR.misses.nonEmpty || frontR.failures.nonEmpty) =>
        backendClient.getResult(frontR.misses ++ frontR.failures.keySet) map { backR =>
          combineGetResult(frontR, backR)
        }
      case frontR => Future.value(frontR)
    }
  }

  protected def combineGetResult(frontR: GetResult, backR: GetResult): GetResult = {
    // when fallback, merge the front hits with back result
    GetResult.merged(Seq(GetResult(frontR.hits), backR))
  }

  // Gets remains the same
  abstract override def getsResult(keys: Iterable[String]) = {
    super.getsResult(keys)
  }

  abstract override def release() {
    super.release()
    backendClient.release()
  }
}

/**
 * ReadRepair client.
 * can only be mixed into FallbackRead client;
 * before combining frontend and backend result, use backend result to repair frontend missed key;
 * frontend repairing is not not blocking or exposed.
 */
trait ReadRepair { self: FallbackRead =>
  override def combineGetResult(frontR: GetResult, backR: GetResult): GetResult = {
    // when readrepair, use back hit to repair front miss
    backR.hits foreach {
      case (k, v) => set(k, v.value)
    }
    GetResult.merged(Seq(GetResult(frontR.hits), backR))
  }
}

object MigrationClient {
  def newMigrationClient(zkHosts: String, zkPath: String) = {
    val zkClient = DefaultZkClientFactory.get(DefaultZkClientFactory.hostSet(zkHosts))._1

    val oldPoolPath = zkPath+"/" + MigrationConstants.PoolNames.OldPool.toString
    val newPoolPath = zkPath+"/" + MigrationConstants.PoolNames.NewPool.toString

    // verify the format of the path (zkPath, zkClient)
    assert(zkClient.get().exists(zkPath, false) != null)
    assert(zkClient.get().exists(oldPoolPath, false) != null)
    assert(zkClient.get().exists(newPoolPath, false) != null)

    // create client for old and new pool
    val oldClient = Memcached.client
      .configured(Memcached.param.EjectFailedHost(false))
      .newRichClient("twcache!"+zkHosts+"!"+oldPoolPath)
    val newClient = Memcached.client
      .configured(Memcached.param.EjectFailedHost(false))
      .newRichClient("twcache!"+zkHosts+"!"+newPoolPath)

    val migrationStatsReceiver = ClientStatsReceiver.scope("migrationclient")

    // create MigrationClient, by oldClient newClient, (zkPath, zkClient)
    new MigrationClient(oldClient, newClient, zkPath, zkClient, migrationStatsReceiver)
  }
}
