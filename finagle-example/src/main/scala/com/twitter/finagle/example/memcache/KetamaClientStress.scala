package com.twitter.finagle.example.memcache

import com.twitter.common.args.Flags
import com.twitter.conversions.time._
import com.twitter.finagle.builder.{Cluster, ClientBuilder}
import com.twitter.finagle.memcached
import com.twitter.finagle.memcached.CacheNode
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.memcached.replication._
import com.twitter.finagle.memcached.{CachePoolCluster, PartitionedClient}
import com.twitter.finagle.stats.OstrichStatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.ostrich.admin.{AdminHttpService, RuntimeEnvironment}
import com.twitter.util._
import java.util.concurrent.atomic.AtomicLong
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import scala.Some

object KetamaClientStress {
  private[this] case class Config(
    hosts:       String    = "localhost:11211",
    replicas:    String    = null,
    op:          String    = "set",
    keysize:     Int       = 55,
    valuesize:   Int       = 1,
    numkeys:     Int       = 1,
    rwRatio:     Int       = 99, // 99% read
    loadrate:    Int       = 0,
    concurrency: Int       = 1,
    stats:       Boolean   = true,
    tracing:     Boolean   = true,
    cap:         Int       = Int.MaxValue
  )
  private[this] val throughput_count = new AtomicLong
  private[this] val load_count = new AtomicLong
  private[this] val timer = DefaultTimer.twitter
  private[this] var loadTask: TimerTask = null

  def proc(op: () => Future[Any], qps: Int) {
    if (qps == 0)
      op() ensure {
        throughput_count.incrementAndGet()
        proc(op, 0)
      }
    else if (qps > 0)
      loadTask = timer.schedule(Time.now, 1.seconds) {
        1 to qps foreach { _ => op() ensure { throughput_count.incrementAndGet() }}
      }
    else
      1 to (qps * -1) foreach { _ => proc(op, 0) }
  }

  private[this] def randomString(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
    val sb = new StringBuilder
    for (i <- 1 to length) {
      val randomNum = util.Random.nextInt(chars.length)
      sb.append(chars(randomNum))
    }
    sb.toString
  }

  private[this] def createCluster(hosts: String): Cluster[CacheNode] = {
    CachePoolCluster.newStaticCluster(
      PartitionedClient.parseHostPortWeights(hosts) map {
        case (host, port, weight) => new CacheNode(host, port, weight)
      } toSet)
  }

  def main(args: Array[String]) {
    val config = Flags(Config(), args)

    // the client builder
    var builder = ClientBuilder()
        .name("ketamaclient")
        .codec(Memcached())
        .failFast(false)
        .hostConnectionCoresize(config.concurrency)
        .hostConnectionLimit(config.concurrency)

    if (config.stats)    builder = builder.reportTo(new OstrichStatsReceiver)
    if (config.tracing)  com.twitter.finagle.tracing.Trace.enable()
    else                 com.twitter.finagle.tracing.Trace.disable()

    println(builder)

    // the test keys/values
    val keyValueSet: Seq[(String, ChannelBuffer)] = 1 to config.numkeys map { _ =>
      (randomString(config.keysize), ChannelBuffers.wrappedBuffer(randomString(config.valuesize).getBytes)) }
    def nextKeyValue: (String, ChannelBuffer) = keyValueSet((load_count.getAndIncrement()%config.numkeys).toInt)

    // local admin service
    val runtime = RuntimeEnvironment(this, Array()/*no args for you*/)
    val adminService = new AdminHttpService(2000, 100/*backlog*/, runtime)
    adminService.start()

    val primaryPool = createCluster(config.hosts)
    val replicaPool =
      if (config.replicas == null || config.replicas.equals("")) null
      else createCluster(config.replicas)

    if (replicaPool == null) {
      val ketamaClient = memcached.KetamaClientBuilder()
          .clientBuilder(builder)
          .cachePoolCluster(primaryPool)
          .failureAccrualParams(Int.MaxValue, Duration.Top)
          .build()

      val operation = config.op match {
        case "set" =>
          () => {
            val (key, value) = nextKeyValue
            ketamaClient.set(key, value)
          }
        case "getHit" =>
          keyValueSet foreach { case (k, v) => ketamaClient.set(k, v)() }
          () => {
            val (key, _) = nextKeyValue
            ketamaClient.get(key)
          }
        case "getMiss" =>
          keyValueSet foreach { case (k, _) => ketamaClient.delete(k)() }
          () => {
            val (key, _) = nextKeyValue
            ketamaClient.get(key)
          }
        case "gets" =>
          keyValueSet foreach { case (k, v) => ketamaClient.set(k, v)() }
          () => {
            val (key, _) = nextKeyValue
            ketamaClient.gets(key)
          }
        case "getsMiss" =>
          keyValueSet foreach { case (k, _) => ketamaClient.delete(k)() }
          () => {
            val (key, _) = nextKeyValue
            ketamaClient.gets(key)
          }
        case "getsThenCas" =>
          keyValueSet map { case (k, v) => ketamaClient.set(k, v)() }
          val casMap: scala.collection.mutable.Map[String, (ChannelBuffer, ChannelBuffer)] = scala.collection.mutable.Map()

          () => {
            val (key, value) = nextKeyValue
            casMap.remove(key) match {
              case Some((_, unique)) => ketamaClient.cas(key, value, unique)
              case None => ketamaClient.gets(key) map {
                case Some(r) => casMap(key) = r
                case None => // not expecting
              }
            }
          }
        case "add" =>
          val (key, value) = (randomString(config.keysize), ChannelBuffers.wrappedBuffer(randomString(config.valuesize).getBytes))
          () => {
            ketamaClient.add(key+load_count.getAndIncrement().toString, value)
          }
        case "replace" =>
          keyValueSet foreach { case (k, v) => ketamaClient.set(k, v)() }
          () => {
            val (key, value) = nextKeyValue
            ketamaClient.replace(key, value)
          }
      }

      proc(operation, config.loadrate)
    } else {
      val replicationClient = ReplicationClient.newBaseReplicationClient(
        Seq(primaryPool, replicaPool),
        Some(builder),
        None, (Int.MaxValue, Duration.Top))

      val operation = config.op match {
        case "set" => () => {
          val (key, value) = nextKeyValue
          replicationClient.set(key, value)
        }
        case "getAllHit" =>
          keyValueSet foreach { case (k, v) => replicationClient.set(k, v)() }
          () => {
            val (key, _) = nextKeyValue
            replicationClient.getAll(key)
          }
        case "getAllMiss" =>
          keyValueSet foreach { case (k, _) => replicationClient.delete(k)() }
          () => {
            val (key, _) = nextKeyValue
            replicationClient.getAll(key)
          }
        case "getOneHit" =>
          keyValueSet foreach { case (k, v) => replicationClient.set(k, v)() }
          () => {
            val (key, _) = nextKeyValue
            replicationClient.getOne(key, false)
          }
        case "getOneMiss" =>
          keyValueSet foreach { case (k, _) => replicationClient.delete(k)() }
          () => {
            val (key, _) = nextKeyValue
            replicationClient.getOne(key, false)
          }
        case "getSetMix" =>
          assert(config.rwRatio >=0 && config.rwRatio < 100)
          keyValueSet foreach { case (k, v) => replicationClient.set(k, v)() }
          () => {
            val c = load_count.getAndIncrement()
            val (key, value) = keyValueSet((c%config.numkeys).toInt)
            if (c % 100 >= config.rwRatio)
              replicationClient.set(key, value)
            else
              replicationClient.getOne(key, false)
          }
        case "getsAll" =>
          keyValueSet foreach { case (k, v) => replicationClient.set(k, v)() }
          () => {
            val (key, _) = nextKeyValue
            replicationClient.getsAll(key)
          }
        case "getsAllMiss" =>
          keyValueSet foreach { case (k, _) => replicationClient.delete(k)() }
          () => {
            val (key, _) = nextKeyValue
            replicationClient.getsAll(key)
          }
        case "getsAllThenCas" =>
          keyValueSet map { case (k, v) => replicationClient.set(k, v)() }
          val casMap: scala.collection.mutable.Map[String, ReplicationStatus[Option[(ChannelBuffer, ReplicaCasUnique)]]] = scala.collection.mutable.Map()

          () => {
            val (key, value) = nextKeyValue
            casMap.remove(key) match {
              case Some(ConsistentReplication(Some((_, RCasUnique(uniques))))) =>
                replicationClient.cas(key, value, uniques)
              case Some(ConsistentReplication(None)) =>
                // not expecting this to ever happen
                replicationClient.set(key, value)
              case Some(InconsistentReplication(resultSeq)) =>
                // not expecting this to ever happen
                replicationClient.set(key, value)
              case Some(FailedReplication(failureSeq)) =>
                // not expecting this to ever happen
                replicationClient.set(key, value)
              case None => replicationClient.getsAll(key) map { casMap(key) = _ }
            }
          }
        case "add" =>
          val (key, value) = (randomString(config.keysize), ChannelBuffers.wrappedBuffer(randomString(config.valuesize).getBytes))
          () => {
            replicationClient.add(key+load_count.getAndIncrement().toString, value)
          }
        case "replace" =>
          keyValueSet foreach { case (k, v) => replicationClient.set(k, v)() }
          () => {
            val (key, value) = nextKeyValue
            replicationClient.replace(key, value)
          }
      }
      proc(operation, config.loadrate)
    }

    val elapsed = Stopwatch.start()
    while (true) {
      Thread.sleep(5000)
      val howlong = elapsed()
      val howmuch_load = load_count.get()
      val howmuch_throughput = throughput_count.get()
      assert(howmuch_throughput > 0)

      printf("load: %6d QPS, throughput: %6d QPS\n",
        howmuch_load / howlong.inSeconds, howmuch_throughput / howlong.inSeconds)

      // stop generating load
      if (howmuch_load >= config.cap && loadTask != null) {
        timer.stop()
        loadTask.cancel()
      }

      // quit the loop when all load is drained
      if (howmuch_load >= config.cap && (config.loadrate == 0 || howmuch_throughput >= howmuch_load)) {
        sys.exit()
      }
    }
  }
}
