package com.twitter.finagle.serverset2

import com.twitter.app.App
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.logging.Logger
import com.twitter.util.{Await, Future}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryOneTime
import org.apache.curator.test.TestingServer
import org.apache.zookeeper.CreateMode

/**
 * Companion to the [[ServerSetResolver]] test which deterministically adds and removes
 * members from a serverset on a local instance of ZooKeeper. Run this separately
 * to isolate benchmarking the client. If you don't kill it, it will automatically
 * exit after 1 hr. See the flags for toggle-able settings.
 */
private[serverset2] object LocalServerSetService extends App {

  private val initialMembers =
    flag("members.init", 500, "Number of members to start with in the serverset")
  private val additionsPerCycle =
    flag("members.add", 100, "Number of members to add each churn cycle")
  private val removalsPerCycle =
    flag("members.remove", 25, "Number of members to remove each churn cycle")
  private val maxMembers = flag("members.max", 1000, "Max members to keep in the serverset")
  private val churnFrequency = flag(
    "churn.frequency",
    200.milliseconds,
    "How often to add/remove members to a single serverset"
  )
  private val numberOfServersets = flag("serversets.count", 25, "Number of serversets to churn")
  private val zkListenPort =
    flag("zk.listenport", 2181, "port that the localhost zookeeper will listen on")

  private val timer = DefaultTimer
  private val logger = Logger(getClass)
  private var zkClient: CuratorFramework = null
  private val serversets = createServerSetPaths(numberOfServersets())
  private val membersets = new Array[Seq[String]](numberOfServersets())
  @volatile private var nextServerSetToChurn = 0
  @volatile private var nextMemberId = 0

  def createServerSetPaths(num: Int): Seq[String] =
    (1 to num).map { id => s"/twitter/service/testset_$id/staging/job" }

  def main(): Unit = {
    logger.info(s"Starting zookeeper on localhost:${zkListenPort()}")

    // Use Curator's built in testing server to start zookeeper on the
    // listening port specified by our flags
    val zkServer = new TestingServer(zkListenPort())

    zkClient = CuratorFrameworkFactory
      .builder()
      .connectString(zkServer.getConnectString)
      .retryPolicy(new RetryOneTime(1000))
      .build()

    logger.info(s"Connecting on localhost")
    zkClient.start()

    // initialize each serverset to `initialMembers` members
    (0 until numberOfServersets()).foreach { id =>
      membersets(id) = Seq.empty[String]
      addMembers(id, initialMembers())
    }

    scheduleUpdate()

    postmain {
      logger.info("Shutting down")

      timer.stop()
      zkClient.close()
      zkServer.close()
    }

    // User must manually kill this app
    Await.result(Future.never)
  }

  private def scheduleUpdate(): Unit =
    timer.doLater(churnFrequency()) { update() }

  private def update(): Unit = {
    // choose the single serverset to update, advance
    // the next id for future updates.
    val setToUpdate = nextServerSetToChurn
    nextServerSetToChurn += 1
    nextServerSetToChurn %= serversets.size

    // add + remove + truncate to max. This is intentionally "flappy" behavior
    addMembers(setToUpdate, additionsPerCycle())
    removeMembers(setToUpdate, removalsPerCycle())

    if (membersets(setToUpdate).size > maxMembers())
      removeMembers(setToUpdate, membersets(setToUpdate).size - maxMembers())

    scheduleUpdate()

    logger.info(s"ServerSet ${setToUpdate + 1} now has ${membersets(setToUpdate).size} members.")
  }

  private def addMembers(serversetIndex: Int, toAdd: Int): Unit = {
    (1 to toAdd).foreach { _ =>
      membersets(serversetIndex) :+= zkClient
        .create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
        .forPath(serversets(serversetIndex) + "/member_", nextJsonMember())
    }
  }

  private def removeMembers(index: Int, toRemove: Int): Unit = {
    (1 to toRemove).foreach { _ =>
      val removeMe = membersets(index).head
      membersets(index) = membersets(index).tail
      zkClient.delete().forPath(removeMe)
    }
  }

  private def nextJsonMember(): Array[Byte] = {
    val id = nextMemberId
    nextMemberId += 1

    // Generate a valid, unique ip address (since the resolvers
    // will de-dupe hosts based on ip)
    val someIp = s"${(id % 254) + 1}.${(id >> 1) % 255}.${(id >> 2) % 255}.${(id >> 3) % 255}"
    s"""|{
       |  "status": "ALIVE",
       |  "additionalEndpoints": {
       |    "health": {"host": "$someIp", "port": 31753},
       |    "aurora": {"host": "$someIp", "port": 31753},
       |    "http":   {"host": "$someIp", "port": 31324}
       |  },
       |  "serviceEndpoint": {"host": "$someIp", "port": 31324},
       |  "shard": 6
       |}""".stripMargin.getBytes("UTF-8")
  }

}
