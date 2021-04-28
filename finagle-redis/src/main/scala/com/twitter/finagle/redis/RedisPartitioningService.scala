package com.twitter.finagle.redis

import com.twitter.finagle.partitioning.ConsistentHashPartitioningService
import com.twitter.finagle.partitioning.ConsistentHashPartitioningService.NoPartitioningKeys
import com.twitter.finagle.partitioning.PartitioningService.PartitionedResults
import com.twitter.finagle.partitioning.param.NumReps
import com.twitter.finagle.redis.param.RedisKeyHasher
import com.twitter.finagle.redis.protocol.{Command, Reply, StatusReply}
import com.twitter.finagle.redis.util.{BufToString, ReplyFormat}
import com.twitter.finagle.util.DefaultLogger
import com.twitter.finagle.{ServiceFactory, Stack, Stackable}
import com.twitter.hashing
import com.twitter.io.Buf
import com.twitter.logging.Level
import com.twitter.util.Future
import scala.collection.{Set => SSet}

private[finagle] object RedisPartitioningService {
  private[finagle] class UnsupportedCommand(msg: String) extends Exception(msg)
  private[finagle] class UnsupportedBatchCommand(msg: String) extends Exception(msg)
  private[finagle] class UnsupportedReply(msg: String) extends Exception(msg)
  private[finagle] class FailedPartitionedCommand(msg: String = null, t: Throwable = null)
      extends Exception(msg, t)

  private[finagle] val role = Stack.Role("RedisPartitioning")
  private[finagle] val description =
    "Partitioning Service based on a consistent hash ring for the redis protocol"

  def module: Stackable[ServiceFactory[Command, Reply]] =
    new ConsistentHashPartitioningService.Module[Command, Reply, Buf] {
      override val role: Stack.Role = RedisPartitioningService.role
      override val description: String = RedisPartitioningService.description

      def newConsistentHashPartitioningService(
        underlying: Stack[ServiceFactory[Command, Reply]],
        params: Stack.Params
      ): ConsistentHashPartitioningService[Command, Reply, Buf] = {
        val RedisKeyHasher(hasher) = params[RedisKeyHasher]
        val NumReps(numReps) = params[NumReps]

        new RedisPartitioningService(
          underlying,
          params,
          hasher,
          numReps
        )
      }
    }

  private val StatusOK = StatusReply("OK")
}

private[finagle] class RedisPartitioningService(
  underlying: Stack[ServiceFactory[Command, Reply]],
  params: Stack.Params,
  keyHasher: hashing.KeyHasher = hashing.KeyHasher.MURMUR3,
  numReps: Int = NumReps.Default)
    extends ConsistentHashPartitioningService[Command, Reply, Buf](
      underlying,
      params,
      keyHasher,
      numReps
    ) {

  import RedisPartitioningService._
  import com.twitter.finagle.redis.protocol._

  private[this] val logger = DefaultLogger

  final override protected def getKeyBytes(key: Buf): Array[Byte] =
    Buf.ByteArray.Owned.extract(key)

  private[this] def unsupportedCommand(cmd: Command): Nothing = {
    val msg = s"Unsupported command: $cmd"
    if (logger.isLoggable(Level.DEBUG))
      logger.log(Level.DEBUG, msg)
    throw new UnsupportedCommand(msg)
  }

  private[this] def unsupportedReply(reply: Reply): Nothing = {
    val msg = s"UnsupportedReply: $reply"
    if (logger.isLoggable(Level.DEBUG))
      logger.log(Level.DEBUG, msg)
    throw new UnsupportedReply(msg)
  }

  protected def getPartitionKeys(command: Command): Seq[Buf] =
    command match {
      // the following commands assume talking to a single redis server,
      // this is incompatible with key based routing.
      case keys: Keys => unsupportedCommand(keys)
      case migrate: Migrate => unsupportedCommand(migrate)
      case select: Select => unsupportedCommand(select)
      case scan: Scan => unsupportedCommand(scan)
      case randomkey: Randomkey.type => unsupportedCommand(randomkey)

      // eval operations could possibly be supported, however the fact that the
      // reply must be cast to a particular type makes it non-obvoius how we should
      // handle the results in mergeResponse. Leaving it as a TODO.
      case eval: Eval => unsupportedCommand(eval)
      case evalSha: EvalSha => unsupportedCommand(evalSha)

      // mSetNx is unsupported because we cannot guarantee the atomicity of this operation across
      // multiple servers
      case mSetNx: MSetNx => unsupportedCommand(mSetNx)

      case _: Ping.type => Seq(Buf.Empty)

      case kc: KeyCommand => Seq(kc.key)

      case kcs: KeysCommand => kcs.keys
      case _ => unsupportedCommand(command)
    }

  protected def createPartitionRequestForKeys(command: Command, pKeys: Seq[Buf]): Command =
    command match {
      case c: PFCount => c.copy(keys = pKeys)
      case d: Del => d.copy(keys = pKeys)
      case s: SInter => s.copy(keys = pKeys)
      case m: MGet => m.copy(keys = pKeys)
      case m: MSet => m.copy(kv = m.kv.filterKeys(pKeys.toSet).toMap)
      case _ => unsupportedCommand(command)
    }

  // this is only called for partitioned commands, i.e. the commands referred to in
  // createPartitionRequestForKeys
  protected override def mergeResponses(
    originalReq: Command,
    pr: PartitionedResults[Command, Reply]
  ): Reply = {
    if (pr.failures.nonEmpty) {
      if (logger.isLoggable(Level.DEBUG)) {
        logger.log(Level.DEBUG, "failures in bulk reply")
        for {
          (cmd, t) <- pr.failures
        } {
          logger.log(Level.DEBUG, s"Command: $cmd", t)
        }
      }

      pr.failures.head match {
        case (_, t) =>
          throw new FailedPartitionedCommand("Partitioned command failed, first error is", t)
      }
    }

    // if we get here, there are no failures in results

    originalReq match {
      case _: Ping.type => NoReply

      case _: PFCount | _: Del =>
        IntegerReply(
          pr.successes.map {
            case (_, IntegerReply(n)) => n
            case (_, reply) => unsupportedReply(reply)
          }.sum
        )

      case MGet(keys) =>
        val resultsMap: Map[Buf, Reply] =
          pr.successes.flatMap {
            case (MGet(pkeys), MBulkReply(messages)) => pkeys.zip(messages)
            case (_, rep) => unsupportedReply(rep)
          }.toMap

        // we map the keys over the results because the order of results should match
        // the order of the supplied keys
        MBulkReply(keys.map(resultsMap).toList)

      case _: SInter => aggregateSetIntersection(pr.successes.map(_._2))

      // from the redis documentation:
      // > Simple string reply: always OK since MSET can't fail.
      case _: MSet => StatusOK

      case wat => unsupportedCommand(wat)
    }
  }

  // Since the set intersection command (SInter) may be split across multiple partitions
  // it's necessary to perform some post-processing in this step. This function takes the
  // results from the various partitions and reduces them using the Set.intersect method
  // and returns the result
  private[this] def aggregateSetIntersection(reps: Seq[Reply]): Reply = {
    val sets =
      reps.map {
        case MBulkReply(messages) => ReplyFormat.toBuf(messages).toSet
        case EmptyMBulkReply => SSet.empty[Buf]
        case rep => unsupportedReply(rep)
      }

    if (sets.isEmpty) {
      EmptyMBulkReply
    } else {
      val reduced = sets.reduce(_ intersect _)

      if (reduced.isEmpty) EmptyMBulkReply
      else MBulkReply(reduced.toList.map(BulkReply))
    }
  }

  final protected def noPartitionInformationHandler(req: Command): Future[Nothing] = {
    val ex = new NoPartitioningKeys(
      s"NoPartitioningKeys in for the thrift method: ${BufToString(req.name)}")
    if (logger.isLoggable(Level.DEBUG))
      logger.log(Level.DEBUG, "partitionRequest failed: ", ex)
    Future.exception(ex)
  }
}
