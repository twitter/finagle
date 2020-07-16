package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf

abstract class XInfo(sub: Buf, args: Seq[Buf] = Seq()) extends Command {
  RequireClientProtocol(args.forall(_.length > 0), "Found empty arg")

  override def name: Buf = Command.XINFO

  override def body: Seq[Buf] = sub +: args
}

case class XInfoConsumers(key: Buf, groupname: Buf)
    extends XInfo(Buf.Utf8("CONSUMERS"), Seq(key, groupname))

case class XInfoGroups(key: Buf) extends XInfo(Buf.Utf8("GROUPS"), Seq(key))

case class XInfoStream(key: Buf) extends XInfo(Buf.Utf8("STREAM"), Seq(key))

case object XInfoHelp extends XInfo(Buf.Utf8("HELP"))

object XAdd {
  final val AutogenId = Buf.Utf8("*")
}

case class XAdd(key: Buf, id: Option[Buf], fv: Map[Buf, Buf]) extends StrictKeyCommand {
  override def name: Buf = Command.XADD

  override def body: Seq[Buf] = {
    val fvList: List[Buf] = fv.iterator.flatMap {
      case (f, v) => f :: v :: Nil
    }.toList

    key :: id.getOrElse(XAdd.AutogenId) :: fvList
  }

  override protected def validate(): Unit = {
    super.validate()
    RequireClientProtocol(id.isEmpty || id.exists(!_.isEmpty), "ID cannot be empty")
    RequireClientProtocol(fv.nonEmpty, "Must pass field/value pairs")
    RequireClientProtocol(
      fv.forall { case (f, v) => !f.isEmpty && !v.isEmpty },
      "All field value pairs must be non-empty"
    )
  }
}

case class XTrim(key: Buf, size: Long, exact: Boolean) extends StrictKeyCommand {
  override def name: Buf = Command.XTRIM

  override def body: Seq[Buf] = {
    val exactPart = if (exact) None else Some(Buf.Utf8("~"))
    val args =
      (Some(Buf.Utf8("MAXLEN")) :: exactPart :: Some(Buf.Utf8(size.toString)) :: Nil).flatten
    Seq(key) ++ args
  }

  override protected def validate(): Unit = {
    super.validate()
    RequireClientProtocol(size >= 0, "Size must be >= 0")
  }
}

case class XDel(key: Buf, ids: Seq[Buf]) extends StrictKeyCommand {
  RequireClientProtocol(ids.nonEmpty, "ids must not be empty")

  override def name: Buf = Command.XDEL

  override def body: Seq[Buf] = Seq(key) ++ ids
}

abstract class XRangeCommand(
  key: Buf,
  start: Buf,
  end: Buf,
  count: Option[Long],
  reversed: Boolean = false)
    extends StrictKeyCommand {
  override def name: Buf = if (reversed) Command.XREVRANGE else Command.XRANGE

  override def body: Seq[Buf] = {
    val countSeq = count.map(c => Seq(Buf.Utf8("COUNT"), Buf.Utf8(c.toString))).getOrElse(Seq.empty)
    Seq(key, start, end) ++ countSeq
  }

  override protected def validate(): Unit = {
    super.validate()
    RequireClientProtocol(start.length > 0, "Start cannot be empty")
    RequireClientProtocol(end.length > 0, "End cannot be empty")
  }
}

case class XRange(key: Buf, start: Buf, end: Buf, count: Option[Long])
    extends XRangeCommand(key, start, end, count)

case class XRevRange(key: Buf, start: Buf, end: Buf, count: Option[Long])
    extends XRangeCommand(key, start, end, count, reversed = true)

case class XLen(key: Buf) extends StrictKeyCommand {
  override def name: Buf = Command.XLEN
}

case class XRead(count: Option[Long], blockMs: Option[Long], keys: Seq[Buf], ids: Seq[Buf])
    extends Command {
  RequireClientProtocol(keys.nonEmpty, "Empty stream keys")
  RequireClientProtocol(ids.nonEmpty, "Empty stream Ids")
  RequireClientProtocol(keys.size == ids.size, "Must have same number of stream keys and IDs")

  override def name: Buf = Command.XREAD

  override def body: Seq[Buf] = {
    val countSeq = count.map(c => Seq("COUNT", c.toString).map(Buf.Utf8.apply)).getOrElse(Seq.empty)
    val blockSeq =
      blockMs.map(b => Seq("BLOCK", b.toString).map(Buf.Utf8.apply)).getOrElse(Seq.empty)
    countSeq ++ blockSeq ++ Seq(Buf.Utf8("STREAMS")) ++ keys ++ ids
  }
}

case class XReadGroup(
  group: Buf,
  consumer: Buf,
  count: Option[Long],
  blockMs: Option[Long],
  keys: Seq[Buf],
  ids: Seq[Buf])
    extends Command {
  RequireClientProtocol(keys.nonEmpty, "Empty stream keys")
  RequireClientProtocol(ids.nonEmpty, "Empty stream Ids")
  RequireClientProtocol(keys.size == ids.size, "Must have same number of stream keys and IDs")

  override def name: Buf = Command.XREADGROUP

  override def body: Seq[Buf] = {
    val countSeq = count.map(c => Seq("COUNT", c.toString).map(Buf.Utf8.apply)).getOrElse(Seq.empty)
    val blockSeq =
      blockMs.map(b => Seq("BLOCK", b.toString).map(Buf.Utf8.apply)).getOrElse(Seq.empty)
    Seq(Buf.Utf8("GROUP"), group, consumer) ++ countSeq ++ blockSeq ++ Seq(
      Buf.Utf8("STREAMS")) ++ keys ++ ids
  }
}

abstract class XGroupCommand(sub: Buf, args: Seq[Buf] = Seq()) extends StrictKeyCommand {
  RequireClientProtocol(args.forall(_.length > 0), "Empty key found")

  override def name: Buf = Command.XGROUP

  override def body: Seq[Buf] = sub +: args
}

case class XGroupCreate(key: Buf, groupName: Buf, id: Buf)
    extends XGroupCommand(Buf.Utf8("CREATE"), Seq(key, groupName, id))

case class XGroupSetId(key: Buf, id: Buf) extends XGroupCommand(Buf.Utf8("SETID"), Seq(key, id))

case class XGroupDestroy(key: Buf, groupName: Buf)
    extends XGroupCommand(Buf.Utf8("DESTROY"), Seq(key, groupName))

case class XGroupDelConsumer(key: Buf, groupName: Buf, consumerName: Buf)
    extends XGroupCommand(Buf.Utf8("DELCONSUMER"), Seq(key, groupName, consumerName))

case class XAck(key: Buf, group: Buf, ids: Seq[Buf]) extends StrictKeyCommand {
  override def name: Buf = Command.XACK

  override def body: Seq[Buf] = Seq(key, group) ++ ids

  override protected def validate(): Unit = {
    super.validate()
    RequireClientProtocol(group.length > 0, "Group cannot be empty")
    RequireClientProtocol(ids.nonEmpty, "Ids cannot be empty")
    RequireClientProtocol(ids.forall(_.length > 0), "Found an empty ID")
  }
}

case class XPending(key: Buf, group: Buf) extends StrictKeyCommand {
  override def name: Buf = Command.XPENDING

  override def body: Seq[Buf] = Seq(key, group)

  override protected def validate(): Unit = {
    super.validate()
    RequireClientProtocol(group.length > 0, "Group cannot be empty")
  }
}

case class XPendingRange(
  key: Buf,
  group: Buf,
  start: Buf,
  end: Buf,
  count: Long,
  consumer: Option[Buf])
    extends StrictKeyCommand {
  override def name: Buf = Command.XPENDING

  override def body: Seq[Buf] =
    Seq(key, group, start, end, Buf.Utf8(count.toString)) ++ consumer.toSeq

  override protected def validate(): Unit = {
    super.validate()
    RequireClientProtocol(group.length > 0, "Group cannot be empty")
    RequireClientProtocol(start.length > 0, "Start cannot be empty")
    RequireClientProtocol(end.length > 0, "End cannot be empty")
    if (consumer.isDefined)
      RequireClientProtocol(consumer.get.length > 0, "Consumer cannot be empty")
  }
}

case class XClaim(
  key: Buf,
  group: Buf,
  consumer: Buf,
  minIdleTime: Long,
  ids: Seq[Buf],
  idle: Option[XClaimMillisOrUnixTs],
  retryCount: Option[Long],
  force: Boolean,
  justId: Boolean)
    extends StrictKeyCommand {
  override def name: Buf = Command.XCLAIM

  override def body: Seq[Buf] = {
    val forcePart = if (force) Seq("FORCE") else Seq.empty
    val justIdPart = if (justId) Seq("JUSTID") else Seq.empty
    val idlePart = idle match {
      case Some(XClaimMillis(n)) => Seq("IDLE", n.toString)
      case Some(XClaimUnixTs(n)) => Seq("TIME", n.toString)
      case None => Seq.empty
    }
    val retryCountPart = retryCount.map(r => Seq("RETRYCOUNT", r.toString)).getOrElse(Seq.empty)

    val options = (idlePart ++ retryCountPart ++ forcePart ++ justIdPart).map(Buf.Utf8.apply)

    Seq(key, group, consumer, Buf.Utf8(minIdleTime.toString)) ++ ids ++ options
  }

  override protected def validate(): Unit = {
    super.validate()
    RequireClientProtocol(group.length > 0, "Group cannot be empty")
    RequireClientProtocol(consumer.length > 0, "Consumer cannot be empty")
    RequireClientProtocol(ids.nonEmpty, "IDs cannot be empty")
    RequireClientProtocol(ids.forall(_.length > 0), "Found empty ID")
  }
}

sealed trait XClaimMillisOrUnixTs
case class XClaimMillis(ms: Long) extends XClaimMillisOrUnixTs
case class XClaimUnixTs(ts: Long) extends XClaimMillisOrUnixTs
