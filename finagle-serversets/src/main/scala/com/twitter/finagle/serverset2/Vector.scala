package com.twitter.finagle.serverset2

import com.twitter.finagle.util.InetSocketAddressUtil.parseHostPorts
import com.twitter.util.NonFatal

private[serverset2] sealed trait Selector {
  def matches(e: Entry): Boolean
}

private[serverset2] object Selector {
  case class Host(host: String, port: Int) extends Selector {
    def matches(e: Entry) = e match {
      case Endpoint(_, host2, port2, _, _, _) => host2 == host && port2 == port
      case _ => false
    }
  }

  case class Member(which: String) extends Selector {
    def matches(e: Entry) = e match {
      case Endpoint(_, _, _, _, _, id) => which == id
      case _ => false
    }
  }

  case class Shard(which: Int) extends Selector {
    def matches(e: Entry) = e match {
      case Endpoint(_, _, _, id, _, _) => which == id
      case _ => false
    }
  }

  def parse(select: String) = select.split("=", 2) match {
    case Array("inet", arg) =>
      try {
        val (host, port) = parseHostPorts(arg).head
        Some(Host(host, port))
      } catch {
        case NonFatal(_) => None
      }
    case Array("member", which) => Some(Member(which))
    case Array("shard", which) =>
      try Some(Shard(which.toInt)) catch {
        case NonFatal(_) => None
      }
    case _ => None
  }
}

private[serverset2] case class Descriptor(
  selector: Selector,
  weight: Double,
  priority: Int
) {
  def matches(e: Entry) = selector matches e
}

private[serverset2] object Descriptor {
  def parseDict(d: Object => Option[Object]): Option[Descriptor] = for {
    StringObj(s) <- d("select")
    selector <- Selector.parse(s)
  } yield {
    val w = for { DoubleObj(w) <- d("weight") } yield w
    val p = for { IntObj(p) <- d("priority") } yield p
    Descriptor(selector, w getOrElse 1.0, p getOrElse 1)
  }
}

private[serverset2] case class Vector(vector: Seq[Descriptor]) {
  def weightOf(entry: Entry) =
    vector.foldLeft(1.0) {
      case (w, d) if d matches entry => w*d.weight
      case (w, _) => w
    }
}

private[serverset2] object Vector {
  def parseJson(json: String): Option[Vector] = {
    val d = JsonDict(json)
    val vec = for {
      SeqObj(vec) <- d("vector").toSeq
      DictObj(d) <- vec
      desc <- Descriptor.parseDict(d)
    } yield desc

    Some(Vector(vec))
  }
}
