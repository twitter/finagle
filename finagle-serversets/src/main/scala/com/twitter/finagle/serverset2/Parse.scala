package com.twitter.finagle.serverset2

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.ImmutableMap

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

private[serverset2] object IntObj {
  def unapply(o: Object): Option[Int] = o match {
    case i: java.lang.Integer => Some(i)
    case _ => None
  }
}

private[serverset2] object DoubleObj {
  def unapply(o: Object): Option[Double] = o match {
    case d: java.lang.Double => Some(d)
    case _ => None
  }
}

private[serverset2] object StringObj {
  def unapply(o: Object): Option[String] = o match {
    case s: String => Some(s)
    case _ => None
  }
}

private[serverset2] object SeqObj {
  def unapply(o: Object): Option[Seq[Object]] = o match {
    case l: java.util.List[_] => Some(l.asScala.toSeq.asInstanceOf[Seq[Object]])
    case _ => None
  }
}

private[serverset2] object DictObj {
  def unapply(o: Object): Option[Object => Option[Object]] = o match {
    case m: java.util.Map[_, _] =>
      val mm = m.asInstanceOf[java.util.Map[Object, Object]]
      Some(key => Option(mm.get(key)))
    case _ => None
  }
}

private[serverset2] object JsonDict {
  private[this] val m = new ObjectMapper

  def apply(json: String): (Object => Option[Object]) = {
    val o = try m.readValue(json, classOf[java.util.Map[Object, Object]])
    catch {
      case NonFatal(_) => return Function.const(None)
    }

    // If the object is a Curator service discovery record then translate it to a serversets entry.
    // TODO: Is this enough to definitively say the object is a Curator service discovery record?
    if (o.containsKey("name") && o.containsKey("id") && o.containsKey("address") && o.containsKey("port")) {
      key => {
        val host = o.get("address")
        val port = o.get("port")
        key match {
          case "serviceEndpoint" => Some(ImmutableMap.of("host", host, "port", port))
          case "status" => Some("ALIVE")
          case _ => None
        }
      }
    } else {
      key => Option(o.get(key))
    }
  }
}
