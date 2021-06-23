package com.twitter.finagle.http

import com.twitter.app.Flaggable

private[finagle] sealed trait Protocol

private[finagle] object Protocol {
  case object HTTP_1_1 extends Protocol {
    override def toString: String = "HTTP/1.1"
  }
  case object HTTP_2 extends Protocol {
    override def toString: String = "HTTP/2"
  }

  val Default: Protocol = HTTP_2

  val Supported: Seq[Protocol] = Seq(HTTP_1_1, HTTP_2)

  implicit val flaggable: Flaggable[Protocol] = (str: String) =>
    str.toUpperCase match {
      case "HTTP/1.1" => HTTP_1_1
      case "HTTP/2" => HTTP_2
      case _ =>
        throw new IllegalArgumentException(
          s"'$str' is not a supported Finagle HTTP Protocol. " +
            s"Must be one of ${Supported.mkString("{'", "', '", "'}")}.")
    }
}
