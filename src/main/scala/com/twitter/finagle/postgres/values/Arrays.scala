package com.twitter.finagle.postgres.values

import scala.collection.immutable.Queue
import scala.util.parsing.combinator.RegexParsers

import com.twitter.util.{Return, Throw, Try}
import org.jboss.netty.buffer.ChannelBuffer
object Arrays {

  object ArrayStringParser extends RegexParsers {

    val value = """([^",}]|\")*""".r | """"([^"]|\")*"""".r
    val valueComma = "," ~ value ^^ { case "," ~ v => v }
    val values = (value ~ valueComma.*) ^^ { case first ~ rest => first :: rest } | value.? ^^ (_.toList)
    val array = "{" ~ values ~ "}" ^^ { case _ ~ vs ~ _ => vs }
    val maybeArrayValue = array | value ^^ (List(_))
    val maybeArrayValueComma = ("," ~ maybeArrayValue) ^^ { case _ ~ v => v}
    val maybeArrayValues =
      (maybeArrayValue ~ maybeArrayValueComma.*) ^^ { case first ~ rest => first ::: rest.flatten } |
        maybeArrayValue.* ^^ (_.flatten)
    val root = "{" ~ maybeArrayValues ~ "}" ^^ {
      case _ ~ vs ~ _ => vs
    }

    def apply(str: String) = parseAll(root, str) match {
      case Success(strings, _) => Return(strings)
      case Failure(_, _) | Error(_, _) => Throw(new Exception("Failed to parse array string"))
    }

  }

  def decodeArrayText[T](str: String, elemDecoder: ValueDecoder[T]) = {
    ArrayStringParser(str).flatMap {
      strings => strings.map(elemDecoder.decodeText).foldLeft[Try[Queue[T]]](Return(Queue.empty[T])) {
        (accum, next) => accum.flatMap {
          current => next.map {
            v => current enqueue v.value
          }
        }
      }
    }
  }

  def decodeArrayBinary[T](buf: ChannelBuffer, elemDecoder: ValueDecoder[T]) = {
    val ndims = buf.readInt()
    val flags = buf.readInt()
    val elemOid = buf.readInt()
  }

}
