package com.twitter.finagle.postgres.values

import java.nio.charset.Charset

import scala.util.parsing.combinator.RegexParsers

import io.netty.buffer.{ByteBuf, Unpooled}

object HStores {
  object HStoreStringParser extends RegexParsers {
    def key:Parser[String] = "\"" ~ """([^"\\]*(\\.[^"\\]*)*)""".r ~ "\"" ^^ {
      case o~value~c => value.replace("\\\"", "\"").replace("\\\\", "\\")
    }

    def value = key | "NULL"

    def item:Parser[(String, Option[String])] = key ~ "=>" ~ value ^^ {
      case key~arrow~"NULL" => (key, None)
      case key~arrow~value => (key, Some(value))
    }

    def items:Parser[Map[String, Option[String]]] = repsep(item, ", ") ^^ { l => l.toMap }

    def apply(input:String):Option[Map[String, Option[String]]] = parseAll(items, input) match {
      case Success(result, _) => Some(result)
      case failure:NoSuccess => None
    }
  }

  def parseHStoreString(str: String) = HStoreStringParser(str)

  def formatHStoreString(hstore: Map[String, Option[String]]) = hstore.map {
    case (k, v) =>
      val key = s""""${k.replace("\"", "\\\"")}""""
      val value = v.map(str => s""""${str.replace("\"", "\\\"")}"""").getOrElse("NULL")
      s"""$key => $value"""
  }.mkString(",")

  def decodeHStoreBinary(buf: ByteBuf, charset: Charset) = {
    val count = buf.readInt()
    val pairs = Array.fill(count) {
      val keyLength = buf.readInt()
      val key = Array.fill(keyLength)(buf.readByte())
      val valueLength = buf.readInt()
      val value = valueLength match {
        case -1 => None
        case l =>
          val valueBytes = Array.fill(l)(buf.readByte())
          Some(valueBytes)
      }
      new String(key, charset) -> value.map(new String(_, charset))
    }
    pairs.toMap
  }

  def encodeHStoreBinary(hstore: Map[String, Option[String]], charset: Charset) = {
    val buf = Unpooled.buffer()
    buf.writeInt(hstore.size)
    hstore foreach {
      case (key, value) =>
        val keyBytes = key.getBytes(charset)
        buf.writeInt(keyBytes.length)
        buf.writeBytes(keyBytes)
        value match {
          case None => buf.writeInt(-1)
          case Some(v) =>
            val valueBytes = v.getBytes(charset)
            buf.writeInt(valueBytes.length)
            buf.writeBytes(valueBytes)
        }
    }
    buf
  }

}
