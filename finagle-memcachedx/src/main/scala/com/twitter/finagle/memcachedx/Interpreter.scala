package com.twitter.finagle.memcachedx

import com.twitter.finagle.memcachedx.protocol._
import com.twitter.finagle.Service
import com.twitter.finagle.memcachedx.util.{ParserUtils, AtomicMap}
import com.twitter.io.{Buf, Charsets}
import com.twitter.util.{Future, Time}

/**
 * Evalutes a given Memcached operation and returns the result.
 */
class Interpreter(map: AtomicMap[Buf, Entry]) {
  import ParserUtils._

  def apply(command: Command): Response = {
    command match {
      case Set(key, flags, expiry, value) =>
        map.lock(key) { data =>
          data(key) = Entry(value, expiry)
          Stored()
        }
      case Add(key, flags, expiry, value) =>
        map.lock(key) { data =>
          val existing = data.get(key)
          existing match {
            case Some(entry) if entry.valid =>
              NotStored()
            case _ =>
              data(key) = Entry(value, expiry)
              Stored()
          }
        }
      case Replace(key, flags, expiry, value) =>
        map.lock(key) { data =>
          val existing = data.get(key)
          existing match {
            case Some(entry) if entry.valid =>
              data(key) = Entry(value, expiry)
              Stored()
            case Some(_) =>
              data.remove(key) // expired
              NotStored()
            case _ =>
              NotStored()
          }
        }
      case Append(key, flags, expiry, value: Buf) =>
        map.lock(key) { data =>
          val existing = data.get(key)
          existing match {
            case Some(entry) if entry.valid =>
              data(key) = Entry(entry.value.concat(value), expiry)
              Stored()
            case Some(_) =>
              data.remove(key) // expired
              NotStored()
            case _ =>
              NotStored()
          }
        }
      case Prepend(key, flags, expiry, value) =>
        map.lock(key) { data =>
          val existing = data.get(key)
          existing match {
            case Some(entry) if entry.valid =>
              data(key) = Entry(value.concat(entry.value), expiry)
              Stored()
            case Some(_) =>
              data.remove(key) // expired
              NotStored()
            case _ =>
              NotStored()
          }
        }
      case Get(keys) =>
        Values(
          keys flatMap { key =>
            map.lock(key) { data =>
              data.get(key) filter { entry =>
                if (!entry.valid)
                  data.remove(key) // expired
                entry.valid
              } map { entry =>
                Value(key, entry.value)
              }
            }
          }
        )
      case Delete(key) =>
        map.lock(key) { data =>
          if (data.remove(key).isDefined)
            Deleted()
          else
            NotFound()
        }
      case Incr(key, delta) =>
        map.lock(key) { data =>
          val existing = data.get(key)
          existing match {
            case Some(entry) if entry.valid =>
              val Buf.Utf8(existingString) = entry.value
              if (!existingString.isEmpty && !DigitsPattern.matcher(existingString).matches())
                throw new ClientError("cannot increment or decrement non-numeric value")

              val existingValue: Long =
                if (existingString.isEmpty) 0L
                else existingString.toLong

              val result: Long = existingValue + delta
              data(key) = Entry(Buf.Utf8(result.toString), entry.expiry)

              Number(result)
            case Some(_) =>
              data.remove(key) // expired
              NotFound()
            case _ =>
              NotFound()
          }
        }
      case Decr(key, value) =>
        map.lock(key) { data =>
          apply(Incr(key, -value))
        }
      case Quit() =>
        NoOp()
    }
  }
}

case class Entry(value: Buf, expiry: Time) {
  /**
   * Whether or not the cache entry has expired
   */
  def valid: Boolean = expiry == Time.epoch || Time.now < expiry
}

class InterpreterService(interpreter: Interpreter) extends Service[Command, Response] {
  def apply(command: Command) = Future(interpreter(command))
}
