package com.twitter.finagle.memcached

import com.google.common.hash.Hashing
import com.twitter.finagle.Service
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.util.{AtomicMap, ParserUtils}
import com.twitter.io.Buf
import com.twitter.util.{Future, Time}

/**
 * Evaluates a given Memcached operation and returns the result.
 */
class Interpreter(map: AtomicMap[Buf, Entry]) {

  import ParserUtils._
  import Interpreter._

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
      case Cas(key, flags, expiry, value, casUnique) =>
        map.lock(key) { data =>
          val existing = data.get(key)
          existing match {
            case Some(entry) if entry.valid =>
              val currentValue = entry.value
              if (casUnique.equals(generateCasUnique(currentValue))) {
                data(key) = Entry(value, expiry)
                Stored()
              } else {
                NotStored()
              }
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
          keys.flatMap { key =>
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
      case Gets(keys) =>
        getByKeys(keys)
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

  private def getByKeys(keys: Seq[Buf]): Values = {
    Values(
      keys.flatMap { key =>
        map.lock(key) { data =>
          data.get(key).filter { entry =>
            entry.valid
          }.map { entry =>
            val value = entry.value
            Value(key, value, Some(generateCasUnique(value)))
          }
        }
      }
    )
  }

}

private[memcached] object Interpreter {
  /*
  * Using non-cryptographic goodFastHash Hashing Algorithm
  * for we only care about speed for testing.
  *
  * The real memcached uses uint64_t for cas tokens,
  * so we convert the hash to a String
  * representation of an unsigned Long so it can be
  * used as a cas token.
  */
  private[memcached] def generateCasUnique(value: Buf): Buf = {
    val hashAsUnsignedLong = Hashing.goodFastHash(32)
      .newHasher(value.length)
      .putBytes(Buf.ByteArray.Owned.extract(value))
      .hash()
      .padToLong
      .abs
    Buf.Utf8(hashAsUnsignedLong.toString)
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
