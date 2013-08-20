package com.twitter.finagle.memcached

import com.twitter.finagle.memcached.protocol._
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer
import org.jboss.netty.util.CharsetUtil
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.util.{Time, Future}
import com.twitter.finagle.Service
import com.twitter.finagle.memcached.util.{ParserUtils, AtomicMap}

/**
 * Evalutes a given Memcached operation and returns the result.
 */
class Interpreter(map: AtomicMap[ChannelBuffer, Entry]) {
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
      case Append(key, flags, expiry, value) =>
        map.lock(key) { data =>
          val existing = data.get(key)
          existing match {
            case Some(entry) if entry.valid =>
              data(key) = Entry(wrappedBuffer(entry.value, value), expiry)
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
              data(key) = Entry(wrappedBuffer(value, entry.value), expiry)
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
                Value(key, wrappedBuffer(entry.value))
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
              val existingString = entry.value.toString(CharsetUtil.US_ASCII)
              if (!existingString.isEmpty && !existingString.matches(DIGITS))
                throw new ClientError("cannot increment or decrement non-numeric value")

              val existingValue =
                if (existingString.isEmpty) 0L
                else existingString.toLong

              val result = existingValue + delta
              data(key) = Entry(result.toString, entry.expiry)

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

case class Entry(value: ChannelBuffer, expiry: Time) {
  /**
   * Whether or not the cache entry has expired
   */
  def valid: Boolean = expiry == Time.epoch || Time.now < expiry
}

class InterpreterService(interpreter: Interpreter) extends Service[Command, Response] {
  def apply(command: Command) = Future(interpreter(command))
}
