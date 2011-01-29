package com.twitter.finagle.memcached

import com.twitter.finagle.memcached.protocol._
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer
import org.jboss.netty.util.CharsetUtil
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.util.Future
import com.twitter.finagle.Service
import com.twitter.finagle.memcached.util.{ParserUtils, AtomicMap}

/**
 * Evalutes a given Memcached operation and returns the result.
 */
class Interpreter(map: AtomicMap[ChannelBuffer, ChannelBuffer]) {
  import ParserUtils._

  def apply(command: Command): Response = {
    command match {
      case Set(key, flags, expiry, value) =>
        map.lock(key) { data =>
          data(key) = value
          Stored()
        }
      case Add(key, flags, expiry, value) =>
        map.lock(key) { data =>
          val existing = data.get(key)
          if (existing.isDefined)
            NotStored()
          else {
            data(key) = value
            Stored()
          }
        }
      case Replace(key, flags, expiry, value) =>
        map.lock(key) { data =>
          val existing = data.get(key)
          if (existing.isDefined) {
            data(key) = value
            Stored()
          } else {
            NotStored()
          }
        }
      case Append(key, flags, expiry, value) =>
        map.lock(key) { data =>
          val existing = data.get(key)
          if (existing.isDefined) {
            data(key) = wrappedBuffer(value, existing.get)
            Stored()
          } else {
            NotStored()
          }
        }
      case Prepend(key, flags, expiry, value) =>
        map.lock(key) { data =>
          val existing = data.get(key)
          if (existing.isDefined) {
            data(key) = wrappedBuffer(existing.get, value)
            Stored()
          } else {
            NotStored()
          }
        }
      case Get(keys) =>
        Values(
          keys flatMap { key =>
            map.lock(key) { data =>
              data.get(key) map { datum =>
                Value(key, wrappedBuffer(datum))
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
          if (existing.isDefined) {
            val existingString = existing.get.toString(CharsetUtil.US_ASCII)
            if (!existingString.isEmpty && !existingString.matches(DIGITS))
              throw new ClientError("cannot increment or decrement non-numeric value")

            val existingValue =
              if (existingString.isEmpty) 0
              else existingString.toInt

            val result = existingValue + delta
            data(key) = result.toString

            Number(result)
          } else {
            NotFound()
          }
        }
      case Decr(key, value) =>
        map.lock(key) { data =>
          apply(Incr(key, -value))
        }
    }
  }
}

class InterpreterService(interpreter: Interpreter) extends Service[Command, Response] {
  def apply(command: Command) = Future(interpreter(command))
  override def release() = ()
}
