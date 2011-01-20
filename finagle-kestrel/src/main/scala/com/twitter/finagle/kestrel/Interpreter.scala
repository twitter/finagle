package com.twitter.finagle.kestrel

import com.twitter.finagle.kestrel.protocol._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.conversions.time._
import java.util.concurrent.{TimeUnit, BlockingQueue}
import com.twitter.util.{Future, MapMaker}

class Interpreter(Queue: () => BlockingQueue[ChannelBuffer]) {
  private[this] val queues = MapMaker[ChannelBuffer, BlockingQueue[ChannelBuffer]] { config =>
    config.compute { key =>
      Queue()
    }
  }

  def apply(command: Command): Response = {
    command match {
      case Get(queueName, options) =>
        val timeoutOption = options.find(_.isInstanceOf[Timeout]).asInstanceOf[Option[Timeout]]
        val wait = timeoutOption.map(_.duration).getOrElse(0.seconds)
        val item = queues(queueName).poll(wait.inMilliseconds, TimeUnit.MILLISECONDS)
        if (item eq null)
          Values(Seq.empty)
        else
          Values(Seq(Value(queueName, item)))
      case Set(queueName, flags, expiry, value) =>
        queues(queueName).add(value)
        Stored
      case Delete(queueName) =>
        queues.remove(queueName)
        Deleted
      case Flush(queueName) =>
        queues.remove(queueName)
        Deleted
      case FlushAll() =>
        queues.clear()
        Deleted
      case Version() =>
        NotFound
      case ShutDown() =>
        NotFound
      case DumpConfig() =>
        NotFound
      case Stats() =>
        NotFound
      case DumpStats() =>
        NotFound
    }
  }
}