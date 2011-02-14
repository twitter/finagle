package com.twitter.finagle.kestrel

import com.twitter.finagle.kestrel.protocol._
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers._
import com.twitter.conversions.time._
import _root_.java.util.concurrent.{TimeUnit, BlockingDeque}
import com.twitter.finagle.Service
import com.twitter.util.{StateMachine, Future}
import com.twitter.util.StateMachine.InvalidStateTransition

class Interpreter(queues: collection.mutable.Map[ChannelBuffer, BlockingDeque[ChannelBuffer]]) extends StateMachine {
  case class NoTransaction() extends State
  case class OpenTransaction(queueName: ChannelBuffer, item: ChannelBuffer) extends State
  state = NoTransaction()

  def apply(command: Command): Response = {
    command match {
      case Get(queueName, options) =>
        state match {
          case NoTransaction() =>
            if (options.contains(Abort())) throw new InvalidStateTransition("NoTransaction", "abort")
            if (options.contains(Close())) throw new InvalidStateTransition("NoTransaction", "close")

            val timeoutOption = options.find(_.isInstanceOf[Timeout]).asInstanceOf[Option[Timeout]]
            val wait = timeoutOption.map(_.duration).getOrElse(0.seconds)
            val item = queues(queueName).poll(wait.inMilliseconds, TimeUnit.MILLISECONDS)

            if (item eq null)
              Values(Seq.empty)
            else {
              if (options.contains(Open()))
                state = OpenTransaction(queueName, item)
              Values(Seq(Value(wrappedBuffer(queueName), wrappedBuffer(item))))
            }
          case OpenTransaction(txnQueueName, item) =>
            require(queueName == txnQueueName,
              "Cannot operate on a different queue than the one for which you have an open transaction")

            if (options.contains(Close())) {
              state = NoTransaction()
              apply(Get(queueName, options - Close()))
            } else if (options.contains(Abort())) {
              if (options.contains(Open())) throw new InvalidStateTransition("OpenTransaction", "open")

              queues(queueName).addFirst(item)
              state = NoTransaction()
              Values(Seq.empty)
            } else {
              throw new InvalidStateTransition("OpenTransaction", "get/" + options)
            }
        }
      case Set(queueName, expiry, value) =>
        queues(queueName).add(value)
        Stored()
      case Delete(queueName) =>
        queues.remove(queueName)
        Deleted()
      case Flush(queueName) =>
        queues.remove(queueName)
        Deleted()
      case FlushAll() =>
        queues.clear()
        Deleted()
      case Version() =>
        NotFound()
      case ShutDown() =>
        NotFound()
      case DumpConfig() =>
        NotFound()
      case Stats() =>
        NotFound()
      case DumpStats() =>
        NotFound()
      case Reload() =>
        NotFound()
    }
  }
}

class InterpreterService(interpreter: Interpreter) extends Service[Command, Response] {
  def apply(request: Command) = Future(interpreter(request))
  override def release() = ()
}
