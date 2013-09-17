/**
 * Copyright 2012-2013  Foursquare Labs, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.finagle.dual

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.FrameDecoder
import scala.annotation.tailrec
import scalaj.collection.Imports._

object DualProtocolFrameDecoder {
   // Helpers for the state machine that detects HTTP.

  case class Transition(ch: Byte, state: Int)
  object Transition {
    def apply(ch: Char, state: Int): Transition = apply(ch.toByte, state)
  }

  case class State(transitions: Seq[Transition] = Seq(), isFinal: Boolean = false)

  object State {
    def apply(transitions: Transition*): State = State(transitions = transitions)
  }

  // Simple state machine to recognize GET, HEAD, POST, PUT, and DELETE.
  val httpMethodStates = Array[State](
    /*  0 */ State(Transition('G', 1), Transition('P', 7), Transition('H', 4), Transition('D', 12)),
    /*  1 */ State(Transition('E', 2)),
    /*  2 */ State(Transition('T', 3)),
    /*  3 */ State(isFinal = true),
    /*  4 */ State(Transition('E', 5)),
    /*  5 */ State(Transition('A', 6)),
    /*  6 */ State(Transition('D', 3)),
    /*  7 */ State(Transition('O', 8), Transition('U', 9)),
    /*  8 */ State(Transition('S', 9)),
    /*  9 */ State(Transition('T', 3)),
    /* 10 */ State(Transition('E', 11)),
    /* 11 */ State(Transition('L', 12)),
    /* 12 */ State(Transition('E', 13)),
    /* 13 */ State(Transition('T', 14)),
    /* 14 */ State(Transition('E', 3))
  )

  // Decisions made by the HTTP-detection state machine.
  sealed trait ProtocolDecision
  case object NoDecision extends ProtocolDecision
  case object HttpDecision extends ProtocolDecision
  case object NotHttpDecision extends ProtocolDecision
}


/**
 * A Netty [[org.jboss.netty.handler.codec.frame.FrameDecoder]] that detects the start of the HTTP protocol and
 * switches the Netty pipeline it is in to a HTTP protocol stack. If HTTP is not detected, the pipeline is left alone
 * and the existing handlers remain in place.
 *
 * Theory of Operation:
 *
 * As data is received by Netty, it eventually reaches this class for processing in the decode() method. That method
 * uses a simple state machine to recognize whether an HTTP method name (i.e., GET, POST, HEAD, PUT, or DELETE) forms
 * the start of the network exchange. The state machine can either recognize HTTP, reject HTTP, or require more input.
 *
 * If the state machine rejects HTTP, then this class simply "gets out of the way" by removing itself from the Netty
 * pipeline to allow the other handlers in the pipeline to process the data. This is an optimization for the non-HTTP
 * case since we expect HTTP to be used for admin purposes. The data is injected again into the pipeline by
 * returning a new ChannelBuffer with the same data. (FrameDecoder checks to see if we consumed data if we return the
 * same ChannelBuffer and throws an exception if we didn't. This is not optimal behavior to say the least! It would
 * be more useful if Netty supported reinjecting the same ChannelBuffer.)
 *
 * If the state machine recognizes HTTP, it removes the existing handlers from the pipeline, adds the handlers for
 * the HTTP protocol stack after itself, then finally removes itself from the pipeline. (Removal of this class from the
 * pipeline must occur *last* as per the Netty javadocs for FrameDecoer.) The data is then injected back into the
 * pipeline in the same manner as before.
 *
 * If the decoder does not have enough information yet (e.g., it only sees 'G' and 'E'), it returns null so that
 * FrameDecoder will invoke it later when more data is available.
 *
 * NOTES:
 * - "Unfold" mode is disabled (via the base class' constructor parameter). This causes Object arrays returned by the
 * decode() method to be "unfolded" before the frames are re-injected into the pipeline. We do not use this feature
 * so it has been disabled.
 */
class DualProtocolFrameDecoder(
    thisHandlerName: String,
    httpPipeline: ChannelPipeline,
    regularHandlers: Seq[ChannelHandler]) extends FrameDecoder(false) {

  /**
   * Transfer the handlers in otherPipeline into pipeline after the handler named afterHandlerName. The handlers will
   * no longer be in otherPipeline once this operation completes.
   *
   * @param pipeline to which the handlers should be added
   * @param afterHandlerName name of handler after which the handlers from otherPipeline should be added
   * @param otherPipeline contains the handlers to be transferred
   */
  protected def patchPipeline(pipeline: ChannelPipeline, afterHandlerName: String, otherPipeline: ChannelPipeline) {
    val names = otherPipeline.toMap.asScala

    var previousName = afterHandlerName
    names.foreach { case (name, handler) =>
      otherPipeline.remove(handler)
      pipeline.addAfter(previousName, name, handler)
      previousName = name
    }
  }

  /**
   * Decide whether or not a data stream contains HTTP.
   *
   * @param context in which this class is operating
   * @param channel underlying channel
   * @param buffer contains the data on which to pass judgment
   * @return an object that the FrameDecoder can inject into the pipeline
   */
  protected override def decode(context: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): AnyRef = {
    import DualProtocolFrameDecoder.{HttpDecision, NoDecision, NotHttpDecision, ProtocolDecision, State, httpMethodStates}

    @tailrec
    def nextState(buf: ChannelBuffer, currentState: State): Either[ProtocolDecision, State] = {
      if (currentState.isFinal) {
        Right(currentState)
      } else {
        if (buf.readableBytes() >= 1) {
          val b = buf.readByte()
          currentState.transitions.find(_.ch == b) match {
            case Some(transition) => nextState(buf, httpMethodStates(transition.state))
            case None => Left(NotHttpDecision)
          }
        } else {
          Right(currentState)
        }
      }
    }

    buffer.markReaderIndex()
    val decision = nextState(buffer, httpMethodStates(0)) match {
      case Right(state) => if (state.isFinal) HttpDecision else NoDecision
      case Left(theDecision) => theDecision
    }
    buffer.resetReaderIndex()

    decision match {
      // If no decision can be made, tell FrameDecoder to invoke us later when there is more data available.
      case NoDecision => null

      case HttpDecision =>
        // Remove the non-HTTP protocol's handlers. Then install the HTTP handlers. Then remove this handler last.
        // (See class docs for the reason.) Finally, have the pipeline process the data anew (by returning a new
        // ChannelBuffer).
        val pipeline = context.getPipeline
        regularHandlers.foreach(handler => pipeline.remove(handler))
        patchPipeline(pipeline, thisHandlerName, httpPipeline)
        pipeline.remove(this)
        buffer.readBytes(buffer.readableBytes())

      case NotHttpDecision =>
        // The pipeline already contains the non-HTTP protocol's handlers. We only need to remove this class from the
        // pipeline and then have the pipeline process the data anew (by returning a new ChannelBuffer).
        context.getPipeline.remove(this)
        buffer.readBytes(buffer.readableBytes())
    }
  }
}
