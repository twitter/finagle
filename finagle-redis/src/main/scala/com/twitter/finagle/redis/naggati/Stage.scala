/*
 * Copyright 2010 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.finagle.redis.naggati

import org.jboss.netty.buffer.ChannelBuffer

/**
 * The next step for processing after this stage.
 * - Incomplete: Need more data; call the same stage again later when more data arrives.
 * - GoToStage(stage): Finished with this stage; continue with another stage.
 * - Emit(obj): Complete protocol object decoded; return to the first stage and start a new object.
 */
sealed trait NextStep
case object Incomplete extends NextStep
case class GoToStage(stage: Stage) extends NextStep
case class Emit(obj: AnyRef) extends NextStep

/**
 * A decoder stage.
 */
trait Stage {
  def apply(buffer: ChannelBuffer): NextStep
}
