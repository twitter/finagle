package com.twitter.finagle.http.exp.routing

import com.twitter.finagle.exp.routing.MessageField

object Fields {

  /**
   * A [[MessageField]] that represents the extracted [[ParameterMap]] found
   * when matching a [[Route]].
   */
  object ParameterMapField extends MessageField[ParameterMap]

  /**
   * A [[MessageField]] that represents the memoized [[com.twitter.finagle.http.Request.path]],
   * which does not include any potential query params from the
   * [[com.twitter.finagle.http.Request.uri]].
   */
  object PathField extends MessageField[String]
}
