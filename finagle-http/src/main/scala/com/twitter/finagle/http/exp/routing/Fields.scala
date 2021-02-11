package com.twitter.finagle.http.exp.routing

import com.twitter.finagle.exp.routing.Field

object Fields {

  /** A [[Field]] that represents the extracted [[ParameterMap]] found when matching a [[Route]]. */
  object ParameterMapField extends Field[ParameterMap]

}
