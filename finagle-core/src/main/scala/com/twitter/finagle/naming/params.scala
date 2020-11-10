package com.twitter.finagle.naming

import com.twitter.finagle.{Name, Stack}

/**
 * A class eligible for configuring how paths will be displayed in metrics
 * metadata
 */
case class DisplayBoundName(displayFn: Name.Bound => String)

object DisplayBoundName {
  private[finagle] val Default: Name.Bound => String = { name: Name.Bound =>
    name.idStr
  }

  implicit val param: Stack.Param[DisplayBoundName] = Stack.Param(DisplayBoundName(Default))
}

