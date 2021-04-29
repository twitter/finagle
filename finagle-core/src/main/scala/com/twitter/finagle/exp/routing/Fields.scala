package com.twitter.finagle.exp.routing

object Fields {

  /**
   * A [[Field]] that will contain the found [[Route]] information, within the context of a
   * [[RoutingService]] request.
   */
  object RouteInfo extends MessageField[Route[_, _, _]]
}
