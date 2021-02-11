package com.twitter.finagle.http.exp

import com.twitter.finagle.http.{Request, Response}

package object routing {

  type Route = com.twitter.finagle.exp.routing.Route[Request, Response, Schema]

}
