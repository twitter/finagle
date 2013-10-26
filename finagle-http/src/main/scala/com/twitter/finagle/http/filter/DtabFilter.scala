package com.twitter.finagle.http.filter

import com.twitter.finagle.{Dtab, SimpleFilter, Service}
import org.jboss.netty.handler.codec.http.HttpMessage
import com.twitter.finagle.http.codec.HttpDtab

/**
 * Delegate to the dtab contained inside of the request.
 */
private[finagle] class DtabFilter[Req <: HttpMessage, Rep <: HttpMessage]
    extends SimpleFilter[Req, Rep] {

  def apply(req: Req, service: Service[Req, Rep]) = {
    val dtab = HttpDtab.read(req)
    if (dtab.isEmpty) service(req) else Dtab.unwind {
      Dtab.delegate(dtab)
      HttpDtab.clear(req)
      service(req)
    }
  }
}
