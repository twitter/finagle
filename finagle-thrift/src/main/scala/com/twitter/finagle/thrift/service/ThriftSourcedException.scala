package com.twitter.finagle.thrift.service

import com.twitter.finagle.SourcedException

private[thrift] object ThriftSourcedException {

  def setServiceName(ex: Throwable, serviceName: String): Throwable = ex match {
    case se: SourcedException if !serviceName.isEmpty =>
      se.serviceName = serviceName
      se
    case _ => ex
  }
}
