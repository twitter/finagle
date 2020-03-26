package com.twitter.finagle.thrift.service

import com.twitter.finagle.service.{ReqRep, ResponseClass, ResponseClassifier}
import com.twitter.finagle.thrift.ThriftMethodStats
import com.twitter.scrooge.ThriftMethod
import com.twitter.util.{Throw, Throwables, Try}

private[thrift] object ThriftMethodStatsHandler {

  def apply(
    method: ThriftMethod
  )(
    responseClassifier: ResponseClassifier,
    thriftMethodStats: ThriftMethodStats,
    args: method.Args,
    response: Try[method.SuccessType]
  ): Unit = {
    val responseClass =
      responseClassifier.applyOrElse(ReqRep(args, response), ResponseClassifier.Default)
    responseClass match {
      case ResponseClass.Ignorable => // Do nothing.
      case ResponseClass.Successful(_) =>
        thriftMethodStats.successCounter.incr()
      case ResponseClass.Failed(_) =>
        thriftMethodStats.failuresCounter.incr()
        response match {
          case Throw(ex) =>
            thriftMethodStats.failuresScope.counter(Throwables.mkString(ex): _*).incr()
          case _ =>
        }
    }
  }
}
