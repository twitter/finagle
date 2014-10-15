package com.twitter.finagle.smtp.transport

private[smtp] object CodecUtil {
  val aggregation = "aggregateMultiline"

  def getInfo(rep: String): String = rep drop 4
  def getCode(rep: String): Int = rep.take(3).toInt
}
