package com.twitter.finagle.http

import scala.collection.immutable
protected abstract class ParamMapVersionSpecific extends immutable.Map[String, String] {
  protected def setParam[B >: String](kv: (String, B)): ParamMap
  def updated[V1 >: String](key: String, value: V1): ParamMap = setParam(key, value)

  protected def clearParam(name: String): ParamMap
  def removed(name: String): ParamMap = clearParam(name)
}
