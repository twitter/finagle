
package com.twitter.finagle.http

protected abstract class ParamMapVersionSpecific {
  def setParam[B >: String](kv: (String, B)): ParamMap
  def updated[V1 >: String](key: String, value: V1): ParamMap = setParam(key, value)
  def +[V1 >: String](key: String, value: V1): ParamMap = setParam(key, value)

  def clearParam(name: String): ParamMap
  def removed(name: String): ParamMap = clearParam(name)

}