
package com.twitter.finagle.http

import scala.collection.immutable

protected trait ParamMapVersionSpecific extends immutable.Map[String, String] {
  def setParam[B >: String](kv: (String, B)): ParamMap
  def clearParam(name: String): ParamMap
  def -(name: String): ParamMap = clearParam(name)
}