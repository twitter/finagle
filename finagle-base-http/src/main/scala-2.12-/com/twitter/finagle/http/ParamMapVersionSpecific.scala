package com.twitter.finagle.http

import scala.collection.immutable

protected trait ParamMapVersionSpecific
    extends immutable.Map[String, String]
    with immutable.MapLike[String, String, ParamMap] {
  protected def setParam[B >: String](kv: (String, B)): ParamMap
  protected def clearParam(name: String): ParamMap
  def -(name: String): ParamMap = clearParam(name)

  override def empty: ParamMap = ParamMap()
}
