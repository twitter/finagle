package com.twitter.finagle.http.exp.routing

/** A lookup from [[String name]] to [[ParameterValue parameter value]]. */
private[http] sealed abstract class ParameterMap {

  /** Return true if a named parameter is present, false otherwise. */
  def isDefinedAt(name: String): Boolean

  /** Return Some(ParameterValue) for a named parameter if it is present, None otherwise. */
  private[routing] def getParam(name: String): Option[ParameterValue]

  /** Return Some(classOf[*type*]) for a named parameter if it is present, None otherwise. */
  def getParamClass(name: String): Option[Class[_]]

  /** Return Some(String) value for a named parameter if it is present, None otherwise. */
  def get(name: String): Option[String]

  /** Return Some(String) value for a named parameter if it is present and a [[StringParam]], None otherwise. */
  def getString(name: String): Option[String]

  /** Return Some(Int) value for a named parameter if it is present and a [[IntParam]], None otherwise. */
  def getInt(name: String): Option[Int]

  /** Return Some(Long) value for a named [[Parameter]] if it is present and a [[LongParam]], None otherwise. */
  def getLong(name: String): Option[Long]

  /** Return Some(Boolean) value for a named [[Parameter]] if it is present and a [[BooleanParam]], None otherwise. */
  def getBoolean(name: String): Option[Boolean]

  /** Return Some(Float) value for a named [[Parameter]] if it is present and a [[FloatParam]], None otherwise. */
  def getFloat(name: String): Option[Float]

  /** Return Some(Double) value for a named [[Parameter]] if it is present and a [[DoubleParam]], None otherwise. */
  def getDouble(name: String): Option[Double]
}

/** A [[ParameterMap]] that contains no results. */
private[http] object EmptyParameterMap extends ParameterMap {
  def isDefinedAt(name: String): Boolean = false
  private[routing] def getParam(name: String): Option[ParameterValue] = None
  def getParamClass(name: String): Option[Class[_]] = None
  def get(name: String): Option[String] = None
  def getString(name: String): Option[String] = None
  def getInt(name: String): Option[Int] = None
  def getLong(name: String): Option[Long] = None
  def getBoolean(name: String): Option[Boolean] = None
  def getFloat(name: String): Option[Float] = None
  def getDouble(name: String): Option[Double] = None
}

/** A [[ParameterMap]] backed by a [[Map]]. */
private[http] case class MapParameterMap private[routing] (
  private val underlying: Map[String, ParameterValue])
    extends ParameterMap {

  def isDefinedAt(name: String): Boolean = underlying.isDefinedAt(name)

  def getParamClass(name: String): Option[Class[_]] = underlying.get(name) match {
    case Some(_: StringValue) => Some(classOf[String])
    case Some(_: IntValue) => Some(classOf[Int])
    case Some(_: LongValue) => Some(classOf[Long])
    case Some(_: BooleanValue) => Some(classOf[Boolean])
    case Some(_: FloatValue) => Some(classOf[Float])
    case Some(_: DoubleValue) => Some(classOf[Double])
    case _ => None
  }

  private[routing] def getParam(name: String): Option[ParameterValue] = underlying.get(name)

  def get(name: String): Option[String] = underlying.get(name) match {
    case Some(pv) => Some(pv.value)
    case _ => None
  }

  def getString(name: String): Option[String] = underlying.get(name) match {
    case Some(pv: StringValue) => Some(pv.value)
    case _ => None
  }

  def getInt(name: String): Option[Int] = underlying.get(name) match {
    case Some(i: IntValue) => Some(i.intValue)
    case _ => None
  }

  def getLong(name: String): Option[Long] = underlying.get(name) match {
    case Some(l: LongValue) => Some(l.longValue)
    case _ => None
  }

  def getBoolean(name: String): Option[Boolean] = underlying.get(name) match {
    case Some(b: BooleanValue) => Some(b.booleanValue)
    case _ => None
  }

  def getFloat(name: String): Option[Float] = underlying.get(name) match {
    case Some(f: FloatValue) => Some(f.floatValue)
    case _ => None
  }

  def getDouble(name: String): Option[Double] = underlying.get(name) match {
    case Some(d: DoubleValue) => Some(d.doubleValue)
    case _ => None
  }

}
