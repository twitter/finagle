package com.twitter.finagle.exp.routing

private[routing] object FieldMap {

  /** Singleton, immutable instance of an empty [[FieldMap]] */
  val empty: FieldMap = new FieldMap(Map.empty)
}

/**
 * An immutable wrapper around a [[Map]] of [[Field fields]]. This class is meant to limit
 * access to the underlying map and *NOT* allow for iteration of the [[Field fields]] and their
 * values to ensure that privacy scoping of a [[Field]] will be honored by only being able to
 * set or retrieve specific [[Field fields]] within their allowed access.
 */
private[routing] class FieldMap(fields: Map[Field[_], Any]) {

  /**
   * Return `Some(value: FieldType)` for the specified [[Field field]]
   * if it is present in this [[FieldMap]], otherwise `None`.
   */
  def get[FieldType](field: Field[FieldType]): Option[FieldType] = fields.get(field) match {
    case r @ Some(_) => r.asInstanceOf[Some[FieldType]]
    case _ => None
  }

  /**
   * Return the [[FieldType value]] for the specified [[Field field]]
   * if it is present in this [[FieldMap]], otherwise return the
   * user supplied [[FieldType]].
   */
  def getOrElse[FieldType](field: Field[FieldType], orElse: => FieldType): FieldType =
    fields.get(field) match {
      case Some(r) => r.asInstanceOf[FieldType]
      case _ => orElse
    }

  /** Set the value for a [[FieldType field]] on this [[FieldMap]] */
  def set[FieldType](
    field: Field[FieldType],
    value: FieldType
  ): FieldMap = new FieldMap(fields + (field -> value))

}
