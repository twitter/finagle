package com.twitter.finagle.exp.routing

import scala.annotation.varargs

/**
 * A contract for a container envelope for a value,
 * which can allow for setting extra field metadata associated with the value.
 */
sealed abstract class Message[+T](val value: T, fields: Map[Field[_], Any]) {

  /** Set the value for a [[FieldType field]] on this [[Message message]] */
  def set[FieldType](field: Field[FieldType], value: FieldType): Message[T]

  /** Set the values for the corresponding [[Field fields]] on this [[Message message]] */
  def setAll(fields: Map[Field[_], Any]): Message[T]

  /** Set the values for the corresponding [[Field fields]] on this [[Message message]] */
  @varargs
  def setAll(fields: (Field[_], Any)*): Message[T]

  /**
   * Return the [[FieldType value]] for the specified [[Field field]]
   * if it is present in this [[Message]], otherwise return the
   * user supplied [[FieldType]].
   */
  def getOrElse[FieldType](field: Field[FieldType], orElse: => FieldType): FieldType =
    fields.get(field) match {
      case Some(value) => value.asInstanceOf[FieldType]
      case _ => orElse
    }

  /**
   * Return `Some(value: FieldType)` for the specified [[Field field]]
   * if it is present in this [[Message]], otherwise `None`.
   */
  def get[FieldType](field: Field[FieldType]): Option[FieldType] = fields.get(field) match {
    case r @ Some(_) => r.asInstanceOf[Some[FieldType]]
    case _ => None
  }

}

object Request {

  /** Create a new [[Request]] with `value` and no populated fields */
  def apply[T](value: T): Request[T] = new Request(value)
}

/**
 * A contract for a container envelope for a request value,
 * which can allow for setting extra field metadata associated with the value.
 */
class Request[+T](value: T, fields: Map[Field[_], Any]) extends Message[T](value, fields) {

  def this(value: T) = this(value, Map.empty)

  /** @inheritdoc */
  @varargs
  def setAll(fields: (Field[_], Any)*): Request[T] =
    copy(fields = this.fields ++ fields)

  /** @inheritdoc */
  def setAll(append: Map[Field[_], Any]): Request[T] =
    copy(fields = this.fields ++ fields)

  /** @inheritdoc */
  def set[FieldType](
    field: Field[FieldType],
    value: FieldType
  ): Request[T] = copy(fields + (field -> value))

  // create a new Request, where the `value` stays constant, but the `fields` are updated
  private[this] def copy(
    fields: Map[Field[_], Any]
  ): Request[T] = new Request(this.value, fields)

  override def equals(obj: Any): Boolean = obj match {
    case r: Request[T] => r.value == value
    case _ => super.equals(obj)
  }

}

object Response {

  /** Create a new [[Response]] with `value` and no populated fields */
  def apply[T](value: T): Response[T] = new Response(value)
}

/**
 * A contract for a container envelope for a response value,
 * which can allow for setting extra field metadata associated with the value.
 */
class Response[+T](value: T, fields: Map[Field[_], Any]) extends Message[T](value, fields) {

  def this(value: T) = this(value, Map.empty)

  /** @inheritdoc */
  @varargs
  def setAll(fields: (Field[_], Any)*): Response[T] =
    copy(this.fields ++ fields)

  /** @inheritdoc */
  def setAll(append: Map[Field[_], Any]): Response[T] =
    copy(this.fields ++ fields)

  /** @inheritdoc */
  def set[FieldType](
    field: Field[FieldType],
    value: FieldType
  ): Response[T] = copy(this.fields + (field -> value))

  // create a new Response, where the `value` stays constant, but the `fields` are updated
  private[this] def copy(
    fields: Map[Field[_], Any]
  ): Response[T] = new Response(this.value, fields)

  override def equals(obj: Any): Boolean = obj match {
    case r: Response[T] => r.value == value
    case _ => super.equals(obj)
  }
}
