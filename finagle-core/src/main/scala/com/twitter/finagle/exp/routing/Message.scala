package com.twitter.finagle.exp.routing

/**
 * A contract for a container envelope for a value,
 * which can allow for setting extra field metadata associated with the value.
 */
sealed abstract class Message[+T](val value: T, fields: Map[MessageField[_], Any])
    extends FieldMap(fields.asInstanceOf[Map[Field[_], Any]])

object Request {

  /** Create a new [[Request]] with `value` and no populated fields */
  def apply[T](value: T): Request[T] = new Request(value)
}

/**
 * A contract for a container envelope for a request value,
 * which can allow for setting extra field metadata associated with the value.
 */
final class Request[+T](value: T, fields: Map[MessageField[_], Any])
    extends Message[T](value, fields) {

  def this(value: T) = this(value, Map.empty)

  /**
   * Return `Some(value: FieldType)` for the specified [[MessageField field]]
   * if it is present in the [[FieldMap]], otherwise `None`.
   */
  def get[FieldType](
    field: MessageField[FieldType]
  ): Option[FieldType] = super.get(field)

  /**
   * Return the [[FieldType value]] for the specified [[MessageField field]]
   * if it is present in the [[FieldMap]], otherwise return the
   * user supplied [[FieldType]].
   */
  def getOrElse[FieldType](field: MessageField[FieldType], orElse: => FieldType): FieldType =
    super.getOrElse(field, orElse)

  /** @inheritdoc */
  def set[FieldType](
    field: MessageField[FieldType],
    value: FieldType
  ): Request[T] = new Request(this.value, this.fields + (field -> value))

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
final class Response[+T](value: T, fields: Map[MessageField[_], Any])
    extends Message[T](value, fields) {

  def this(value: T) = this(value, Map.empty)

  /**
   * Return `Some(value: FieldType)` for the specified [[MessageField field]]
   * if it is present in the [[FieldMap]], otherwise `None`.
   */
  def get[FieldType](
    field: MessageField[FieldType]
  ): Option[FieldType] = super.get(field)

  /**
   * Return the [[FieldType value]] for the specified [[MessageField field]]
   * if it is present in the [[FieldMap]], otherwise return the
   * user supplied [[FieldType]].
   */
  def getOrElse[FieldType](field: MessageField[FieldType], orElse: => FieldType): FieldType =
    super.getOrElse(field, orElse)

  /** @inheritdoc */
  def set[FieldType](
    field: MessageField[FieldType],
    value: FieldType
  ): Response[T] = new Response(this.value, this.fields + (field -> value))

  override def equals(obj: Any): Boolean = obj match {
    case r: Response[T] => r.value == value
    case _ => super.equals(obj)
  }
}
