package com.twitter.finagle.exp.routing

/**
 * The key associated with a field value of type `T`.
 * @tparam T The type of value associated with this message field key.
 */
sealed abstract class Field[+T]

/**
 * The key associated with a [[Message]] field.
 * @tparam T The type of value associated with this message field key.
 */
abstract class MessageField[+T] extends Field[T]

/**
 * The key associated with a [[Route]] field.
 * @tparam T The type of value associated with this message field key.
 */
abstract class RouteField[+T] extends Field[T]
