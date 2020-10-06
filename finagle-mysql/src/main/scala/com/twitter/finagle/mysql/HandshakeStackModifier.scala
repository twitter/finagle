package com.twitter.finagle.mysql

import com.twitter.finagle.Stack

/**
 * Defines a modification of [[Stack.Params]] based on the initial server handshake message.
 *
 * This is useful for clients that want to behave differently based on the Mysql server version, for example.
 *
 * @param modifyParams returns the modified [[Stack.Params]] based on the input handshake message
 */
private[finagle] case class HandshakeStackModifier(
  modifyParams: (Stack.Params, HandshakeInit) => Stack.Params)

private[finagle] object HandshakeStackModifier {
  implicit val param = Stack.Param(HandshakeStackModifier((params, _) => params))
}
