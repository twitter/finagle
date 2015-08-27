package com.twitter.finagle.memcached.protocol.text

import com.twitter.io.Buf

sealed abstract class Decoding
case class Tokens(tokens: Seq[Buf]) extends Decoding
case class TokensWithData(
    tokens: Seq[Buf],
    data: Buf,
    casUnique: Option[Buf] = None)
  extends Decoding
case class ValueLines(lines: Seq[TokensWithData]) extends Decoding
case class StatLines(lines: Seq[Tokens]) extends Decoding
