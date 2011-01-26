package com.twitter.finagle.memcached.protocol.text

import org.jboss.netty.buffer.ChannelBuffer

sealed abstract class Decoding
case class Tokens(tokens: Seq[ChannelBuffer])                              extends Decoding
case class TokensWithData(tokens: Seq[ChannelBuffer], data: ChannelBuffer) extends Decoding
case class ValueLines(lines: Seq[TokensWithData])                          extends Decoding

