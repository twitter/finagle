package com.twitter.finagle.http

object DefaultHeaderMap {
  def apply(headers: (String, String)*): HeaderMap = HashBackedHeaderMap(headers:_*)
}