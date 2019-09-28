package com.twitter.finagle.http.headers

object DefaultHeaderMap {
  def apply(headers: (String, String)*): HeaderMap = HashBackedHeaderMap(headers:_*)
}