package com.twitter.finagle.http.headers

import com.twitter.finagle.http.HeaderMap

object DefaultHeaderMap {
  def apply(headers: (String, String)*): HeaderMap = JTreeMapBackedHeaderMap(headers: _*)
}
