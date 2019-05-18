package com.twitter.finagle.service

import com.twitter.finagle.Filter

/**
 * Allows filtering of a `ServicePerEndpoint` when constructing new ServicePerEndpoint
 * through `MethodBuilder`
 */
trait Filterable[+T] {

  /**
   * Prepend the given type-agnostic [[Filter]].
   */
  def filtered(filter: Filter.TypeAgnostic): T
}
