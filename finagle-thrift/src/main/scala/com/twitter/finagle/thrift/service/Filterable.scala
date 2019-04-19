package com.twitter.finagle.thrift.service

import com.twitter.finagle.Filter

/**
 * Used in conjunction with a `ServicePerEndpoint` builder to allow for filtering
 * of a `ServicePerEndpoint`.
 */
trait Filterable[+T] extends com.twitter.finagle.service.Filterable[T] {

  /**
   * Prepend the given type-agnostic [[Filter]].
   */
  def filtered(filter: Filter.TypeAgnostic): T

}
