package com.twitter.finagle.stats

import java.util.NoSuchElementException

private object Helpers {

  /** Extract a metric snapshot from the underlying collection using it's hierarchical name */
  def get[T <: MetricsView.Snapshot](name: String, i: Iterable[T]): T = {
    find(name, i) match {
      case Some(t) => t
      case None => throw new NoSuchElementException(name)
    }
  }

  /**
   * Optionally extract a metric snapshot from the underlying collection using it's hierarchical
   * name
   */
  def find[T <: MetricsView.Snapshot](name: String, i: Iterable[T]): Option[T] = {
    i.find(_.hierarchicalName == name)
  }
}
