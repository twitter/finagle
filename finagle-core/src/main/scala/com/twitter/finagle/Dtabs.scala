package com.twitter.finagle

/**
 * Static methods for Java callers
 */
object Dtabs {
  def empty() = Dtab(Vector.empty[Dentry])
  def local() = Dtab.local
  def base() = Dtab.base
  def setLocal(dtab: Dtab): Unit = Dtab.local = dtab
  def setBase(dtab: Dtab): Unit = Dtab.base = dtab
  
}