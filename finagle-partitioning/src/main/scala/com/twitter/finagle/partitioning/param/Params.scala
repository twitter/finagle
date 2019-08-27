package com.twitter.finagle.partitioning.param

import com.twitter.finagle.Stack
import com.twitter.hashing

case class EjectFailedHost(v: Boolean) {
  def mk(): (EjectFailedHost, Stack.Param[EjectFailedHost]) =
    (this, EjectFailedHost.param)
}

object EjectFailedHost {
  implicit val param: Stack.Param[EjectFailedHost] = Stack.Param(EjectFailedHost(false))
}

case class KeyHasher(hasher: hashing.KeyHasher) {
  def mk(): (KeyHasher, Stack.Param[KeyHasher]) =
    (this, KeyHasher.param)
}

object KeyHasher {
  implicit val param: Stack.Param[KeyHasher] = Stack.Param(KeyHasher(hashing.KeyHasher.KETAMA))
}

case class NumReps(reps: Int) {
  def mk(): (NumReps, Stack.Param[NumReps]) =
    (this, NumReps.param)
}

object NumReps {
  val Default = 160

  implicit val param: Stack.Param[NumReps] = Stack.Param(NumReps(Default))
}
