package com.twitter.finagle.util

import com.twitter.finagle.Stack
import com.twitter.util.Var

case class TestParam(p1: Int) {
  def mk() = (this, TestParam.param)
}
object TestParam {
  implicit val param = Stack.Param(TestParam(1))
}

case class TestParam2(p2: Int) {
  def mk() = (this, TestParam2.param)
}
object TestParam2 {
  implicit val param = Stack.Param(TestParam2(1))
}

class NotCaseClassParam(val ncc: Var[Int]) {
  def mk() = (this, NotCaseClassParam.param)

}
object NotCaseClassParam {
  implicit val param = new Stack.Param[NotCaseClassParam] {
    val default = new NotCaseClassParam(Var(3))
    override def show(p: NotCaseClassParam): Seq[(String, () => String)] =
      Seq(("ncc", () => p.ncc.sample().toString))
  }
}

