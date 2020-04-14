package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.PgSqlSpec
import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.postgresql.machine.StateMachine.Respond
import com.twitter.util.Throw
import org.specs2.matcher.MatchResult

abstract class MachineSpec[R <: Response] extends PgSqlSpec {

  sealed trait StepSpec
  case class checkResult(name: String)(val spec: PartialFunction[StateMachine.TransitionResult[_, R], MatchResult[_]]) extends StepSpec
  case class checkFailure(name: String)(val spec: Throwable => MatchResult[_]) extends StepSpec
  case class receive(msg: BackendMessage) extends StepSpec

  def machineSpec(machine: StateMachine[R])(checks: StepSpec*) = {
    def step(previous: StateMachine.TransitionResult[machine.State, R], remains: List[StepSpec]): MatchResult[_] = remains match {
      case Nil => ok
      case (c@checkResult(name)) :: tail =>
        previous must beLike(c.spec).updateMessage(msg => s"$name: $msg")
        step(previous, tail)
      case (c@checkFailure(name)) :: tail =>

        previous must beLike[StateMachine.TransitionResult[machine.State, R]] {
          case StateMachine.Transition(_, Respond(Throw(ex))) => c.spec(ex)
          case StateMachine.Complete(_, Some(Throw(ex))) => c.spec(ex)
        }.updateMessage(msg => s"$name: $msg")

        step(previous, tail)
      case receive(msg) :: tail =>
        previous must beLike {
          case StateMachine.Transition(s, _) =>
            step(machine.receive(s, msg), tail)
        }
    }

    step(machine.start, checks.toList)
  }
}
