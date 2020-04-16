package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.PgSqlSpec
import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.PgSqlClientError
import com.twitter.finagle.postgresql.PgSqlServerError
import com.twitter.finagle.postgresql.PropertiesSpec
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.postgresql.machine.StateMachine.Respond
import com.twitter.util.Throw
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalacheck.Prop
import org.specs2.matcher.MatchResult

abstract class MachineSpec[R <: Response] extends PgSqlSpec { self: PropertiesSpec =>

  sealed trait StepSpec
  case class checkResult(name: String)(val spec: PartialFunction[StateMachine.TransitionResult[_, R], MatchResult[_]]) extends StepSpec
  case class checkFailure(name: String)(val spec: Throwable => MatchResult[_]) extends StepSpec
  case class receive(msg: BackendMessage) extends StepSpec

  object receive {
    def error: BackendMessage.ErrorResponse = BackendMessage.ErrorResponse(Map.empty) // TODO
  }

  def oneMachineSpec(machine: StateMachine[R], allowPreemptiveFailure: Boolean = false)(checks: StepSpec*): MatchResult[_] = {
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
        previous must beLike[StateMachine.TransitionResult[machine.State, R]] {
          case StateMachine.Transition(s, _) =>
            step(machine.receive(s, msg), tail)
          // This allows inejecting backend messages in random places, which can result in
          //   inserting in a place where the machine wouldn't actually read the message.
          case StateMachine.Complete(_, Some(Throw(_: PgSqlClientError))) if allowPreemptiveFailure => ok
        }
    }

    step(machine.start, checks.toList)
  }

  // Given a list of steps, insert a ErrorResponse randomly and checks that the machine handled it
  def genError(xs: List[StepSpec]): Gen[List[StepSpec]] = {
    // take everything before a machine failure, ReadyForQuery message or some other error.
    val steps = xs.takeWhile {
      case receive(BackendMessage.ErrorResponse(_)) => false
      case receive(BackendMessage.ReadyForQuery(_)) => false
      case checkFailure(_) => false
      case _ => true
    }

    for {
      error <- Arbitrary.arbitrary[BackendMessage.ErrorResponse]
      insert <- Gen.choose(1, steps.size)
    } yield {
      val (head, _) = steps.splitAt(insert)
      head ++ List(
        receive(error), // TODO: ideally, we would conditionally add this if the state machine isn't already failed. See allowPreemptiveFailure
        checkFailure("handles injected failure") {
          case PgSqlServerError(e) => e must beEqualTo(error)
        }
      )
    }
  }

  def machineErrorSpec(machine: StateMachine[R])(steps: StepSpec*) = {
    Prop.forAll(genError(steps.toList)) { errorSteps =>
      oneMachineSpec(machine, allowPreemptiveFailure = true)(errorSteps: _*)
    }
  }

  // TODO: ideally we generate fragments here, but not sure how to do that with scalacheck
  def machineSpec(machine: StateMachine[R])(steps: StepSpec*) = {
    oneMachineSpec(machine)(steps: _*)
    machineErrorSpec(machine)(steps: _*)
  }
}
