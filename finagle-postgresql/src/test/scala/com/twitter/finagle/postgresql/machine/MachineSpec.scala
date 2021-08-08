package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql._
import com.twitter.finagle.postgresql.machine.StateMachine.Respond
import com.twitter.util.Throw
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.Assertion
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

abstract class MachineSpec[R <: Response] extends PgSqlSpec {
  self: PropertiesSpec with ScalaCheckDrivenPropertyChecks =>

  sealed trait StepSpec
  case class checkResult(
    name: String
  )(
    val spec: PartialFunction[StateMachine.TransitionResult[_, R], Assertion])
      extends StepSpec
  case class checkFailure(name: String)(val spec: Throwable => Assertion) extends StepSpec
  case class receive(msg: BackendMessage) extends StepSpec

  object receive {
    def error: BackendMessage.ErrorResponse = BackendMessage.ErrorResponse(Map.empty) // TODO
  }

  def oneMachineSpec(
    machine: StateMachine[R],
    allowPreemptiveFailure: Boolean = false
  )(
    checks: StepSpec*
  ): Assertion = {
    def step(
      previous: StateMachine.TransitionResult[machine.State, R],
      remains: List[StepSpec]
    ): Assertion =
      remains match {
        case Nil => succeed
        case (c @ checkResult(name)) :: tail =>
          previous must beLike(c.spec) //.updateMessage(msg => s"$name: $msg")
          step(previous, tail)
        case (c @ checkFailure(name)) :: tail =>
          previous must beLike[StateMachine.TransitionResult[machine.State, R]] {
            case StateMachine.Transition(_, Respond(Throw(ex))) => c.spec(ex)
            case StateMachine.Complete(_, Some(Throw(ex))) => c.spec(ex)
          } //.updateMessage(msg => s"$name: $msg")

          step(previous, tail)
        case receive(msg) :: tail =>
          previous must beLike[StateMachine.TransitionResult[machine.State, R]] {
            case StateMachine.Transition(s, _) =>
              step(machine.receive(s, msg), tail)
            // This allows inejecting backend messages in random places, which can result in
            //   inserting in a place where the machine wouldn't actually read the message.
            case StateMachine.Complete(_, Some(Throw(_: PgSqlClientError)))
                if allowPreemptiveFailure =>
              succeed
          }
      }

    step(machine.start, checks.toList)
  }

  type ErrorHandler = BackendMessage.ErrorResponse => List[StepSpec]
  protected val defaultErrorHandler: ErrorHandler = (error: BackendMessage.ErrorResponse) =>
    checkFailure("handles injected failure") {
      case PgSqlServerError(e) => e must equal(error)
    } :: Nil

  // Given a list of steps, insert a ErrorResponse randomly and checks that the machine handled it
  def genError(xs: List[StepSpec], errorHandler: ErrorHandler): Gen[List[StepSpec]] = {
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
      head ++ (receive(error) :: errorHandler(error))
    }
  }

  def machineErrorSpec(
    machine: StateMachine[R],
    errorHandler: ErrorHandler = defaultErrorHandler
  )(
    steps: StepSpec*
  ) = {
    forAll(genError(steps.toList, errorHandler)) { errorSteps =>
      oneMachineSpec(machine, allowPreemptiveFailure = true)(errorSteps: _*)
    }
  }

  // TODO: ideally we generate fragments here, but not sure how to do that with scalacheck
  def machineSpec(
    machine: StateMachine[R],
    errorHandler: ErrorHandler = defaultErrorHandler
  )(
    steps: StepSpec*
  ) = {
    oneMachineSpec(machine)(steps: _*)
    machineErrorSpec(machine, errorHandler)(steps: _*)
  }
}
