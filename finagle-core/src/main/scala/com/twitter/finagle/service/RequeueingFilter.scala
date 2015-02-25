package com.twitter.finagle.service

import com.twitter.finagle.{param, ServiceFactory, Stack, Stackable, Status}

object RequeueingFilter {
  val role = Stack.Role("RequeueingFilter")

  // A cap on the number of tries on WriteExceptions
  private[finagle] val MaxTries = 25

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module2[param.Stats, param.Timer, ServiceFactory[Req, Rep]] {
      val role = RequeueingFilter.role
      val description = "Retry automatically on WriteExceptions"

      def make(stats: param.Stats, prmTimer: param.Timer, next: ServiceFactory[Req, Rep]) = {

        val param.Timer(timer) = prmTimer
        val param.Stats(sr) = stats

        // requeue WriteException failures iff the stack is Open
        val policy = RetryPolicy
          .tries(MaxTries, RetryPolicy.WriteExceptionsOnly)
          .filterEach { (_: Any) => next.status == Status.Open }

        new RetryingFilter[Req, Rep](policy, timer, sr.scope("automatic")) andThen next
      }
    }
}
