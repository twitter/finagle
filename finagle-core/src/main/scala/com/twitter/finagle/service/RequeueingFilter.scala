package com.twitter.finagle.service

import com.twitter.finagle.{Stack, Stackable, param, ServiceFactory}

object RequeueingFilter {
  val role = Stack.Role("RequeueingFilter")

  // A cap on the number of retries on WriteExceptions
  // If you exceed 25 WriteExceptions, there is probably a bug
  private[this] val MaxRetries = 25

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module2[param.Stats, param.Timer, ServiceFactory[Req, Rep]] {
      val role = RequeueingFilter.role
      val description = "Retry automatically on WriteExceptions"
      def make(stats: param.Stats, prmTimer: param.Timer, next: ServiceFactory[Req, Rep]) = {
        val param.Timer(timer) = prmTimer
        val param.Stats(sr) = stats
        val policy = RetryPolicy.tries(MaxRetries, RetryPolicy.WriteExceptionsOnly)
        new RetryingFilter[Req, Rep](policy, timer, sr.scope("automatic")) andThen next
      }
    }
}
