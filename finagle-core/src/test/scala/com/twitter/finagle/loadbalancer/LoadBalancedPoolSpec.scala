package com.twitter.finagle.loadbalancer
 
// TODO:
//    "fall back to no filtering if everything is marked dead" in {
//      Time.withCurrentTimeFrozen { timeControl =>
//        val failureAccrual = new FailureAccrualStrategy[Int, Int](underlying, 2, 10.seconds)
//         
//        underlying.dispatch(123, services) returns Some(service, Future.exception(new Exception))
//        failureAccrual.dispatch(123, services) must beSomething
//        failureAccrual.dispatch(123, services) must beSomething
//        there were two(underlying).dispatch(123, services)
//         
//        // The one should have failed now. Make the other one fail as well.
//        (underlying.dispatch(123, Seq(service1))
//           returns Some(service1, Future.exception(new Exception)))
//        failureAccrual.dispatch(123, services) must beSomething
//        failureAccrual.dispatch(123, services) must beSomething
//        there were two(underlying).dispatch(123, services)
//        there were two(underlying).dispatch(123, Seq(service1))
//         
//        // They're both failed, so we should revert now.
//        failureAccrual.dispatch(123, services) must beSomething
//        there were three(underlying).dispatch(123, services)
//      }
