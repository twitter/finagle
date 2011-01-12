package com.twitter.finagle.service

import com.twitter.util.Future

/**
 * A Service is an asynchronous function from Request to Future[Response]. It is the
 * basic unit of an RPC interface.
 *
 * Currently this interface doesn't support streaming responses.  This
 * can be tackled in a number of ways:
 *
 *   - a similar continuation-future passing scheme
 *   - create a Future[] subclass (eg. ContinuingFuture) that has the
 *     logical flatMap implementation.
 *
 * '''Note:''' this is an abstract class (vs. a trait) to maintain java
 * compatibility.
 */
abstract class Service[-Req, +Rep] extends (Req => Future[Rep]) {
  def map[Req1](f: (Req1) => (Req)) = new Service[Req1, Rep] {
    def apply(req1: Req1) = Service.this.apply(f(req1))
  }

  /**
   * This is the method to override/implement to create your own Service.
   */
  def apply(request: Req): Future[Rep]
}

/**
 * A Filter acts as a decorator/transformer of a service. It may apply
 * transformations to the input and output of that service:
 *
 *           (*  MyService  *)
 * [ReqIn -> (ReqOut -> RepIn) -> RepOut]
 *
 * For example, you may have a POJO service that takes Strings and parses them as Ints.
 * If you want to expose this as a Network Service via Thrift, it is nice to isolate the
 * protocol handling from the business rules. Hence you might have a Filter that converts
 * back and forth between Thrift structs. Again, your service deals with POJOs:
 *
 * [ThriftIn -> (String  ->  Int) -> ThriftOut]
 *
 */
abstract class Filter[-ReqIn, +RepOut, +ReqOut, -RepIn]
  extends ((ReqIn, Service[ReqOut, RepIn]) => Future[RepOut])
{
  /**
   * This is the method to override/implement to create your own Filter.
   *
   * @param  request  the input request type
   * @param  service  a service that takes the output request type and the input response type
   *
   */
  def apply(request: ReqIn, service: Service[ReqOut, RepIn]): Future[RepOut]

  /**
   * Chains a series of filters together:
   *
   *    myModularService = handleExcetions.andThen(thrift2Pojo.andThen(parseString))
   *
   * @param  next  another filter to follow after this one
   *
   */
  def andThen[Req2, Rep2](next: Filter[ReqOut, RepIn, Req2, Rep2]) =
    new Filter[ReqIn, RepOut, Req2, Rep2] {
      def apply(request: ReqIn, service: Service[Req2, Rep2]) = {
        Filter.this.apply(request, new Service[ReqOut, RepIn] {
          def apply(request: ReqOut): Future[RepIn] = next(request, service)
        })
      }
    }

  /**
   * Terminates a filter chain in a service. For example,
   *
   *     myFilter.andThen(myService)
   *
   * @param  service  a service that takes the output request type and the input response type.
   *
   */
  def andThen(service: Service[ReqOut, RepIn]) = new Service[ReqIn, RepOut] {
    def apply(request: ReqIn) = Filter.this.apply(request, service)
  }

  /**
   * Conditionally propagates requests down the filter chain. This may useful if you are statically
   * wiring together filter chains based on a configuration file, for instance.
   *
   * @param  condAndFilter  a tuple of boolean and filter.
   *
   */
  def andThenIf[Req2 >: ReqOut, Rep2 <: RepIn](
    condAndFilter: (Boolean, Filter[ReqOut, RepIn, Req2, Rep2])) =
    condAndFilter match {
      case (true, filter) => andThen(filter)
      case (false, _)     => this
    }
}

abstract class SimpleFilter[Req, Rep] extends Filter[Req, Rep, Req, Rep]
