package com.twitter.finagle

import com.twitter.util.Future

/**
 *  A Filter acts as a decorator/transformer of a service. It may apply
 * transformations to the input and output of that service:
 *
 *           (*  MyService  *)
 * [ReqIn -> (ReqOut -> RepIn) -> RepOut]
 *
 * For example, you may have a POJO service that takes Strings and
 * parses them as Ints.  If you want to expose this as a Network
 * Service via Thrift, it is nice to isolate the protocol handling
 * from the business rules. Hence you might have a Filter that
 * converts back and forth between Thrift structs. Again, your service
 * deals with POJOs:
 *
 * [ThriftIn -> (String  ->  Int) -> ThriftOut]
 *
 * Thus, a Filter[A, B, C, D] converts a Service[C, D] to a Service[A, B].
 * In other words, it converts a Service[ReqOut, RepIn] to a
 * Service[ReqIn, ReqOut].
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
   *    myModularService = handleExceptions.andThen(thrift2Pojo.andThen(parseString))
   *
   * '''Note:''' synchronously thrown exceptions in the underlying service are automatically
   * lifted into Future.exception.
   *
   * @param  next  another filter to follow after this one
   *
   */
  def andThen[Req2, Rep2](next: Filter[ReqOut, RepIn, Req2, Rep2]) =
    new Filter[ReqIn, RepOut, Req2, Rep2] {
      def apply(request: ReqIn, service: Service[Req2, Rep2]) = {
        Filter.this.apply(request, new Service[ReqOut, RepIn] {
          def apply(request: ReqOut): Future[RepIn] = next(request, service)
          override def release() = service.release()
          override def isAvailable = service.isAvailable
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
    def apply(request: ReqIn) = Filter.this.apply(request, Service.rescue(service))
    override def release() = service.release()
    override def isAvailable = service.isAvailable
  }

  def andThen(factory: ServiceFactory[ReqOut, RepIn]): ServiceFactory[ReqIn, RepOut] =
    new ServiceFactory[ReqIn, RepOut] {
      def make() = factory.make() map { Filter.this andThen _ }
      override def close() = factory.close()
      override def isAvailable = factory.isAvailable
      override def toString = factory.toString
    }

  /**
   * Conditionally propagates requests down the filter chain. This may
   * useful if you are statically wiring together filter chains based
   * on a configuration file, for instance.
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

object Filter {
  def identity[Req, Rep] = new SimpleFilter[Req, Rep] {
    def apply(request: Req, service: Service[Req, Rep]) = service(request)
  }
}
