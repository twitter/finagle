package com.twitter.finagle

import com.twitter.util.{NonFatal, Future, Time}

/**
 * A [[Filter]] acts as a decorator/transformer of a [[Service service]].
 * It may apply transformations to the input and output of that service:
 * {{{
 *           (*  MyService  *)
 * [ReqIn -> (ReqOut -> RepIn) -> RepOut]
 * }}}
 * For example, you may have a POJO service that takes Strings and
 * parses them as Ints.  If you want to expose this as a Network
 * Service via Thrift, it is nice to isolate the protocol handling
 * from the business rules. Hence you might have a Filter that
 * converts back and forth between Thrift structs. Again, your service
 * deals with POJOs:
 * {{{
 * [ThriftIn -> (String  ->  Int) -> ThriftOut]
 * }}}
 *
 * Thus, a `Filter[A, B, C, D]` converts a `Service[C, D]` to a `Service[A, B]`.
 * In other words, it converts a `Service[ReqOut, RepIn]` to a
 * `Service[ReqIn, RepOut]`.
 *
 */
abstract class Filter[-ReqIn, +RepOut, +ReqOut, -RepIn]
  extends ((ReqIn, Service[ReqOut, RepIn]) => Future[RepOut])
{
  /**
   * This is the method to override/implement to create your own Filter.
   *
   * @param request the input request type
   * @param service a service that takes the output request type and the input response type
   *
   */
  def apply(request: ReqIn, service: Service[ReqOut, RepIn]): Future[RepOut]

  /**
   * Chains a series of filters together:
   *
   * {{{
   * myModularService = handleExceptions.andThen(thrift2Pojo.andThen(parseString))
   * }}}
   *
   * '''Note:''' synchronously thrown exceptions in the underlying service are automatically
   * lifted into Future.exception.
   *
   * @param next another filter to follow after this one
   */
  def andThen[Req2, Rep2](next: Filter[ReqOut, RepIn, Req2, Rep2]) =
    new Filter[ReqIn, RepOut, Req2, Rep2] {
      def apply(request: ReqIn, service: Service[Req2, Rep2]) = {
        val svc: Service[ReqOut, RepIn] = new Service[ReqOut, RepIn] with Proxy {
          // note that while `Service.rescue` could be used here it would
          // entail an extra allocation.
          def apply(request: ReqOut): Future[RepIn] = {
            try {
              next(request, service)
            } catch {
              case NonFatal(e) => Future.exception(e)
            }
          }
          def self = service
          override def close(deadline: Time) = service.close(deadline)
          override def status = service.status
          override def toString() = service.toString()
        }

        Filter.this.apply(request, svc)
      }
    }

  /**
   * Terminates a filter chain in a service. For example,
   *
   * {{{
   *   myFilter.andThen(myService)
   * }}}
   * @param service a service that takes the output request type and the input response type.
   */
  def andThen(service: Service[ReqOut, RepIn]): Service[ReqIn, RepOut] = {
    val svc = Service.rescue(service)
    new Service[ReqIn, RepOut] {
      def apply(request: ReqIn) = Filter.this.apply(request, svc)
      override def close(deadline: Time) = service.close(deadline)
      override def status = service.status
    }
  }

  def andThen(f: ReqOut => Future[RepIn]): ReqIn => Future[RepOut] = {
    val service = Service.mk(f)
    req => Filter.this.apply(req, service)
  }

  def andThen(factory: ServiceFactory[ReqOut, RepIn]): ServiceFactory[ReqIn, RepOut] =
    new ServiceFactory[ReqIn, RepOut] {
      val fn: Service[ReqOut, RepIn] => Service[ReqIn, RepOut] =
        svc => Filter.this.andThen(svc)
      def apply(conn: ClientConnection): Future[Service[ReqIn, RepOut]] =
        factory(conn).map(fn)
      def close(deadline: Time) = factory.close(deadline)
      override def status = factory.status
      override def toString() = factory.toString()
    }

  /**
   * Conditionally propagates requests down the filter chain. This may
   * useful if you are statically wiring together filter chains based
   * on a configuration file, for instance.
   *
   * @param condAndFilter a tuple of boolean and filter.
   */
  def andThenIf[Req2 >: ReqOut, Rep2 <: RepIn](
    condAndFilter: (Boolean, Filter[ReqOut, RepIn, Req2, Rep2])
  ): Filter[ReqIn, RepOut, Req2, Rep2] =
    condAndFilter match {
      case (true, filter) => andThen(filter)
      case (false, _)     => this
    }
}

/**
 * A [[Filter]] where the request and reply types are the same.
 */
abstract class SimpleFilter[Req, Rep] extends Filter[Req, Rep, Req, Rep]

object Filter {
  implicit def canStackFromSvc[Req, Rep]
    : CanStackFrom[Filter[Req, Rep, Req, Rep], Service[Req, Rep]] =
    new CanStackFrom[Filter[Req, Rep, Req, Rep], Service[Req, Rep]] {
      def toStackable(_role: Stack.Role, filter: Filter[Req, Rep, Req, Rep]) =
        new Stack.Module0[Service[Req, Rep]] {
          val role = _role
          val description = role.name
          def make(next: Service[Req, Rep]) = filter andThen next
        }
    }

  implicit def canStackFromFac[Req, Rep]
    : CanStackFrom[Filter[Req, Rep, Req, Rep], ServiceFactory[Req, Rep]] =
    new CanStackFrom[Filter[Req, Rep, Req, Rep], ServiceFactory[Req, Rep]] {
      def toStackable(_role: Stack.Role, filter: Filter[Req, Rep, Req, Rep]) =
        new Stack.Module0[ServiceFactory[Req, Rep]] {
          val role = _role
          val description = role.name
          def make(next: ServiceFactory[Req, Rep]) = filter andThen next
        }
    }

  /**
   * TypeAgnostic filters are like SimpleFilters but they leave the Rep and Req types unspecified
   * until `toFilter` is called.
   */
  trait TypeAgnostic {
    def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep]

    def andThen(next: TypeAgnostic): TypeAgnostic = new TypeAgnostic {
      def toFilter[Req, Rep] = toFilter[Req, Rep].andThen(next.toFilter[Req, Rep])
    }
  }

  def identity[Req, Rep] = new SimpleFilter[Req, Rep] {
    override def andThen[Req2, Rep2](next: Filter[Req, Rep, Req2, Rep2]) = next
    override def andThen(service: Service[Req, Rep]) = service
    override def andThen(factory: ServiceFactory[Req, Rep]) = factory

    def apply(request: Req, service: Service[Req, Rep]) = service(request)
  }

  def mk[ReqIn, RepOut, ReqOut, RepIn](
    f: (ReqIn, ReqOut => Future[RepIn]) => Future[RepOut]
  ): Filter[ReqIn, RepOut, ReqOut, RepIn] = new Filter[ReqIn, RepOut, ReqOut, RepIn] {
    def apply(request: ReqIn, service: Service[ReqOut, RepIn]) = f(request, service)
  }

  /**
   * Chooses a filter to apply based on incoming requests. If the given partial
   * function is not defined at the request, then the request goes directly to
   * the next service.
   *
   * @param pf a partial function mapping requests to Filters that should
   *           be applied
   */
  def choose[Req, Rep](
    pf: PartialFunction[Req, Filter[Req, Rep, Req, Rep]]
  ): Filter[Req, Rep, Req, Rep] = new Filter[Req, Rep, Req, Rep] {
    private[this] val const: (Req => SimpleFilter[Req, Rep]) =
      Function.const(Filter.identity[Req, Rep])

    def apply(request: Req, service: Service[Req, Rep]): Future[Rep] =
      pf.applyOrElse(request, const)(request, service)
  }
}
