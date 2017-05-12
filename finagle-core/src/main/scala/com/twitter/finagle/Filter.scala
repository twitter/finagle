package com.twitter.finagle

import com.twitter.util.{Future, Time}

/**
 * A [[Filter]] acts as a decorator/transformer of a [[Service service]].
 * It may apply transformations to the input and output of that service:
 * {{{
 *           (*  MyService  *)
 * [ReqIn -> (ReqOut -> RepIn) -> RepOut]
 * }}}
 * For example, you may have a service that takes `Strings` and
 * parses them as `Ints`.  If you want to expose this as a Network
 * Service via Thrift, it is nice to isolate the protocol handling
 * from the business rules. Hence you might have a Filter that
 * converts back and forth between Thrift structs. Again, your service
 * deals with plain objects:
 * {{{
 * [ThriftIn -> (String  ->  Int) -> ThriftOut]
 * }}}
 *
 * Thus, a `Filter[A, B, C, D]` converts a `Service[C, D]` to a `Service[A, B]`.
 * In other words, it converts a `Service[ReqOut, RepIn]` to a
 * `Service[ReqIn, RepOut]`.
 *
 * @see The [[https://twitter.github.io/finagle/guide/ServicesAndFilters.html#filters user guide]]
 *      for details and examples.
 */
abstract class Filter[-ReqIn, +RepOut, +ReqOut, -RepIn]
  extends ((ReqIn, Service[ReqOut, RepIn]) => Future[RepOut])
{
  import Filter.AndThen

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
   * @note synchronously thrown exceptions in the underlying service are automatically
   * lifted into Future.exception.
   *
   * @param next another filter to follow after this one
   */
  def andThen[Req2, Rep2](next: Filter[ReqOut, RepIn, Req2, Rep2]): Filter[ReqIn, RepOut, Req2, Rep2] =
    if (next eq Filter.identity) this.asInstanceOf[Filter[ReqIn, RepOut, Req2, Rep2]]
    // Rewrites Filter composition via `andThen` with AndThen's composition
    // which is just function composition.
    else AndThen(service => andThen(next.andThen(service)))

  /**
   * Convert the [[Filter.TypeAgnostic]] filter to a Filter and chain it with
   * `andThen`.
   */
  def agnosticAndThen(next: Filter.TypeAgnostic): Filter[ReqIn, RepOut, ReqOut, RepIn] =
    andThen(next.toFilter[ReqOut, RepIn])

  /**
   * Terminates a filter chain in a [[Service]]. For example,
   *
   * {{{
   *   myFilter.andThen(myService)
   * }}}
   * @param service a service that takes the output request type and the input response type.
   */
  def andThen(service: Service[ReqOut, RepIn]): Service[ReqIn, RepOut] = {
    val svc = Service.rescue(service)
    new Service[ReqIn, RepOut] {
      def apply(request: ReqIn): Future[RepOut] = Filter.this.apply(request, svc)
      override def close(deadline: Time): Future[Unit] = service.close(deadline)
      override def status: Status = service.status
    }
  }

  /**
   * Terminates a filter chain in a [[ServiceFactory]]. For example,
   *
   * {{{
   *   myFilter.andThen(myServiceFactory)
   * }}}
   * @param factory a service factory that takes the output request type and
   *                the input response type.
   */
  def andThen(factory: ServiceFactory[ReqOut, RepIn]): ServiceFactory[ReqIn, RepOut] =
    new ServiceFactory[ReqIn, RepOut] {
      val fn: Service[ReqOut, RepIn] => Service[ReqIn, RepOut] =
        svc => Filter.this.andThen(svc)
      def apply(conn: ClientConnection): Future[Service[ReqIn, RepOut]] =
        factory(conn).map(fn)
      def close(deadline: Time): Future[Unit] = factory.close(deadline)
      override def status: Status = factory.status
      override def toString: String = factory.toString()
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
  // `AndThen` is a function that represents the prefix of the filter chain to
  // transform a terminal Service received as an argument.
  private case class AndThen[ReqIn, RepOut, ReqOut, RepIn](
      build: Service[ReqOut, RepIn] => Service[ReqIn, RepOut])
    extends Filter[ReqIn, RepOut, ReqOut, RepIn]
  {
    override def andThen[Req2, Rep2](
      next: Filter[ReqOut, RepIn, Req2, Rep2]
    ): Filter[ReqIn, RepOut, Req2, Rep2] =
      if (next eq Filter.identity) this.asInstanceOf[Filter[ReqIn, RepOut, Req2, Rep2]]
      else AndThen(service => build(next.andThen(service)))

    override def andThen(service: Service[ReqOut, RepIn]): Service[ReqIn, RepOut] =
      build(service)

    override def andThen(
      factory: ServiceFactory[ReqOut, RepIn]
    ): ServiceFactory[ReqIn, RepOut] =
      new ServiceFactory[ReqIn, RepOut] {
        val fn: Service[ReqOut, RepIn] => Service[ReqIn, RepOut] =
          svc => AndThen.this.andThen(svc)
        def apply(conn: ClientConnection): Future[Service[ReqIn, RepOut]] =
          factory(conn).map(fn)
        def close(deadline: Time): Future[Unit] = factory.close(deadline)
        override def status: Status = factory.status
        override def toString: String = factory.toString()
      }

    def apply(request: ReqIn, service: Service[ReqOut, RepIn]): Future[RepOut] =
      build(service)(request)
  }

  private case object Identity extends SimpleFilter[Any, Nothing] {
    override def andThen[Req2, Rep2](
      next: Filter[Any, Nothing, Req2, Rep2]): Filter[Any, Nothing, Req2, Rep2] = next

    override def andThen(service: Service[Any, Nothing]): Service[Any, Nothing] = service

    override def andThen(factory: ServiceFactory[Any, Nothing]): ServiceFactory[Any, Nothing] = factory

    def apply(request: Any, service: Service[Any, Nothing]): Future[Nothing] = service(request)
  }

  implicit def canStackFromSvc[Req, Rep]
    : CanStackFrom[Filter[Req, Rep, Req, Rep], Service[Req, Rep]] =
    new CanStackFrom[Filter[Req, Rep, Req, Rep], Service[Req, Rep]] {
      def toStackable(
        _role: Stack.Role,
        filter: Filter[Req, Rep, Req, Rep]
      ): Stack.Module0[Service[Req, Rep]] =
        new Stack.Module0[Service[Req, Rep]] {
          val role: Stack.Role = _role
          val description: String = role.name
          def make(next: Service[Req, Rep]): Service[Req, Rep] =
            filter.andThen(next)
        }
    }

  implicit def canStackFromFac[Req, Rep]
    : CanStackFrom[Filter[Req, Rep, Req, Rep], ServiceFactory[Req, Rep]] =
    new CanStackFrom[Filter[Req, Rep, Req, Rep], ServiceFactory[Req, Rep]] {
      def toStackable(
        _role: Stack.Role,
        filter: Filter[Req, Rep, Req, Rep]
      ): Stack.Module0[ServiceFactory[Req, Rep]] =
        new Stack.Module0[ServiceFactory[Req, Rep]] {
          val role: Stack.Role = _role
          val description: String = role.name
          def make(next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] =
            filter.andThen(next)
        }
    }

  /**
   * TypeAgnostic filters are like SimpleFilters but they leave the Rep and Req types unspecified
   * until `toFilter` is called.
   */
  trait TypeAgnostic { self =>
    def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep]

    def andThen(next: TypeAgnostic): TypeAgnostic =
      new TypeAgnostic {
        def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] =
          self.toFilter[Req, Rep].andThen(next.toFilter[Req, Rep])
      }

    /**
     * Convert this to an appropriately-typed [[Filter]] and compose
     * with `andThen`.
     */
    def andThen[ReqIn, RepOut, ReqOut, RepIn](
      next: Filter[ReqIn, RepOut, ReqOut, RepIn]
    ): Filter[ReqIn, RepOut, ReqOut, RepIn] =
      toFilter[ReqIn, RepOut].andThen(next)

    /**
     * Convert this to an appropriately-typed [[Filter]] and compose
     * with `andThen`.
     */
    def andThen[Req, Rep](svc: Service[Req, Rep]): Service[Req, Rep] =
      toFilter[Req, Rep].andThen(svc)

    /**
     * Convert this to an appropriately-typed [[Filter]] and compose
     * with `andThen`.
     */
    def andThen[Req, Rep](factory: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] =
      toFilter[Req, Rep].andThen(factory)
  }

  object TypeAgnostic {
    val Identity: TypeAgnostic =
      new TypeAgnostic {
        def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] = identity[Req, Rep]
      }
  }

  def identity[Req, Rep]: SimpleFilter[Req, Rep] =
    Identity.asInstanceOf[SimpleFilter[Req, Rep]]

  def mk[ReqIn, RepOut, ReqOut, RepIn](
    f: (ReqIn, ReqOut => Future[RepIn]) => Future[RepOut]
  ): Filter[ReqIn, RepOut, ReqOut, RepIn] = new Filter[ReqIn, RepOut, ReqOut, RepIn] {
    def apply(request: ReqIn, service: Service[ReqOut, RepIn]): Future[RepOut] =
      f(request, service)
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
