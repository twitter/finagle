package com.twitter.finagle

import com.twitter.util.Future
import com.twitter.util.Time

import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal

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
    extends ((ReqIn, Service[ReqOut, RepIn]) => Future[RepOut]) {
  import Filter.AndThen
  import Filter.Adapter
  import Filter.SafeService

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
  def andThen[Req2, Rep2](
    next: Filter[ReqOut, RepIn, Req2, Rep2]
  ): Filter[ReqIn, RepOut, Req2, Rep2] =
    if (next eq Filter.identity) {
      this.asInstanceOf[Filter[ReqIn, RepOut, Req2, Rep2]]
    } else {
      // Rewrites Filter composition via `andThen` with AndThen's composition
      // which is just function composition.
      AndThen(this, next, service => andThenService(next.andThenService(service)))
    }

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
    // wrap user-supplied Services such that NonFatal exceptions thrown synchronously
    // in their `Service.apply` method are lifted into a `Future.exception`.
    val rescued = service match {
      case _: SafeService[_, _] =>
        service
      case _ =>
        Service.rescue(service)
    }
    andThenService(rescued)
  }

  private[twitter] def andThenService(next: Service[ReqOut, RepIn]): Service[ReqIn, RepOut] = {
    val filter: Filter[ReqIn, RepOut, ReqOut, RepIn] = Filter.this
    new Adapter(filter, next)
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
      private val fn: Service[ReqOut, RepIn] => Service[ReqIn, RepOut] =
        svc => Filter.this.andThen(svc)
      def apply(conn: ClientConnection): Future[Service[ReqIn, RepOut]] =
        factory(conn).map(fn)
      def close(deadline: Time): Future[Unit] = factory.close(deadline)
      override def status: Status = factory.status
      override def toString: String = {
        s"${Filter.this.toString}.andThen(${factory.toString})"
      }
    }

  /**
   * Conditionally propagates requests down the filter chain. This may
   * be useful if you are statically wiring together filter chains based
   * on a configuration file, for instance.
   *
   * @param conditional a boolean value indicating whether the filter should be
   *                    included in the filter chain.
   * @param filter the filter to be conditionally included.
   */
  def andThenIf[Req2 >: ReqOut, Rep2 <: RepIn](
    conditional: Boolean,
    filter: Filter[ReqOut, RepIn, Req2, Rep2]
  ): Filter[ReqIn, RepOut, Req2, Rep2] =
    if (conditional) andThen(filter)
    else this

  /**
   * Conditionally propagates requests down the filter chain. This may
   * be useful if you are statically wiring together filter chains based
   * on a configuration file, for instance.
   *
   * @param condAndFilter a tuple of boolean and filter.
   */
  def andThenIf[Req2 >: ReqOut, Rep2 <: RepIn](
    condAndFilter: (Boolean, Filter[ReqOut, RepIn, Req2, Rep2])
  ): Filter[ReqIn, RepOut, Req2, Rep2] =
    condAndFilter match {
      case (true, filter) => andThen(filter)
      case (false, _) => this
    }

  override def toString: String =
    this.getClass.getName
}

/**
 * A [[Filter]] where the request and reply types are the same.
 */
abstract class SimpleFilter[Req, Rep] extends Filter[Req, Rep, Req, Rep]

object Filter {
  private def unroll(filter: Filter[_, _, _, _]): Seq[String] = filter match {
    case AndThen(left, right, _) => unroll(left) ++ unroll(right)
    case _ => Seq(filter.toString)
  }

  // `AndThen` is a function that represents the prefix of the filter chain to
  // transform a terminal Service received as an argument.
  private case class AndThen[ReqIn, RepOut, ReqOut, RepIn](
    first: Filter[_, _, _, _],
    andNext: Filter[_, _, _, _],
    build: Service[ReqOut, RepIn] => Service[ReqIn, RepOut])
      extends Filter[ReqIn, RepOut, ReqOut, RepIn] {

    override def andThen[Req2, Rep2](
      next: Filter[ReqOut, RepIn, Req2, Rep2]
    ): Filter[ReqIn, RepOut, Req2, Rep2] = {
      if (next eq Filter.identity) {
        this.asInstanceOf[Filter[ReqIn, RepOut, Req2, Rep2]]
      } else {
        AndThen(this, next, service => build(next.andThenService(service)))
      }
    }

    override def andThen(underlying: Service[ReqOut, RepIn]): Service[ReqIn, RepOut] = {
      val rescued = Service.rescue(underlying)
      val svc: Service[ReqIn, RepOut] = build(rescued)
      new ServiceProxy[ReqIn, RepOut](svc) {
        override def toString: String =
          s"${AndThen.this.toString}.andThen(${underlying.toString})"
      }
    }

    override def andThen(factory: ServiceFactory[ReqOut, RepIn]): ServiceFactory[ReqIn, RepOut] =
      new ServiceFactory[ReqIn, RepOut] {
        private val fn: Service[ReqOut, RepIn] => Service[ReqIn, RepOut] =
          svc => AndThen.this.andThen(svc)
        def apply(conn: ClientConnection): Future[Service[ReqIn, RepOut]] =
          factory(conn).map(fn)
        def close(deadline: Time): Future[Unit] = factory.close(deadline)
        override def status: Status = factory.status
        override def toString: String =
          s"${AndThen.this.toString}.andThen(${factory.toString})"
      }

    def apply(request: ReqIn, service: Service[ReqOut, RepIn]): Future[RepOut] = {
      val rescued = Service.rescue(service)
      val svc = build(rescued)
      svc(request)
    }

    override def toString: String = {
      val unrolled: Seq[String] = unroll(this)
      val unrolledTail: Seq[String] = unrolled.tail
      s"${unrolled.head}${if (unrolledTail.nonEmpty) unrolledTail.mkString(".andThen(", ").andThen(", ")")
      else ""}"
    }
  }

  /**
   * A marker interface indicating that it's safe to call .apply without wrapping with .rescue
   * @tparam Req
   * @tparam Rep
   */
  private[twitter] trait SafeService[Req, Rep] extends Service[Req, Rep] {}

  private class Adapter[ReqIn, RepOut, ReqOut, RepIn](
    filter: Filter[ReqIn, RepOut, ReqOut, RepIn],
    next: Service[ReqOut, RepIn])
      extends SafeService[ReqIn, RepOut] {
    def apply(request: ReqIn): Future[RepOut] = {
      // wrap the user-supplied `Filter.apply`, lifting synchronous exceptions into Futures.
      try filter.apply(request, next)
      catch {
        case NonFatal(e) => Future.exception(e)
      }
    }

    override def close(deadline: Time): Future[Unit] = next.close(deadline)

    override def status: Status = next.status

    override def toString: String = {
      s"${filter.toString}.andThen(${next.toString})"
    }
  }

  private case object Identity extends SimpleFilter[Any, Nothing] {
    override def andThen[Req2, Rep2](
      next: Filter[Any, Nothing, Req2, Rep2]
    ): Filter[Any, Nothing, Req2, Rep2] = next

    override def andThen(service: Service[Any, Nothing]): Service[Any, Nothing] = service

    override def andThen(factory: ServiceFactory[Any, Nothing]): ServiceFactory[Any, Nothing] =
      factory

    def apply(request: Any, service: Service[Any, Nothing]): Future[Nothing] = service(request)
  }

  implicit def canStackFromSvc[
    Req,
    Rep
  ]: CanStackFrom[Filter[Req, Rep, Req, Rep], Service[Req, Rep]] =
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

  implicit def canStackFromFac[
    Req,
    Rep
  ]: CanStackFrom[Filter[Req, Rep, Req, Rep], ServiceFactory[Req, Rep]] =
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

  implicit def typeAgnosticService[Req, Rep]: CanStackFrom[Filter.TypeAgnostic, Service[Req, Rep]] =
    new CanStackFrom[Filter.TypeAgnostic, Service[Req, Rep]] {
      def toStackable(
        _role: Stack.Role,
        filter: Filter.TypeAgnostic
      ): Stackable[Service[Req, Rep]] = {
        new Stack.Module0[Service[Req, Rep]] {
          def role: Stack.Role = _role
          def description: String = _role.name
          def make(next: Service[Req, Rep]): Service[Req, Rep] =
            filter.andThen(next)
        }
      }
    }

  implicit def typeAgnosticServiceFactory[
    Req,
    Rep
  ]: CanStackFrom[Filter.TypeAgnostic, ServiceFactory[Req, Rep]] =
    new CanStackFrom[Filter.TypeAgnostic, ServiceFactory[Req, Rep]] {
      def toStackable(
        _role: Stack.Role,
        filter: Filter.TypeAgnostic
      ): Stackable[ServiceFactory[Req, Rep]] = {
        new Stack.Module0[ServiceFactory[Req, Rep]] {
          def role: Stack.Role = _role
          def description: String = _role.name
          def make(next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] =
            filter.andThen(next)
        }
      }
    }

  /**
   * TypeAgnostic filters are like SimpleFilters but they leave the Rep and Req types unspecified
   * until `toFilter` is called.
   */
  abstract class TypeAgnostic { self =>
    def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep]

    def andThen(next: TypeAgnostic): TypeAgnostic =
      if (next eq Filter.TypeAgnostic.Identity) {
        this
      } else {
        new TypeAgnostic {
          def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] =
            self.toFilter[Req, Rep].andThen(next.toFilter[Req, Rep])
          override def toString: String =
            s"${self.toString}.andThen(${next.toString})"
        }
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

    override def toString: String = this.getClass.getName
  }

  /**
   * OneTime is a TypeAgnostic filter that can be materialized exactly once.
   * This provides a simple way to create the most commonly used kind of
   * TypeAgnostic filters while ensuring that the apply method is not shared.
   */
  abstract class OneTime extends TypeAgnostic { self =>
    private[this] val toFilterCalled = new AtomicBoolean(false)

    def apply[Req, Rep](req: Req, svc: Service[Req, Rep]): Future[Rep]

    def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] = {
      if (toFilterCalled.compareAndSet(false, true)) {
        new Filter[Req, Rep, Req, Rep] {
          def apply(req: Req, svc: Service[Req, Rep]): Future[Rep] = self.apply(req, svc)
          override def toString: String = self.toString
        }
      } else {
        throw new IllegalStateException(
          "Each instance of a OneTime filter can only have toFilter called once"
        )
      }
    }
  }

  object TypeAgnostic {

    /**
     * A pass-through [[TypeAgnostic]] that in turn uses a
     * [[Filter.Identity]] for its implementation.
     *
     * @see [[Filter.typeAgnosticIdentity]] for Java compatibility.
     */
    val Identity: TypeAgnostic =
      new TypeAgnostic {
        def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] = Filter.identity[Req, Rep]

        override def andThen(next: TypeAgnostic): TypeAgnostic = next

        override def andThen[ReqIn, RepOut, ReqOut, RepIn](
          next: Filter[ReqIn, RepOut, ReqOut, RepIn]
        ): Filter[ReqIn, RepOut, ReqOut, RepIn] = next

        override def andThen[Req, Rep](svc: Service[Req, Rep]): Service[Req, Rep] = svc

        override def andThen[Req, Rep](
          factory: ServiceFactory[Req, Rep]
        ): ServiceFactory[Req, Rep] = factory

        override def toString: String = s"${TypeAgnostic.getClass.getName}Identity"
      }
  }

  /**
   * Returns a [[Filter]] that does nothing beyond passing inputs through
   * to the [[Service]].
   */
  def identity[Req, Rep]: SimpleFilter[Req, Rep] =
    Identity.asInstanceOf[SimpleFilter[Req, Rep]]

  /**
   * A pass-through [[TypeAgnostic]] that in turn uses a
   * [[Filter.Identity]] for its implementation.
   */
  def typeAgnosticIdentity: TypeAgnostic =
    TypeAgnostic.Identity

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
    private[this] val const: Req => SimpleFilter[Req, Rep] =
      Function.const(Filter.identity[Req, Rep])

    def apply(request: Req, service: Service[Req, Rep]): Future[Rep] =
      pf.applyOrElse(request, const)(request, service)
  }
}
