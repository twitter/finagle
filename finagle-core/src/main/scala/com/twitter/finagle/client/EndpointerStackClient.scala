package com.twitter.finagle.client

import com.twitter.finagle._
import com.twitter.finagle.client.EndpointerStackClient.DimensionalClientScopes
import com.twitter.finagle.filter.RequestLogger
import com.twitter.finagle.naming.BindingFactory
import com.twitter.finagle.param._
import com.twitter.finagle.stack.nilStack
import com.twitter.finagle.stats.RelativeNameMarkingStatsReceiver
import com.twitter.finagle.stats.RoleConfiguredStatsReceiver
import com.twitter.finagle.stats.RootFinagleStatsReceiver
import com.twitter.finagle.stats.SourceRole
import com.twitter.finagle.util.Showable

/**
 * The standard template implementation for
 * [[com.twitter.finagle.client.StackClient]].
 *
 * @see The [[https://twitter.github.io/finagle/guide/Clients.html user guide]]
 *      for further details on Finagle clients and their configuration.
 * @see [[StackClient.newStack]] for the default modules used by Finagle
 *      clients.
 */
trait EndpointerStackClient[Req, Rep, This <: EndpointerStackClient[Req, Rep, This]]
    extends StackClient[Req, Rep]
    with Stack.Parameterized[This]
    with CommonParams[This]
    with ClientParams[This]
    with WithClientAdmissionControl[This]
    with WithClientTransport[This]
    with WithClientSession[This]
    with WithSessionQualifier[This] {

  /**
   * Defines the service factory, which establishes connections to a remote
   * peer on apply and returns a service which can write messages onto
   * the wire and read them off of the wire.
   *
   * Concrete StackClient implementations are expected to specify this.
   */
  protected def endpointer: Stackable[ServiceFactory[Req, Rep]]

  def withStack(stack: Stack[ServiceFactory[Req, Rep]]): This =
    copy1(stack = stack)

  override def withStack(
    fn: Stack[ServiceFactory[Req, Rep]] => Stack[ServiceFactory[Req, Rep]]
  ): This =
    withStack(fn(stack))

  /**
   * Creates a new StackClient with `f` applied to `stack`.
   *
   * This is the same as [[withStack]].
   */
  @deprecated(
    "Use withStack(Stack[ServiceFactory[Req, Rep]] => Stack[ServiceFactory[Req, Rep]]) instead",
    "2018-10-30"
  )
  def transformed(f: Stack[ServiceFactory[Req, Rep]] => Stack[ServiceFactory[Req, Rep]]): This =
    withStack(f)

  /**
   * Creates a new StackClient with parameter `p`.
   */
  override def configured[P: Stack.Param](p: P): This =
    withParams(params + p)

  /**
   * Creates a new StackClient with parameter `psp._1` and Stack Param type `psp._2`.
   */
  override def configured[P](psp: (P, Stack.Param[P])): This = {
    val (p, sp) = psp
    configured(p)(sp)
  }

  /**
   * Creates a new StackClient with additional parameters `newParams`.
   */
  override def configuredParams(newParams: Stack.Params): This = {
    withParams(params ++ newParams)
  }

  /**
   * Creates a new StackClient with `params` used to configure this StackClient's `stack`.
   */
  def withParams(params: Stack.Params): This =
    copy1(params = params)

  /**
   * Prepends `filter` to the top of the client. That is, after materializing
   * the client (newClient/newService) `filter` will be the first element which
   * requests flow through. This is a familiar chaining combinator for filters and
   * is particularly useful for `StdStackClient` implementations that don't expose
   * services but instead wrap the resulting service with a rich API.
   */
  def filtered(filter: Filter[Req, Rep, Req, Rep]): This = {
    val role = Stack.Role(filter.getClass.getSimpleName)
    val stackable = Filter.canStackFromFac.toStackable(role, filter)
    withStack(stackable +: stack)
  }

  /**
   * A copy constructor in lieu of defining StackClient as a
   * case class.
   */
  protected def copy1(
    stack: Stack[ServiceFactory[Req, Rep]] = this.stack,
    params: Stack.Params = this.params
  ): This

  protected def injectors: Seq[ClientParamsInjector] = StackClient.DefaultInjectors.injectors

  protected def transformers: Seq[ClientStackTransformer] =
    StackClient.DefaultTransformer.transformers

  /**
   * @inheritdoc
   *
   * @param label0 if an empty String is provided, then the label
   *               from the [[Label]] [[Stack.Params]] is used.
   *               If that is also an empty String, then `dest` is used.
   */
  def newClient(dest: Name, label0: String): ServiceFactory[Req, Rep] = {
    val stats = params[Stats].statsReceiver
    val label1 = params[Label].label

    // For historical reasons, we have two sources for identifying
    // a client. The most recently set `label0` takes precedence.
    val clientLabel = (label0, label1) match {
      case (Label.Default, Label.Default) => Showable.show(dest)
      case (Label.Default, l1) => l1
      case _ => label0
    }

    val originalStack = {
      val baseStack = stack ++ (endpointer +: nilStack)
      params[RequestLogger.Param] match {
        case RequestLogger.Param.Enabled =>
          val transformer = RequestLogger.newStackTransformer(clientLabel)
          transformer(baseStack)
        case RequestLogger.Param.Disabled =>
          baseStack
      }
    }

    val transformedStack =
      transformers.foldLeft(originalStack)((clnt, transformer) => transformer(clnt))

    val clientSr = RoleConfiguredStatsReceiver(
      RelativeNameMarkingStatsReceiver(
        new RootFinagleStatsReceiver(stats, clientLabel, DimensionalClientScopes)),
      SourceRole.Client,
      Some(clientLabel))

    val clientParams = injectors.foldLeft(
      params +
        Label(clientLabel) +
        Stats(clientSr) +
        BindingFactory.Dest(dest)) { case (prms, injector) => injector(prms) }

    transformedStack.make(clientParams)
  }

  def newService(dest: Name, label: String): Service[Req, Rep] = {
    val client = copy1(
      params = params + FactoryToService.Enabled(true)
    ).newClient(dest, label)
    new FactoryToService[Req, Rep](client)
  }
}

private object EndpointerStackClient {
  private val DimensionalClientScopes: Seq[String] = Seq("rpc", "finagle", "client")
}
