package com.twitter.finagle.thrift

import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.Service
import com.twitter.finagle.Thrift
import java.lang.reflect.Constructor
import org.apache.thrift.protocol.TProtocolFactory

private[twitter] object ThriftUtil {
  private[finagle] type BinaryService = Service[Array[Byte], Array[Byte]]

  private def findClass1(name: String): Option[Class[_]] =
    try Some(Class.forName(name))
    catch {
      case _: ClassNotFoundException => None
    }

  private[finagle] def findClass[A](name: String): Option[Class[A]] =
    for {
      cls <- findClass1(name)
    } yield cls.asInstanceOf[Class[A]]

  private def findConstructor[A](clz: Class[A], paramTypes: Class[_]*): Option[Constructor[A]] =
    try {
      Some(clz.getConstructor(paramTypes: _*))
    } catch {
      case _: NoSuchMethodException => None
    }

  // This is based on the generated stubs from Scrooge.
  private val ScroogeGeneratedSuffixes = Seq(
    "$Iface",
    "$ServiceIface",
    "$FutureIface",
    "$MethodPerEndpoint",
    "$ServicePerEndpoint",
    // handles ServiceB extends ServiceA, then using ServiceB$MethodIface
    "$MethodIface",
    // handles ServiceB extends ServiceA, then using ServiceB$MethodPerEndpoint$MethodPerEndpointImpl
    "$MethodPerEndpoint$MethodPerEndpointImpl",
    "$ReqRepServicePerEndpoint",
    // handles ServiceB extends ServiceA, then using ServiceB$ReqRepMethodPerEndpoint$ReqRepMethodPerEndpointImpl
    "$ReqRepMethodPerEndpoint$ReqRepMethodPerEndpointImpl"
  )

  /**
   * Strip Scrooge generated suffix from Scala and Java clients.
   * For service classes that are already Scrooge-generated type
   */
  private[finagle] def stripSuffix(iface: Class[_]): String = {
    val ifaceName = iface.getName
    ScroogeGeneratedSuffixes
      .find(s => ifaceName.endsWith(s)).map(s => ifaceName.stripSuffix(s)).getOrElse(iface.getName)
  }

  private[finagle] val FinagledServerSuffixJava = s"$$Service"
  private[finagle] val FinagledServerSuffixScala = "$FinagleService"
  private[finagle] val FinagledClientSuffixJava = "$ServiceToClient"
  private[finagle] val FinagledClientSuffixScala = "$FinagledClient"

  /**
   * Construct an `Iface` based on an underlying [[com.twitter.finagle.Service]]
   * using whichever Thrift code-generation toolchain is available.
   */
  private[finagle] def constructIface[Iface](
    underlying: Service[ThriftClientRequest, Array[Byte]],
    cls: Class[_],
    clientParam: RichClientParam
  ): Iface = {
    // This is used with Scrooge's Java generated code.
    // The class name passed in should be ServiceName$ServiceIface.
    // Will try to create a ServiceName$ServiceToClient instance.
    def tryJavaServiceNameDotServiceIface(iface: Class[_]): Option[Iface] = {
      val baseName: String = stripSuffix(iface)
      for {
        clientCls <- findClass[Iface](baseName + FinagledClientSuffixJava)
        cons <- findConstructor(
          clientCls,
          classOf[Service[_, _]],
          classOf[RichClientParam]
        )
      } yield {
        cons.newInstance(underlying, clientParam)
      }
    }

    // This is used with Scrooge's Scala generated code.
    // The class name passed in should be ServiceName$MethodPerEndpoint
    // or the higher-kinded version, ServiceName.MethodPerEndpoint.
    // Will try to create a ServiceName$FinagledClient instance.
    def tryScalaServiceNameIface(iface: Class[_]): Option[Iface] = {
      val baseName: String = stripSuffix(iface)
      for {
        clientCls <- findClass[Iface](baseName + FinagledClientSuffixScala)
        cons <- findConstructor(
          clientCls,
          classOf[Service[_, _]],
          classOf[RichClientParam]
        )
      } yield cons.newInstance(underlying, clientParam)
    }

    def tryClass(cls: Class[_]): Option[Iface] =
      tryJavaServiceNameDotServiceIface(cls)
        .orElse(tryScalaServiceNameIface(cls))
        .orElse {
          (Option(cls.getSuperclass) ++ cls.getInterfaces).view.flatMap(tryClass).headOption
        }

    tryClass(cls).getOrElse {
      throw new IllegalArgumentException(
        s"Iface $cls is not a valid thrift iface. For Scala generated code, " +
          "try `YourServiceName$FutureIface`(deprecated), " +
          "`YourServiceName$MethodPerEndpoint` or `YourServiceName.MethodPerEndpoint` " +
          "For Java generated code, try `YourServiceName$ServiceIface`."
      )
    }
  }

  /**
   * Construct a binary [[com.twitter.finagle.Service]] for a given Thrift
   * interface using whichever Thrift code-generation toolchain is available.
   */
  def serverFromIface(impl: AnyRef, serverParam: RichServerParam): BinaryService = {
    // This is used with Scrooge's Java generated code.
    // The class passed in should be ServiceName$ServiceIface.
    // Will try to create a ServiceName$Service instance.
    def tryThriftFinagleService(iface: Class[_]): Option[BinaryService] = {
      val baseName: String = stripSuffix(iface)
      for {
        serviceCls <- findClass[BinaryService](baseName + FinagledServerSuffixJava)
        cons <- findConstructor(serviceCls, iface, classOf[RichServerParam])
      } yield {
        cons.newInstance(impl, serverParam)
      }
    }

    // This is used with Scrooge's Scala generated code.
    // The class passed in should be ServiceName$MethodPerEndpoint,
    // or the higher-kinded version, ServiceName.MethodPerEndpoint.
    // Will try to create a ServiceName$FinagleService.
    def tryScroogeFinagleService(iface: Class[_]): Option[BinaryService] = {
      val baseName: String = stripSuffix(iface)
      (for {
        serviceCls <- findClass[BinaryService](baseName + FinagledServerSuffixScala)
        baseClass <- findClass1(baseName + "$MethodPerEndpoint")
      } yield {
        findConstructor(
          serviceCls,
          baseClass,
          classOf[RichServerParam]
        ).map { cons => cons.newInstance(impl, serverParam) }
      }).flatten
    }

    def tryClass(cls: Class[_]): Option[BinaryService] =
      tryThriftFinagleService(cls)
        .orElse(tryScroogeFinagleService(cls))
        .orElse {
          (Option(cls.getSuperclass) ++ cls.getInterfaces).view.flatMap(tryClass).headOption
        }

    tryClass(impl.getClass).getOrElse {
      throw new IllegalArgumentException(
        s"$impl implements no candidate ifaces. For Scala generated code, " +
          "try `YourServiceName$FutureIface`(deprecated), `YourServiceName$MethodIface`(deprecated)" +
          "`YourServiceName$MethodPerEndpoint` or `YourServiceName`. " +
          "For Java generated code, try `YourServiceName$ServiceIface`."
      )
    }
  }

  @deprecated("Use com.twitter.finagle.thrift.RichServerParam", "2017-08-16")
  def serverFromIface(
    impl: AnyRef,
    protocolFactory: TProtocolFactory,
    stats: StatsReceiver = NullStatsReceiver,
    maxThriftBufferSize: Int = Thrift.param.maxThriftBufferSize,
    label: String
  ): BinaryService = {
    serverFromIface(impl, RichServerParam(protocolFactory, label, maxThriftBufferSize, stats))
  }

  /**
   * Construct a multiplexed binary [[com.twitter.finagle.Service]].
   */
  def serverFromIfaces(
    ifaces: Map[String, AnyRef],
    defaultService: Option[String],
    serverParam: RichServerParam
  ): BinaryService = {
    val services = ifaces.map {
      case (serviceName, impl) =>
        serviceName -> serverFromIface(impl, serverParam)
    }
    new MultiplexedFinagleService(
      services,
      defaultService,
      serverParam.protocolFactory,
      serverParam.maxThriftBufferSize
    )
  }

  @deprecated("Use com.twitter.finagle.thrift.RichServerParam", "2017-08-16")
  def serverFromIfaces(
    ifaces: Map[String, AnyRef],
    defaultService: Option[String],
    protocolFactory: TProtocolFactory,
    stats: StatsReceiver,
    maxThriftBufferSize: Int,
    label: String
  ): BinaryService = {
    serverFromIfaces(
      ifaces,
      defaultService,
      RichServerParam(
        protocolFactory,
        label,
        maxThriftBufferSize,
        stats
      )
    )
  }

  /**
   * Construct a binary [[com.twitter.finagle.Service]] for a given Thrift
   * interface using whichever Thrift code-generation toolchain is available.
   * (Legacy version for backward-compatibility).
   */
  @deprecated("Use com.twitter.finagle.thrift.RichServerParam", "2017-08-16")
  def serverFromIface(
    impl: AnyRef,
    protocolFactory: TProtocolFactory,
    serviceName: String
  ): BinaryService = {
    serverFromIface(impl, RichServerParam(protocolFactory, serviceName))
  }
}
