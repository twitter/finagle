package com.twitter.finagle.exp.swift

import com.facebook.swift.codec.metadata.ReflectionHelper.{
  getAllClassAnnotations, findAnnotatedMethods, 
  extractParameterNames}
import com.facebook.swift.codec.metadata.ThriftType
import com.google.common.collect.Iterables
import java.lang.reflect.Method
import com.twitter.finagle.Service
import scala.collection.JavaConverters._
import com.twitter.util.Future
import com.google.common.reflect.TypeToken

/**
 * Symbols describe the structure of structs and services.
 */

/**
 * A service is a named collection of methods.
 */
case class ServiceSym(name: String, methods: Seq[MethodSym])

object ServiceSym {
  def isService(k: Class[_]): Boolean =
    !getAllClassAnnotations(k, classOf[ThriftService]).isEmpty

  /**
   * Construct a service symbol from a java class using the
   * Swift annotations. ServiceSym will throw an exception
   * if the given class cannot be converted. In addition to
   * the normal Swift requirements -- every type must be 
   * coercible into its thrift equivalent -- all methods must
   * also return [[com.twitter.util.Future]].
   */
  def apply(k: Class[_]): ServiceSym = {
    val annot = {
      val all = getAllClassAnnotations(k, classOf[ThriftService])
      require(!all.isEmpty)
      Iterables.getOnlyElement(all)
    }
    val name = annot.value() match {
      // todo: demunge scala names.
      case "" => k.getSimpleName()
      case n => n
    }

    val methods = for {
      m <- findAnnotatedMethods(k, classOf[ThriftMethod]).asScala.toSeq
    } yield MethodSym(m)

    ServiceSym(name, methods)
  }
}

case class ArgSym(name: String, id: Short, thriftType: ThriftType)

case class MethodSym(
    name: String, method: Method, 
    returnType: ThriftType, args: Seq[ArgSym],
    exceptions: Map[Short, ThriftType])

object MethodSym {
  def apply(m: Method): MethodSym = {
    val annot = m.getAnnotation(classOf[ThriftMethod])
    require(annot != null)

    // todo: check not static
    val name = annot.value()  match {
      case "" => m.getName()
      case n => n
    }
    val returnType = ThriftCatalog.getThriftType(m.getGenericReturnType())
    
    // Require that every method actually returns something, and that
    // this is always available asynchronously. This precludes
    // support for oneway functions, but that's a good thing.
    require(classOf[Future[_]].isAssignableFrom(
        TypeToken.of(m.getGenericReturnType()).getRawType()),
        "methods must return Future values")
    
    val types = m.getGenericParameterTypes()
    val names = extractParameterNames(m)
    val annots = m.getParameterAnnotations()
    val args = for (i <- Seq.range(0, types.length)) yield {
      val annot = annots(i) collectFirst {
        case a: com.facebook.swift.codec.ThriftField => a
      }

      val id = annot map(_.value()) getOrElse (i+1).toShort
      val name = annot flatMap(a => Option(a.name())) getOrElse names(i)
      val thriftType = ThriftCatalog.getThriftType(types(i))
      
      ArgSym(name, id, thriftType)
    }
    
    // todo: We are silently overwriting exception ids here
    // (later wins). Is this OK?
    val exceptions = Map() ++ annot.exception().map { e =>
      e.id() -> ThriftCatalog.getThriftType(e.`type`())
    }

    MethodSym(name, m, returnType, args, exceptions)
  }
}
