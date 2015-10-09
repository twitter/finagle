package com.twitter.finagle.exp.swift

import com.facebook.swift.codec.internal.compiler.CompilerThriftCodecFactory
import com.facebook.swift.codec.metadata.{ThriftCatalog => FBThriftCatalog}
import com.facebook.swift.codec.{ThriftCodec, ThriftCodecManager => FBThriftCodecManager}
import com.google.common.reflect.TypeToken
import com.twitter.util.Future
import java.lang.reflect.Type
import java.util.Collections
import scala.language.existentials

/**
 * A Swift ThriftCatalog modified to also
 * handle [[com.twitter.util.Future]].
*/
private object ThriftCatalog extends FBThriftCatalog {
  private lazy val FutureClass = classOf[Future[_]]
  private lazy val FutureGetType = FutureClass.getMethod("get").getGenericReturnType()

  override def getThriftType(javaType: Type) = {
    val rawType = TypeToken.of(javaType).getRawType()
    if (FutureClass.isAssignableFrom(rawType)) {
      val retType = TypeToken.of(javaType).resolveType(FutureGetType).getType()
      getThriftType(retType)
    } else {
      super.getThriftType(javaType)
    }
  }
}

private object ThriftCodecManager extends FBThriftCodecManager(
  new CompilerThriftCodecFactory(false), ThriftCatalog,
  Collections.emptySet[ThriftCodec[_]]())

