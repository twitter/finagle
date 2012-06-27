package com.twitter.finagle.protobuf.rpc

import java.util.List

import com.google.common.base.Function
import com.google.common.collect.Lists
import com.google.protobuf.Descriptors.MethodDescriptor
import com.google.protobuf.Service
import com.google.protobuf.Message

import scala.collection.JavaConversions._
import scala.collection.mutable._

import org.slf4j.LoggerFactory

object Util {

  private val log = LoggerFactory.getLogger(getClass)

  def extractMethodNames(s: Service): List[String] = {
    return Lists.transform(s.getDescriptorForType().getMethods(),
      new Function[MethodDescriptor, String]() {

        @Override
        def apply(d: MethodDescriptor): String = {
          return d.getName()
        }
      })
  }

  def log(reqOrResp: String, method: String, m: Message) {
    if (log.isDebugEnabled()) {
      log.debug("#log# {} {}: {}", Array[String](reqOrResp, method, m.toString()))
    }
  }
}
