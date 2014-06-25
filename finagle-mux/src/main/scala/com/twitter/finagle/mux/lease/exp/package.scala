package com.twitter.finagle.mux.lease

import java.lang.management.GarbageCollectorMXBean

/**
 * This is the experimental package of mux.lease.  Right now, this is all
 * experimental code around leasing, especially leasing around garbage
 * collections.  We haven't hammered out exactly what the api will be, so it's
 * in large part private and is subject to change.
 *
 * NB: large parts of this package might suddenly end up in util-jvm
 */
package object exp {
  implicit def gcMxBeanToGc(coll: GarbageCollectorMXBean): GarbageCollectorAddable =
    new GarbageCollectorAddable(coll)

  class GarbageCollectorAddable(self: GarbageCollectorMXBean) {
    def +(other: GarbageCollectorMXBean): GarbageCollectorMXBean = new GarbageCollectorMXBean {
      def getCollectionCount() =
        self.getCollectionCount() + other.getCollectionCount()
      def getCollectionTime() =
        self.getCollectionTime() + other.getCollectionTime()
      def getMemoryPoolNames() =
        Array.concat(self.getMemoryPoolNames(), other.getMemoryPoolNames())
      def getName() = self.getName() + "+" + other.getName()
      def isValid() = self.isValid || other.isValid
      def getObjectName = throw new UnsupportedOperationException
    }
  }
}
