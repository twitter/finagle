package com.twitter.finagle.thrift

import com.twitter.app.GlobalFlag
import com.twitter.util.StorageUnit
import com.twitter.conversions.StorageUnitOps._

object maxReusableBufferSize
    extends GlobalFlag[StorageUnit](
      16.kilobytes,
      "Max size (bytes) for ThriftServicePerEndpoint reusable transport buffer"
    )
