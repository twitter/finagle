package com.twitter.finagle.offload

import com.twitter.app.GlobalFlag

object auto
    extends GlobalFlag[Boolean](
      false,
      """Experimental flag. When true, enables offload pool on all servers/clients. The default
     | thread configuration is derived based on available CPUs with CPUs/3 threads going into the
     | IO/Netty pool and CPUs threads allocated for offload. For example, on a 12 CPU host, 4
     | threads would be allocated to Netty and 12 to Offload. Note that 4 threads is the floor
     | for the Netty worker pool so any host with <12 CPUs will default to it.
  """.stripMargin
    )
