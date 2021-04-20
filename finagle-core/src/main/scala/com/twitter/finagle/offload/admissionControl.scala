package com.twitter.finagle.offload

import com.twitter.app.GlobalFlag

object admissionControl
    extends GlobalFlag[OffloadFilterAdmissionControl.Params](
      OffloadFilterAdmissionControl.Disabled,
      """Experimental flag for enabling OffloadPool based admission control. The 
    |basic idea is to use the pending work queue for the OffloadFilter as a
    |signal that the system is overloaded.
    |
    |Note: other admission control mechanisms should be disabled if this is used.
    |They can be disabled via the flags:
    |`com.twitter.server.filter.throttlingAdmissionControl=none`
    |`com.twitter.server.filter.cpuAdmissionControl=none`
    |CPU based admission control is disabled by default while throttlingAdmissionControl
    |is enabled by default.
    |
    |Flag values
    |'none': Offload based admission control is disabled.
    |'default': The default configuration which may change. Currently the same as 'none'.
    |'enabled': Offload based admission control is enabled with common parameters.
    |'maxQueueDelay': the window over which to monitor queue health in terms of a duration. Example: 50.milliseconds""".stripMargin
    )
