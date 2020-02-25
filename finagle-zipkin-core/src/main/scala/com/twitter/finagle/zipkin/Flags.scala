package com.twitter.finagle.zipkin

import com.twitter.app.GlobalFlag
import com.twitter.finagle.zipkin.core.Sampler

object initialSampleRate extends GlobalFlag[Float](Sampler.DefaultSampleRate, "Initial sample rate")
