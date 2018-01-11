package com.twitter.finagle.netty3.http

import com.twitter.finagle.http.AbstractMultipartDecoderTest

class Netty3MultipartDecoderTest extends AbstractMultipartDecoderTest(new Netty3MultipartDecoder)
