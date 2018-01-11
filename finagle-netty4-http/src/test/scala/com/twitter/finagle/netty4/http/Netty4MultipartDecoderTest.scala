package com.twitter.finagle.netty4.http

import com.twitter.finagle.http.AbstractMultipartDecoderTest

class Netty4MultipartDecoderTest extends AbstractMultipartDecoderTest(new Netty4MultipartDecoder)
