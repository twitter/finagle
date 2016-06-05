/*
 * Copyright 2016 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.twitter.finagle.http2;

import java.util.List;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2StreamFrame;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.util.ReferenceCountUtil;

import static io.netty.handler.codec.http2.Http2Error.PROTOCOL_ERROR;
import static io.netty.handler.codec.http2.Http2Exception.streamError;
import static io.netty.util.internal.ObjectUtil.checkNotNull;

/**
 * This is a server-side adapter so that an http2 codec can be downgraded to
 * appear as if it's speaking http/1.1.
 *
 * In particular, this handler converts from {@link Http2StreamFrame} to {@link
 * HttpObject}, and back.  For simplicity, it converts to chunked encoding
 * unless the entire stream is a single header.
 */
@Sharable
class Http2ServerDowngrader extends MessageToMessageCodec<Http2StreamFrame, HttpObject> {

    private final boolean validateHeaders;

    public Http2ServerDowngrader(boolean validateHeaders) {
        this.validateHeaders = validateHeaders;
    }

    public Http2ServerDowngrader() {
        this.validateHeaders = true;
    }

    @Override
    public boolean acceptInboundMessage(Object msg) throws Exception {
        return (msg instanceof Http2HeadersFrame) || (msg instanceof Http2DataFrame);
    }

    private static HttpRequest toHttpRequest(int streamId, Http2Headers http2Headers,
                                             boolean validateHttpHeaders)
                    throws Http2Exception {
        // HTTP/2 does not define a way to carry the version identifier that is included in the
        // HTTP/1.1 request line.
        final CharSequence method = checkNotNull(http2Headers.method(),
                "method header cannot be null in conversion to HTTP/1.x");
        final CharSequence path = checkNotNull(http2Headers.path(),
                "path header cannot be null in conversion to HTTP/1.x");
        HttpRequest msg = new DefaultHttpRequest(
            HttpVersion.HTTP_1_1,
            HttpMethod.valueOf(method.toString()),
            path.toString(),
            validateHttpHeaders);
        try {
            HttpConversionUtil.addHttp2ToHttpHeaders(streamId, http2Headers, msg.headers(),
                                                     msg.protocolVersion(), false, true);
        } catch (Http2Exception e) {
            throw e;
        } catch (Throwable t) {
            throw streamError(streamId, PROTOCOL_ERROR, t,
                              "HTTP/2 to HTTP/1.x headers conversion error");
        }
        return msg;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, Http2StreamFrame frame,
                          List<Object> out) throws Exception {
        if (frame instanceof Http2HeadersFrame) {
            int id = 0; // not really the id
            Http2HeadersFrame headersFrame = (Http2HeadersFrame) frame;
            Http2Headers headers = headersFrame.headers();

            if (headersFrame.isEndStream()) {
                if (headers.method() == null) {
                    LastHttpContent last = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER,
                                                                      validateHeaders);
                    HttpConversionUtil.addHttp2ToHttpHeaders(id, headers, last.trailingHeaders(),
                                                             HttpVersion.HTTP_1_1, true, true);
                    out.add(last);
                } else {
                    FullHttpRequest full = HttpConversionUtil.toHttpRequest(id, headers,
                        ctx.alloc(), validateHeaders);
                    out.add(full);
                }
            } else {
                out.add(toHttpRequest(id, headersFrame.headers(), validateHeaders));
            }

        } else if (frame instanceof Http2DataFrame) {
            Http2DataFrame dataFrame = (Http2DataFrame) frame;
            if (dataFrame.isEndStream()) {
                out.add(new DefaultLastHttpContent(dataFrame.content(), validateHeaders));
            } else {
                out.add(new DefaultHttpContent(dataFrame.content()));
            }
        }
        ReferenceCountUtil.retain(frame);
    }

    private static void encodeLastContent(LastHttpContent last, List<Object> out,
                                          boolean validateHeaders) {
        boolean needFiller = !(last instanceof FullHttpResponse)
            && last.trailingHeaders().isEmpty();
        if (last.content().readableBytes() > 0 || needFiller) {
            out.add(new DefaultHttp2DataFrame(last.content(), last.trailingHeaders().isEmpty()));
        }
        if (!last.trailingHeaders().isEmpty()) {
            Http2Headers headers = HttpConversionUtil.toHttp2Headers(last.trailingHeaders(),
                                                                     validateHeaders);
            out.add(new DefaultHttp2HeadersFrame(headers, true));
        }
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, HttpObject obj,
                          List<Object> out) throws Exception {
        if (obj instanceof HttpResponse) {
            Http2Headers headers = HttpConversionUtil.toHttp2Headers((HttpResponse) obj,
                                                                     validateHeaders);
            boolean noMoreFrames = false;
            if (obj instanceof FullHttpResponse) {
                FullHttpResponse full = (FullHttpResponse) obj;
                noMoreFrames = full.content().readableBytes() == 0
                    && full.trailingHeaders().isEmpty();
            }

            out.add(new DefaultHttp2HeadersFrame(headers, noMoreFrames));
        }

        if (obj instanceof LastHttpContent) {
            LastHttpContent last = (LastHttpContent) obj;
            encodeLastContent(last, out, validateHeaders);
        } else if (obj instanceof HttpContent) {
            HttpContent cont = (HttpContent) obj;
            out.add(new DefaultHttp2DataFrame(cont.content(), false));
        }
        ReferenceCountUtil.retain(obj);
    }
}
