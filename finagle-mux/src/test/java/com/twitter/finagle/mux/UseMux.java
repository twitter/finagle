package com.twitter.finagle;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.twitter.util.Future;

// A compilation test for the Mux API.
public class UseMux {
  static {
    ServiceFactory<ChannelBuffer, ChannelBuffer> client = Mux.newClient(":8000");
    Mux.serve(":8000", new Service<ChannelBuffer, ChannelBuffer>() {
      public Future<ChannelBuffer> apply(ChannelBuffer req) {
        return Future.value(ChannelBuffers.EMPTY_BUFFER);
      }
    });
  }
}
