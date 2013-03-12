package com.twitter.finagle;

import com.twitter.test.*;

// Compilation test. Not actually for running.
public class UseThrift {
  static {
    Thrift.newIface(":8000", B.ServiceIface.class);
    Thrift.serveIface(":8000", null);
    Thrift.withProtocolFactory(null).newIface(":8000", B.ServiceIface.class);
  }
}
