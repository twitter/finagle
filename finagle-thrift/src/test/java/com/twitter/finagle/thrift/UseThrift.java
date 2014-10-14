package com.twitter.finagle.thrift;

import com.twitter.finagle.Thrift;
import com.twitter.test.*;
import com.twitter.finagle.Thrift;

// Compilation test. Not actually for running.
public class UseThrift {
  static {
    Thrift.newIface(":8000", B.ServiceIface.class);
    Thrift.serveIface(":8000", null);
    Thrift.client().withProtocolFactory(null).newIface(":8000", B.ServiceIface.class);
  }
}
