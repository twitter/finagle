package org.apache.harmony.xnet.provider.jsse;

import java.nio.ByteBuffer;

class AddressUtil {
  static long getDirectBufferAddress(ByteBuffer buf) {
    if (!buf.isDirect())
      // XXX - should we bail out here?
      return 0;
    else
      return getDirectBufferAddressNative(buf);
  }

  static native long getDirectBufferAddressNative(ByteBuffer buf);
}
