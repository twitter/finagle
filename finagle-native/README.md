# Finagle OpenSSL Support

## Synopsis
This package contains support for using OpenSSL with Finagle.

## Building

### Prerequisites
APR:     http://apr.apache.org/
OpenSSL: http://openssl.org/

You need OpenSSL 1.0.1 or greater, or 1.0.0g with an SPDY NPN patch applied.

For OpenSSL 1.0.0g, an adaptation of Google's NPN patch is available at:
  https://gist.github.com/1772441

### Instructions
Fetch and apply the patch with the script grab_and_patch_tomcat_native.sh, included in this directory.

- Build tomcat-native (see tomcat-native-*/README.txt)
- Include the produced JAR (jni/dist/tomcat-native-*-dev.jar) in the runtime classpath of your Finagle application. There is no compile-time dependency.
- Include the produced shared objects (jni/native/.libs/libtcnative-1.{so,dylib,dll}) on your library path (java.library.path, or LD_LIBRARY_PATH, DYLD_LIBRARY_PATH, or PATH, respectively.)
- Finagle should automatically try to use the native provider.
