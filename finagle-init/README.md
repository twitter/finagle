Finagle-Init supports service-loading initialization code for Finagle applications.

To use it, implement the `com.twitter.finagle.FinagleInit` trait and create a
resource file in `META-INF/services/FullyQualifiedClassName` which contains the
fully qualified class name of your implementation. Finagle will discover your
module and execute it before any other framework initialization code. Note that
there are *no* ordering guarantees when multiple `FinagleInit` modules are
registered.
