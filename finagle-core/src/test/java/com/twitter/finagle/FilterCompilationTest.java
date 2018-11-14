package com.twitter.finagle;

import org.junit.Test;

import static org.junit.Assert.*;

import com.twitter.util.Future;

public class FilterCompilationTest {

  @Test
  public void testCompilation() {
    Service<Object, String> constSvc = Service.constant(Future.value("hi"));
    ServiceFactory<Object, String> constSvcFactory =
        ServiceFactory.constant(Service.constant(Future.value("hi")));

    Filter<Object, String, Object, String> passThruFilter =
        new Filter<Object, String, Object, String>() {
          @Override
          public Future<String> apply(Object request, Service<Object, String> service) {
            return service.apply(request);
          }
        };

    Filter.TypeAgnostic typeAgnosticPassThruFilter = new Filter.TypeAgnostic() {
      @Override
      public <T, U> Filter<T, U, T, U> toFilter() {
        return new Filter<T, U, T, U>() {
          @Override
          public Future<U> apply(T request, Service<T, U> service) {
            return service.apply(request);
          }
        };
      }
    };

    passThruFilter.andThen(Filter.identity());
    typeAgnosticPassThruFilter.andThen(Filter.typeAgnosticIdentity());

    passThruFilter.agnosticAndThen(typeAgnosticPassThruFilter);
    typeAgnosticPassThruFilter.andThen(passThruFilter);

    Filter.<Object, String>identity().andThen(passThruFilter);
    Filter.typeAgnosticIdentity().andThen(typeAgnosticPassThruFilter);

    passThruFilter.andThen(constSvc);
    typeAgnosticPassThruFilter.andThen(constSvc);

    passThruFilter.andThen(constSvcFactory);
    typeAgnosticPassThruFilter.andThen(constSvcFactory);
  }

  @Test
  public void testToString() {
    Service<Object, String> constSvc = Service.constant(Future.value("hi"));
    ServiceFactory<Object, String> constSvcFactory =
        ServiceFactory.constant(Service.constant(Future.value("hi")));

    Filter<Object, String, Object, String> passThruFilter =
        new Filter<Object, String, Object, String>() {
          @Override
          public Future<String> apply(Object request, Service<Object, String> service) {
            return service.apply(request);
          }

          @Override
          public String toString() {
            return "com.twitter.finagle.PassThruFilter";
          }
        };

    Filter.TypeAgnostic typeAgnosticPassThruFilter = new Filter.TypeAgnostic() {
      @Override
      public <T, U> Filter<T, U, T, U> toFilter() {
        return new Filter<T, U, T, U>() {
          @Override
          public Future<U> apply(T request, Service<T, U> service) {
            return service.apply(request);
          }
          @Override
          public String toString() {
            return "com.twitter.finagle.TypeAgnosticPassThruFilter.Filter";
          }
        };
      }

      @Override
      public String toString() {
        return "com.twitter.finagle.TypeAgnosticPassThruFilter";
      }
    };

    assertEquals(
        "com.twitter.finagle.PassThruFilter",
        passThruFilter.andThen(Filter.identity()).toString());

    assertEquals(
        "com.twitter.finagle.TypeAgnosticPassThruFilter",
        typeAgnosticPassThruFilter.andThen(Filter.typeAgnosticIdentity()).toString());

    assertEquals(
        "com.twitter.finagle.PassThruFilter"
            + ".andThen(com.twitter.finagle.TypeAgnosticPassThruFilter.Filter)",
        passThruFilter.agnosticAndThen(typeAgnosticPassThruFilter).toString());

    assertEquals(
        "com.twitter.finagle.TypeAgnosticPassThruFilter.Filter"
            + ".andThen(com.twitter.finagle.PassThruFilter)",
        typeAgnosticPassThruFilter.andThen(passThruFilter).toString());

    assertEquals(
        "com.twitter.finagle.PassThruFilter",
        Filter.<Object, String>identity().andThen(passThruFilter).toString());

    assertEquals(
        "com.twitter.finagle.TypeAgnosticPassThruFilter",
        Filter.typeAgnosticIdentity().andThen(typeAgnosticPassThruFilter).toString());

    assertEquals(
        "com.twitter.finagle.PassThruFilter"
            + ".andThen(com.twitter.finagle.service.ConstantService(ConstFuture(Return(hi))))",
        passThruFilter.andThen(constSvc).toString());

    assertEquals(
        "com.twitter.finagle.TypeAgnosticPassThruFilter.Filter"
            + ".andThen(com.twitter.finagle.service.ConstantService(ConstFuture(Return(hi))))",
        typeAgnosticPassThruFilter.andThen(constSvc).toString());

    assertEquals(
        "com.twitter.finagle.PassThruFilter"
            + ".andThen(com.twitter.finagle.service.ConstantService(ConstFuture(Return(hi))))",
        passThruFilter.andThen(constSvcFactory).toString());

    assertEquals(
        "com.twitter.finagle.TypeAgnosticPassThruFilter.Filter"
            + ".andThen(com.twitter.finagle.service.ConstantService(ConstFuture(Return(hi))))",
        typeAgnosticPassThruFilter.andThen(constSvcFactory).toString());
  }
}
