package com.twitter.finagle;

import org.junit.Test;

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
    typeAgnosticPassThruFilter.andThen(Filter.TypeAgnostic$.MODULE$.Identity());

    passThruFilter.agnosticAndThen(typeAgnosticPassThruFilter);
    typeAgnosticPassThruFilter.andThen(passThruFilter);

    Filter.<Object, String>identity().andThen(passThruFilter);
    Filter.TypeAgnostic$.MODULE$.Identity().andThen(typeAgnosticPassThruFilter);

    passThruFilter.andThen(constSvc);
    typeAgnosticPassThruFilter.andThen(constSvc);

    passThruFilter.andThen(constSvcFactory);
    typeAgnosticPassThruFilter.andThen(constSvcFactory);
  }
}
