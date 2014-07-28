package com.twitter.finagle.exp.swift;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target(METHOD)
public @interface ThriftMethod {
    String value() default "";

    ThriftException[] exception() default {};

    // Note: oneways are currently unsupported.
}
