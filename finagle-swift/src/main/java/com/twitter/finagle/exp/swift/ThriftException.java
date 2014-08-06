package com.twitter.finagle.exp.swift;

import java.lang.annotation.Retention;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
public @interface ThriftException {
    Class<? extends Throwable> type();

    short id();

    String name() default "";
}
