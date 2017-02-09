package com.luissolano;

import io.reactivex.functions.Function;

public interface NonThrowingFunction<T, R> extends Function<T, R> {
    R apply(T t);
}
