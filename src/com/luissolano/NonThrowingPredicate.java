package com.luissolano;

import io.reactivex.functions.Predicate;

public interface NonThrowingPredicate<T> extends Predicate<T> {

    @Override
    boolean test(T t);
}
