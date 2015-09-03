package com.tinkerrocks.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.function.BiPredicate;

/**
 * Created by ashishn on 8/16/15.
 */
public class Predicate<V> extends P<V> {
    public Predicate(BiPredicate<V, V> biPredicate, V value) {
        super(biPredicate, value);
    }

    @SuppressWarnings("unchecked")
    public static <V> P<V> stringContains(final V value) {
        if (!(value instanceof String)) {
            throw new IllegalArgumentException("cannot compare String and class: " + value.getClass());
        }
        return new P(StringContains.subString, value);
    }


}
