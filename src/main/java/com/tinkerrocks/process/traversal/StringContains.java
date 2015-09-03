package com.tinkerrocks.process.traversal;

import java.util.function.BiPredicate;

/**
 * Created by ashishn on 9/3/15.
 */
public enum StringContains implements BiPredicate<String, String> {
    subString {
        @Override
        public boolean test(final String first, final String second) {
            return second.contains(first);
        }
    },
    notSubString {
        @Override
        public boolean test(String o, String s) {
            return !s.contains(o);
        }
    };

    @Override
    public abstract boolean test(String o, String s);

    @Override
    public BiPredicate<String, String> negate() {
        return this.equals(subString) ? subString : notSubString;
    }

}
