package com.tinkerrocks.structure;

import org.apache.commons.lang.ArrayUtils;

import java.util.Arrays;

/**
 * Created by ashishn on 8/5/15.
 */
public class ByteUtil {

    public static boolean startsWith(byte[] source, int offset, byte[] match) {

        if (match.length > (source.length - offset)) {
            return false;
        }

        for (int i = 0; i < match.length; i++) {
            if (source[offset + i] != match[i]) {
                return false;
            }
        }
        return true;
    }


    public static byte[] slice(byte[] arr, int start, int end) {
        return Arrays.copyOfRange(arr, start, end);
    }

    public static byte[] slice(byte[] arr, int start) {
        return Arrays.copyOfRange(arr, start, arr.length);
    }

    public static byte[] merge(byte[] arr1, byte[] arr2) {
        return ArrayUtils.addAll(arr1, arr2);
    }


}
