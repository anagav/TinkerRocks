package com.tinkerrocks.structure;

import com.google.common.primitives.Bytes;
import org.apache.commons.lang.ArrayUtils;
import org.rocksdb.RocksIterator;

import java.util.Arrays;

/**
 * <p>
 * generic utils class.
 * </p>
 * Created by ashishn on 8/5/15.
 */
public class Utils {

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

    public static byte[] merge(byte[] arr1, byte arr2) {
        return ArrayUtils.add(arr1, arr2);
    }


    public static byte[] merge(byte[] arr1, byte[] arr2, byte[] arr3) {
        return ArrayUtils.addAll(ArrayUtils.addAll(arr1, arr2), arr3);
    }

    public static byte[] merge(byte arr1, byte[] arr2, byte[] arr3) {
        return ArrayUtils.addAll(ArrayUtils.add(arr2, 0, arr1), arr3);
    }

    public static byte[] merge(byte[] arr1, byte arr2, byte[] arr3) {
        return ArrayUtils.addAll(ArrayUtils.add(arr1, arr2), arr3);
    }

    public static byte[] merge(byte arr1, byte[] arr2, byte arr3) {
        return ArrayUtils.add(ArrayUtils.add(arr2, 0, arr1), arr3);
    }


    public static byte[] merge(byte[] arr1, byte arr2, byte arr3) {
        return ArrayUtils.add(ArrayUtils.add(arr1, arr2), arr3);
    }

    public static byte[] merge(byte[] arr1, byte[] arr2, byte arr3) {
        return ArrayUtils.add(ArrayUtils.addAll(arr1, arr2), arr3);
    }


    public static void RocksIterUtil(RocksIterator rocksIterator, byte[] seek_key, RocksIteratorCallback rocksIteratorCallback) throws Exception {
        RocksIterUtil(rocksIterator, true, seek_key, rocksIteratorCallback);
    }


    public static void RocksIterUtil(RocksIterator rocksIterator, boolean disposeIterator, byte[] seek_key, RocksIteratorCallback rocksIteratorCallback) throws Exception {
        boolean returnValue = true;
        try {
            for (rocksIterator.seek(seek_key); returnValue && rocksIterator.isValid() &&
                    startsWith(rocksIterator.key(), 0, seek_key); rocksIterator.next()) {
                returnValue = rocksIteratorCallback.process(rocksIterator.key(), rocksIterator.value());
            }
        } finally {
            if (disposeIterator)
                rocksIterator.dispose();
        }
    }


    public static int findLastInArray(byte[] arr, byte toFind) {
        return Bytes.lastIndexOf(arr, toFind) + 1;
    }


    public static boolean compare(byte[] data1, byte[] data2) {
        return Arrays.equals(data1, data2);
    }
}
