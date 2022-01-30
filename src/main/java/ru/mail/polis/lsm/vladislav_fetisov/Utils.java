package ru.mail.polis.lsm.vladislav_fetisov;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import javax.annotation.Nonnull;

public class Utils {
    public static int leftBinarySearch(int l, int r, @Nonnull ByteBuffer key, MappedByteBuffer array, int[] offsets) {
        while (l != r) {
            int mid = (l + r) / 2;
            int res = compareKeys(mid, key, array, offsets);
            if (res == 0) {
                return mid;
            }
            if (res > 0) {
                r = mid;
            } else {
                l = mid + 1;
            }
        }
        if (l == offsets.length) {
            return -1;
        }
        return l;
    }

    public static int rightBinarySearch(int l, int r, @Nonnull ByteBuffer key, MappedByteBuffer array, int[] offsets) {
        if (compareKeys(l, key, array, offsets) >= 0) {
            return -1;
        }
        while (l != r) {
            int mid = (l + r + 1) / 2;
            if (mid >= r) {
                return r;
            }
            int res = compareKeys(mid, key, array, offsets);
            if (res == 0) {
                return mid;
            }
            if (res > 0) {
                r = mid - 1;
            } else {
                l = mid;
            }
        }
        return l;
    }

    private static int compareKeys(int mid, ByteBuffer key, MappedByteBuffer array, int[] offsets) {
        ByteBuffer buffer = readKey(array, mid, offsets);
        return buffer.compareTo(key);
    }


    private static ByteBuffer readKey(MappedByteBuffer from, int index, int[] offsets) {
        from.position(offsets[index]);
        int length = from.getInt();
        ByteBuffer key = from.slice().limit(length);
        return key.asReadOnlyBuffer();
    }
}
