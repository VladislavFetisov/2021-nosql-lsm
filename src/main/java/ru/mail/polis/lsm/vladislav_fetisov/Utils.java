package ru.mail.polis.lsm.vladislav_fetisov;

import java.nio.ByteBuffer;

import javax.annotation.Nonnull;

public class Utils {
    public static int leftBinarySearch(int l, int r, @Nonnull ByteBuffer key, ByteBuffer records, ByteBuffer offsets) {
        while (l != r) {
            int mid = (l + r) / 2;// TODO может быть переполнение
            mid -= (mid % Integer.BYTES);
            int res = compareKeys(mid, key, records, offsets);
            if (res == 0) {
                return mid;
            }
            if (res > 0) {
                r = mid;
            } else {
                l = mid + Integer.BYTES;
            }
        }
        if (l == offsets.limit()) {
            return -1;
        }
        return l;
    }

    public static int rightBinarySearch(int l, int r, @Nonnull ByteBuffer key, ByteBuffer records, ByteBuffer offsets) {
        if (compareKeys(l, key, records, offsets) >= 0) {
            return -1;
        }
        while (l != r) {
            int mid = (l + r + Integer.BYTES) / 2;
            mid -= (mid % Integer.BYTES);
            if (mid >= r) {
                return r;
            }
            int res = compareKeys(mid, key, records, offsets);
            if (res == 0) {
                return mid;
            }
            if (res > 0) {
                r = mid - Integer.BYTES;
            } else {
                l = mid;
            }
        }
        return l;
    }

    private static int compareKeys(int mid, ByteBuffer key, ByteBuffer records, ByteBuffer offsets) {
        ByteBuffer buffer = readKey(mid, records, offsets);
        return buffer.compareTo(key);
    }


    private static ByteBuffer readKey(int index, ByteBuffer records, ByteBuffer offsets) {
        int offset = getInt(offsets, index);
        int length = getInt(records, offset);
        ByteBuffer key = records.slice().limit(length);
        return key.asReadOnlyBuffer();
    }

    private static int getInt(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        return buffer.getInt();
    }
}
