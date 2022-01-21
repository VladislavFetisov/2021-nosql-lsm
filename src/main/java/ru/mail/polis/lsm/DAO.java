package ru.mail.polis.lsm;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

/**
 * Minimal database API.
 */
public interface DAO extends Closeable {
    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey);

    void upsert(Record record);

    /**
     * Appends {@code Byte.MIN_VALUE} to {@code buffer}.
     *
     * @param buffer original {@link ByteBuffer}
     * @return copy of {@code buffer} with {@code Byte.MIN_VALUE} appended
     */
    static ByteBuffer nextKey(ByteBuffer buffer) {
        ByteBuffer result = ByteBuffer.allocate(buffer.remaining() + 1);

        int position = buffer.position();

        result.put(buffer);
        result.put(Byte.MIN_VALUE);

        buffer.position(position);
        result.rewind();

        return result;
    }

    static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        switch (iterators.size()) {
            case 0:
                return Collections.emptyIterator();
            case 1:
                return iterators.get(0);
            case 2:
                return mergeTwo(iterators.get(0), iterators.get(1));
            default:
                return mergeList(iterators);
        }
    }

    static Iterator<Record> mergeList(List<Iterator<Record>> iterators) {
        return iterators
                .stream()
                .reduce(DAO::mergeTwo)
                .get();
    }

    static Iterator<Record> mergeTwo(Iterator<Record> it1, Iterator<Record> it2) {
        List<Record> result = new ArrayList<>();
        Record record1 = null;
        Record record2 = null;
        boolean fromFirst = true;
        boolean fromSecond = true;
        while (true) {
            if (fromFirst && fromSecond) {
                if (!it2.hasNext()) {
                    return mergeRightPart(it1, result);
                }
                if (!it1.hasNext()) {
                    return mergeRightPart(it2, result);
                }
                record1 = it1.next();
                record2 = it2.next();
            } else if (fromFirst) {
                if (!it1.hasNext()) {
                    result.add(record2);
                    return mergeRightPart(it2, result);
                }
                record1 = it1.next();
            } else {
                if (!it2.hasNext()) {
                    result.add(record1);
                    return mergeRightPart(it1, result);
                }
                record2 = it2.next();
            }
            int compare = DAO.toString(record1.getKey()).compareTo(DAO.toString(record2.getKey()));
            if (compare > 0) {
                result.add(record2);
                fromSecond = true;
                fromFirst = false;
            } else if (compare < 0) {
                result.add(record1);
                fromFirst = true;
                fromSecond = false;
            } else {
                result.add(record2);
                fromFirst = true;
                fromSecond = true;
            }
        }
    }

    static Iterator<Record> mergeRightPart(Iterator<Record> iterator, List<Record> result) {
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        return result.iterator();
    }

    static String toString(ByteBuffer buffer) {
        try {
            return StandardCharsets.UTF_8.newDecoder().decode(buffer).toString();
        } catch (CharacterCodingException e) {
            throw new RuntimeException(e);
        }
    }
}
