package ru.mail.polis.lsm;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * Minimal database API.
 */
public interface DAO extends Closeable {
    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey);

    void upsert(Record record);

    void compact() throws IOException;

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
                return mergeTwo(new PeekingIterator<>(iterators.get(0)), new PeekingIterator<>(iterators.get(1)));
            default:
                return mergeList(iterators);
        }
    }

    static Iterator<Record> mergeList(List<Iterator<Record>> iterators) {
        return iterators
                .stream()
                .map(PeekingIterator::new)
                .reduce(DAO::mergeTwo)
                .get();
    }

    static PeekingIterator<Record> mergeTwo(PeekingIterator<Record> it1, PeekingIterator<Record> it2) {
        return new PeekingIterator<>(new Iterator<>() {

            @Override
            public boolean hasNext() {
                return it1.hasNext() || it2.hasNext();
            }

            @Override
            public Record next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                if (!it1.hasNext()) {
                    return it2.next();
                }
                if (!it2.hasNext()) {
                    return it1.next();
                }
                Record record1 = it1.peek();
                Record record2 = it2.peek();
                int compare = record1.getKey().compareTo(record2.getKey());
                if (compare < 0) {
                    it1.next();
                    return record1;
                } else if (compare == 0) {
                    it1.next();
                    it2.next();
                    return record2;
                } else {
                    it2.next();
                    return record2;
                }
            }
        });
    }


    class PeekingIterator<T> implements Iterator<T> {
        private final Iterator<T> iterator;
        private T current = null;

        public PeekingIterator(Iterator<T> iterator) {
            this.iterator = iterator;
        }


        public T peek() {
            if (current == null) {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return current = iterator.next();
            }
            return current;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext() || current != null;
        }

        @Override
        public T next() {
            T res = peek();
            current = null;
            return res;
        }

    }

}
