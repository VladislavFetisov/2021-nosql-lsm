package ru.mail.polis.lsm.vladislav_fetisov;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.annotation.Nullable;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;

public class InMemory implements DAO {
    private final NavigableMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null && toKey == null) {
            return storage.values().iterator();
        }
        return subMap(fromKey, toKey).values().iterator();
    }

    @Override
    public void upsert(Record record) {
        if (record.isTombstone()) {
            storage.remove(record.getKey());
            return;
        }
        storage.put(record.getKey(), record);
    }

    @Override
    public void compact() {

    }

    @Override
    public void close() throws IOException {

    }

    private SortedMap<ByteBuffer, Record> subMap(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if (fromKey == null) {
            return storage.headMap(toKey);
        }
        if (toKey == null) {
            return storage.tailMap(fromKey);
        }
        return storage.subMap(fromKey, toKey);
    }

}
