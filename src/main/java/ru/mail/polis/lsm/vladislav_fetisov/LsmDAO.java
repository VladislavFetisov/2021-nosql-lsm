package ru.mail.polis.lsm.vladislav_fetisov;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

public class LsmDAO implements DAO {
    private final long MEMORY_LIMIT = 56 * 1024 * 1024;
    private static final AtomicInteger SStablesCount = new AtomicInteger();
    private final AtomicInteger memoryConsumption = new AtomicInteger();
    private final List<SSTable> ssTables = new CopyOnWriteArrayList<>();
    private final NavigableMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private final NavigableMap<ByteBuffer, Integer> fileOffsets = new ConcurrentSkipListMap<>();
    private final DAOConfig config;

    public LsmDAO(DAOConfig config) {
        this.config = config;
        try {
            ssTables.addAll(SSTable.getAllSSTables(config.getDir()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Iterator<Record> memRange = map(fromKey, toKey, storage);
        Iterator<Record> SSTablesRange = SSTablesRange(fromKey, toKey);
        PeekingIterator<Record> result = DAO.mergeTwo(new PeekingIterator<>(SSTablesRange), new PeekingIterator<>(memRange));
        return filteredResult(result);
    }

    private Iterator<Record> filteredResult(PeekingIterator<Record> result) {
        List<Record> filteredResult = new ArrayList<>();
        result.forEachRemaining((record) -> {
            if (!record.isTombstone()) {
                filteredResult.add(record);
            }
        });
        return filteredResult.iterator();
    }

    private Iterator<Record> SSTablesRange(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> ranges = new ArrayList<>();
        for (SSTable table : ssTables) {
            ranges.add(table.range(fromKey, toKey));
        }
        return DAO.merge(ranges);
    }

    @Override
    public void upsert(Record record) {
        synchronized (this) {
            int size = record.size();
            if (memoryConsumption.get() + size > MEMORY_LIMIT) {
                try {
                    flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            ByteBuffer key = record.getKey();
            fileOffsets.put(key, memoryConsumption.getAndAdd(size));
            if (record.isTombstone()) {
                //Здесь можно было бы уменьшать memoryConsumption, но по GC не заберет это значение это не очень
                //правдивая информация
            }
            storage.put(key, record);
        }
    }

    private void checkMemory() {
        long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        System.out.println("our memory " + memoryConsumption.get() / 1024L);
        System.out.println("memory use: " + usedMemory / 1024L);
        long free = Runtime.getRuntime().maxMemory() - usedMemory;
        System.out.println("free: " + free / 1024L);
        System.out.println("all " + (usedMemory + free) / 1024L);
    }

    @Override
    public void close() throws IOException {
        flush();
        ssTables.forEach(SSTable::close);
    }

    private void flush() throws IOException {
//        String SStablePrefix = "SStable_";
//        String ext = ".dat";
        int count = SStablesCount.getAndIncrement();
        SSTable ssTable = writeSSTable(count);
        ssTables.add(ssTable);
        storage.clear();
        fileOffsets.clear();
        memoryConsumption.set(0);
    }

    private SSTable writeSSTable(int count) throws IOException {
        Path tablePath = config.getDir().resolve(String.valueOf(count));
        Iterator<Record> recordIterator = storage.values().iterator();
        Iterator<Integer> offsetsIterator = fileOffsets.values().iterator();
        return SSTable.write(recordIterator, tablePath, offsetsIterator);
    }

    static public Iterator<Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey, NavigableMap<ByteBuffer, Record> storage) {
        if (fromKey == null && toKey == null) {
            return storage.values().iterator();
        }
        return subMap(fromKey, toKey, storage).values().iterator();
    }

    static public SortedMap<ByteBuffer, Record> subMap(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey, NavigableMap<ByteBuffer, Record> storage) {
        if (fromKey == null) {
            return storage.headMap(toKey);
        }
        if (toKey == null) {
            return storage.tailMap(fromKey);
        }
        return storage.subMap(fromKey, toKey);
    }

}
