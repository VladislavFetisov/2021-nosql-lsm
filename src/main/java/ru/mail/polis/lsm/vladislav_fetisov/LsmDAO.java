package ru.mail.polis.lsm.vladislav_fetisov;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
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
    private final int MEMORY_LIMIT = 56 * 1024 * 1024;
    private final AtomicInteger memoryConsumption = new AtomicInteger();
    private final List<SSTable> ssTables = new CopyOnWriteArrayList<>();
    private NavigableMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
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
            memoryConsumption.getAndAdd(size);
            storage.put(record.getKey(), record);
        }
    }


    @Override
    public void compact() throws IOException {
        synchronized (this) {
            Iterator<Record> iterator = SSTablesRange(null, null);
            Path lastTableName = config.getDir().resolve(String.valueOf(ssTables.size()));
            SSTable bigSSTable = SSTable.write(iterator, lastTableName);
            for (SSTable ssTable : ssTables) {
                ssTable.close();
                Files.deleteIfExists(ssTable.getOffsetsName());
                Files.deleteIfExists(ssTable.getFileName());
            }
            ssTables.clear();
            Path zeroTableName = config.getDir().resolve(String.valueOf(0));
            SSTable.rename(SSTable.pathWithSuffix(lastTableName, SSTable.SUFFIX_INDEX),
                    SSTable.pathWithSuffix(zeroTableName, SSTable.SUFFIX_INDEX));
            SSTable.rename(lastTableName, zeroTableName);

            bigSSTable.setFileName(zeroTableName);
            ssTables.add(bigSSTable);

        }
    }


//                Path zeroTableName = config.getDir().resolve(String.valueOf(SStablesCount.getAndIncrement()));
//                ssTables.add(SSTable.write(iterator, zeroTableName));

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
        for (SSTable ssTable : ssTables) {
            ssTable.close();
        }
    }

    private void flush() throws IOException {
//        String SStablePrefix = "SStable_";
//        String ext = ".dat";
        SSTable ssTable = writeSSTable(ssTables.size());
        ssTables.add(ssTable);
        storage = new ConcurrentSkipListMap<>();
        memoryConsumption.set(0);
    }

    private SSTable writeSSTable(int count) throws IOException {
        Path tablePath = config.getDir().resolve(String.valueOf(count));
        Iterator<Record> recordIterator = storage.values().iterator();
        return SSTable.write(recordIterator, tablePath);
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
