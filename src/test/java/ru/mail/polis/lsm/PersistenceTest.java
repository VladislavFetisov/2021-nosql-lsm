package ru.mail.polis.lsm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import ru.mail.polis.lsm.vladislav_fetisov.SSTable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static ru.mail.polis.lsm.Utils.assertDaoEquals;
import static ru.mail.polis.lsm.Utils.key;
import static ru.mail.polis.lsm.Utils.keyWithSuffix;
import static ru.mail.polis.lsm.Utils.recursiveDelete;
import static ru.mail.polis.lsm.Utils.sizeBasedRandomData;
import static ru.mail.polis.lsm.Utils.sizeBasedZeros;
import static ru.mail.polis.lsm.Utils.value;
import static ru.mail.polis.lsm.Utils.valueWithSuffix;
import static ru.mail.polis.lsm.Utils.wrap;

class PersistenceTest {
    @Test
    void fs(@TempDir Path data) throws IOException {
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key(1), value(1)));
        }

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> range = dao.range(null, null);
            assertTrue(range.hasNext());

            Record record = range.next();
            assertEquals(key(1), record.getKey());
            assertEquals(value(1), record.getValue());
        }

        recursiveDelete(data);

        assertFalse(Files.exists(data));
        Files.createDirectory(data);

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            assertFalse(dao.range(null, null).hasNext());
        }
    }

    @Test
    void remove(@TempDir Path data) throws IOException {
        // Reference value
        ByteBuffer key = wrap("SOME_KEY");
        ByteBuffer value = wrap("SOME_VALUE");

        // Create dao and fill data
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key, value));
            Iterator<Record> range = dao.range(null, null);

            assertTrue(range.hasNext());
            assertEquals(value, range.next().getValue());
        }

        // Load data and check
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> range = dao.range(null, null);
            assertTrue(range.hasNext());
            assertEquals(value, range.next().getValue());

            // Remove data and flush
            dao.upsert(Record.tombstone(key));
        }

        // Load and check not found
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> range = dao.range(null, null);

            assertFalse(range.hasNext());
        }
    }

    @Test
    void replaceWithClose(@TempDir Path data) throws Exception {
        ByteBuffer key = wrap("KEY");
        ByteBuffer value = wrap("VALUE_1");
        ByteBuffer value2 = wrap("VALUE_2");

        // Initial insert
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.upsert(Record.of(key, value));

            Iterator<Record> range = dao.range(null, null);
            assertTrue(range.hasNext());
            assertEquals(value, range.next().getValue());
        }

        // Reopen
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> range = dao.range(null, null);
            assertTrue(range.hasNext());
            assertEquals(value, range.next().getValue());

            // Replace
            dao.upsert(Record.of(key, value2));

            Iterator<Record> range2 = dao.range(null, null);
            assertTrue(range2.hasNext());
            assertEquals(value2, range2.next().getValue());
        }

        // Reopen
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            // Last value should win
            Iterator<Record> range2 = dao.range(null, null);
            assertTrue(range2.hasNext());
            assertEquals(value2, range2.next().getValue());
        }
    }

    @Test
    void burn(@TempDir Path data) throws IOException {
        ByteBuffer key = wrap("FIXED_KEY");

        int overwrites = 100;
        for (int i = 0; i < overwrites; i++) {
            ByteBuffer value = value(i);
            try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
                dao.upsert(Record.of(key, value));
                assertEquals(value, dao.range(key, null).next().getValue());
            }

            // Check
            try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
                assertEquals(value, dao.range(key, null).next().getValue());
            }
        }
    }

    @Test
    void hugeRecords(@TempDir Path data) throws IOException {
        // Reference value
        System.out.println(data);
        int size = 1024 * 1024;
        byte[] suffix = sizeBasedRandomData(size);
        byte[] zeros = sizeBasedZeros(size);
        int recordsCount = (int) (TestDaoWrapper.MAX_HEAP * 15 / size);
        List<String> keys = generateSequence(recordsCount);

        prepareHugeDao(data, recordsCount, suffix, keys, zeros);

        // Check
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> range = dao.range(null, null);

            for (int i = 0; i < recordsCount; i++) {
                verifyNext(suffix, range, i, keys, zeros);
            }
            assertFalse(range.hasNext());
        }
    }

    private List<String> generateSequence(int recordsCount) {
        return IntStream.range(0, recordsCount)
                .mapToObj(String::valueOf)
                .sorted()
                .collect(Collectors.toList());
    }

    @Test
    void hugeRecordsSearch(@TempDir Path data) throws IOException {
        System.out.println(data);
        // Reference value
        int size = 1024 * 1024;
        byte[] suffix = sizeBasedRandomData(size);
        byte[] zeros = sizeBasedZeros(size);
        int recordsCount = (int) (TestDaoWrapper.MAX_HEAP * 15 / size);
        List<String> keys = generateSequence(recordsCount + 1);

        prepareHugeDao(data, recordsCount, suffix, keys, zeros);

        // Check
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            int searchStep = 4;

            for (int i = 0; i < recordsCount / searchStep; i++) {
                ByteBuffer keyFrom = keyWithSuffix(keys.get(i * searchStep), zeros);
                ByteBuffer keyTo = keyWithSuffix(keys.get(i * searchStep + searchStep), zeros);
                Iterator<Record> range = dao.range(keyFrom, keyTo);
                for (int j = 0; j < searchStep; j++) {
                    verifyNext(suffix, range, i * searchStep + j, keys, zeros);
                }
                assertFalse(range.hasNext());
            }
        }
    }

    @Test
    void compactAfterAll(@TempDir Path data) throws IOException {
        System.out.println(data);
        int size = 1024 * 1024;
        byte[] suffix = sizeBasedRandomData(size);
        byte[] zeros = sizeBasedZeros(size);
        int recordsCount = (int) (TestDaoWrapper.MAX_HEAP / size);
        List<String> keys = generateSequence(recordsCount);

        prepareHugeDao(data, recordsCount, suffix, keys, zeros);

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.compact();
            assertEquals(SSTable.getAllSSTables(data).size(), 1);
            Iterator<Record> range = dao.range(null, null);

            for (int i = 0; i < recordsCount; i++) {
                verifyNext(suffix, range, i, keys, zeros);
            }
            assertFalse(range.hasNext());
        }
    }

    @Test
    void compactAfterClose(@TempDir Path data) throws IOException {
        System.out.println(data);
        int size = 1024 * 1024;
        byte[] suffix = sizeBasedRandomData(size);
        byte[] zeros = sizeBasedZeros(size);
        int recordsCount = (int) (TestDaoWrapper.MAX_HEAP / size);
        List<String> keys = generateSequence(recordsCount);

        prepareHugeDao(data, recordsCount, suffix, keys, zeros);

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.compact();
            assertEquals(SSTable.getAllSSTables(data).size(), 1);
        }
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            Iterator<Record> range = dao.range(null, null);

            for (int i = 0; i < recordsCount; i++) {
                verifyNext(suffix, range, i, keys, zeros);
            }
            assertFalse(range.hasNext());
        }
    }

    @Test
    void manyCompacts(@TempDir Path data) throws IOException {
        System.out.println(data);
        int size = 1024 * 1024;
        byte[] suffix = sizeBasedRandomData(size);
        byte[] zeros = sizeBasedZeros(size);
        int recordsCount = (int) (TestDaoWrapper.MAX_HEAP / size);
        List<String> keys = generateSequence(recordsCount);

        prepareHugeDao(data, recordsCount, suffix, keys, zeros);

        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.compact();
            assertEquals(SSTable.getAllSSTables(data).size(), 1);
        }
        prepareHugeDao(data, recordsCount, zeros, keys, zeros); //new batch of values
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.compact();
            assertEquals(SSTable.getAllSSTables(data).size(), 1);
            Iterator<Record> range = dao.range(null, null);

            for (int i = 0; i < recordsCount; i++) {
                verifyNext(zeros, range, i, keys, zeros);
            }
            assertFalse(range.hasNext());
        }
    }
    @Test
    void burnAndCompact(@TempDir Path data) throws IOException {
        Map<ByteBuffer, ByteBuffer> map = Utils.generateMap(0, 1);

        int overwrites = 100;
        for (int i = 0; i < overwrites; i++) {
            try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
                map.forEach((k, v) -> dao.upsert(Record.of(k, v)));
            }

            // Check
            try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
                assertDaoEquals(dao, map);
            }
        }

        int beforeCompactSize = getDirSize(data);
        System.out.println("before" + beforeCompactSize);
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            dao.compact();
        }

        // just for sure
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            assertDaoEquals(dao, map);
        }

        int size = getDirSize(data);
        System.out.println("after" + size);

        assertTrue(beforeCompactSize / 50 > size);
    }

    private int getDirSize(Path data) throws IOException {
        int[] size = new int[1];

        Files.walkFileTree(data, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                size[0] += (int) attrs.size();
                return FileVisitResult.CONTINUE;
            }
        });

        return size[0];
    }

    private void verifyNext(byte[] suffix, Iterator<Record> range, int index, List<String> keys, byte[] zeros) {
        ByteBuffer key = keyWithSuffix(keys.get(index), zeros);
        ByteBuffer value = valueWithSuffix(index, suffix);

        Record next = range.next();

        assertEquals(key, next.getKey());
        assertEquals(value, next.getValue());
    }

    private void prepareHugeDao(@TempDir Path data, int recordsCount, byte[] suffix, List<String> keys, byte[] zeros) throws IOException {
        try (DAO dao = TestDaoWrapper.create(new DAOConfig(data))) {
            for (int i = 0; i < recordsCount; i++) {
                ByteBuffer key = keyWithSuffix(keys.get(i), zeros);
                ByteBuffer value = valueWithSuffix(i, suffix);
                dao.upsert(Record.of(key, value));
            }

        }

    }
}
