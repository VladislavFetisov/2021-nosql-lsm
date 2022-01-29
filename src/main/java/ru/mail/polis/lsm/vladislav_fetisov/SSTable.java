package ru.mail.polis.lsm.vladislav_fetisov;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import ru.mail.polis.lsm.Record;

class SSTable implements Closeable {
    private static final String SUFFIX_INDEX = "i";
    private static final Method CLEAN;
    private MappedByteBuffer nmap;
    private MappedByteBuffer offsetMap;
    private int[] offsets;
    private final Path file;
    private final Path indexFile;

    static {
        try {
            Class<?> aClass = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = aClass.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new IllegalStateException();
        }
    }

    private SSTable(Path file) {
        this.file = file;
        this.indexFile = Path.of(file + SUFFIX_INDEX);
    }

    static List<SSTable> getAllSSTables(Path dir) throws IOException {
        File file = dir.toFile();
        char i = SUFFIX_INDEX.charAt(0);
        File[] files = file.listFiles(pathname -> {
            String name = pathname.getName();
            return name.charAt(name.length() - 1) != i;
        });
        if (files == null) {
            return Collections.emptyList();
        }
        return Arrays.stream(files)
                .map(file1 -> Integer.parseInt(file1.getName()))
                .sorted()
                .map(integer -> {
                    Path tableName = dir.resolve(Path.of(String.valueOf(integer)));
                    return new SSTable(tableName);
                })
                .collect(Collectors.toList());

    }

    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        if ((fromKey != null || toKey != null) && offsets == null) {
            readOffsets();
        }
        List<Record> records = new ArrayList<>();
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            if (nmap == null) {
                nmap = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
            }
            int leftPos;
            int rightPos;
            int temp;
            if (fromKey == null) {
                leftPos = 0;
            } else {
                temp = leftBinarySearch(0, offsets.length, fromKey, nmap);
                if (temp == -1) {
                    return Collections.emptyIterator();
                }
                leftPos = offsets[temp];
            }
            if (toKey == null) {
                rightPos = (int) channel.size();
            } else {
                temp = rightBinarySearch(0, offsets.length, toKey, nmap);
                if (temp == -1) {
                    return Collections.emptyIterator();
                }
                if (temp == offsets.length) {
                    rightPos = (int) channel.size();
                } else {
                    rightPos = offsets[temp];
                }
            }
            nmap.position(leftPos);
            while (nmap.position() != rightPos) {
                ByteBuffer key = read(nmap);
                ByteBuffer value = read(nmap);
                records.add(Record.of(key, value));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        nmap.position(0);
        return records.iterator();
    }


    private void readOffsets() {
        try (FileChannel channel = FileChannel.open(indexFile, StandardOpenOption.READ)) {
            offsetMap = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
            offsets = new int[(int) (channel.size() / Integer.BYTES)];
            int i = 0;
            while (offsetMap.hasRemaining()) {
                offsets[i++] = offsetMap.getInt();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private ByteBuffer read(MappedByteBuffer from) {
        int length = from.getInt();
        ByteBuffer limit = from.slice().limit(length);
        from.position(from.position() + length);
        return limit;
    }

    private int leftBinarySearch(int l, int r, @Nonnull ByteBuffer key, MappedByteBuffer array) {
        while (l != r) {
            int mid = (l + r) / 2;
            int res = compareKeys(mid, key, array);
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

    private int rightBinarySearch(int l, int r, @Nonnull ByteBuffer key, MappedByteBuffer array) {
        if (compareKeys(l, key, array) >= 0) {
            return -1;
        }
        while (l != r) {
            int mid = (l + r + 1) / 2;
            if (mid >= r) {
                return r;
            }
            int res = compareKeys(mid, key, array);
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

    private int compareKeys(int mid, ByteBuffer key, MappedByteBuffer array) {
        ByteBuffer buffer = readKey(array, mid);
        return buffer.compareTo(key);
    }


    private ByteBuffer readKey(MappedByteBuffer from, int index) {
        from.position(offsets[index]);
        int length = from.getInt();
        ByteBuffer key = from.slice().limit(length);
        return key.asReadOnlyBuffer();
    }

    static SSTable write(Iterator<Record> records, Path tableName, Iterator<Integer> offsets) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        writeToDisc(records, buffer, tableName);
        writeToDisc(offsets, buffer, Path.of(tableName + SUFFIX_INDEX));
        return new SSTable(tableName);
    }

    private static <E> void writeToDisc(Iterator<E> iterator, ByteBuffer forLength, Path file) throws IOException {
        if (!iterator.hasNext()) {
            return;
        }
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            E el = iterator.next();
            if (el instanceof Record) {
                Record record = (Record) el;
                writeRecord(forLength, channel, record);
                while (iterator.hasNext()) {
                    record = (Record) iterator.next();
                    writeRecord(forLength, channel, record);
                }
            } else if (el instanceof Integer) {
                int offset = (int) el;
                writeInt(offset, channel, forLength);
                while (iterator.hasNext()) {
                    offset = (int) iterator.next();
                    writeInt(offset, channel, forLength);
                }
            }
            channel.force(false);
        }
    }

    private static void writeRecord(ByteBuffer forLength, FileChannel channel, Record record) throws IOException {
        writeBuffer(record.getKey(), channel, forLength);
        writeBuffer(record.getValue(), channel, forLength);
    }

    private static void writeBuffer(ByteBuffer value, WritableByteChannel channel, ByteBuffer forLength) throws IOException {
        forLength.position(0);
        forLength.putInt(value.remaining());
        forLength.position(0);
        channel.write(forLength);
        channel.write(value);
    }

    private static void writeInt(int value, WritableByteChannel channel, ByteBuffer elementBuffer) throws IOException {
        elementBuffer.position(0);
        elementBuffer.putInt(value);
        elementBuffer.position(0);
        channel.write(elementBuffer);
    }

    public void close() {
        unmap(nmap);
        unmap(offsetMap);
    }

    private void unmap(@Nullable MappedByteBuffer nmap) {
        if (nmap != null) {
            try {
                CLEAN.invoke(null, nmap);
            } catch (InvocationTargetException | IllegalAccessException e) {
                throw new IllegalStateException();
            }
        }
    }

}
