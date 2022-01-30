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
        List<SSTable> sortedSSTables = Arrays.stream(files)
                .map(file1 -> Integer.parseInt(file1.getName()))
                .sorted()
                .map(integer -> {
                    Path tableName = dir.resolve(Path.of(String.valueOf(integer)));
                    return new SSTable(tableName);
                })
                .collect(Collectors.toList());
        return sortedSSTables;

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
                temp = Utils.leftBinarySearch(0, offsets.length, fromKey, nmap, offsets);
                if (temp == -1) {
                    return Collections.emptyIterator();
                }
                leftPos = offsets[temp];
            }
            if (toKey == null) {
                rightPos = (int) channel.size();
            } else {
                temp = Utils.rightBinarySearch(0, offsets.length, toKey, nmap, offsets);
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
                if (value == null) {
                    records.add(Record.tombstone(key));
                    continue;
                }
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

    @Nullable
    private ByteBuffer read(MappedByteBuffer from) {
        int length = from.getInt();
        if (length == -1) {
            return null;
        }
        ByteBuffer limit = from.slice().limit(length);
        from.position(from.position() + length);
        return limit;
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

    private static void writeBuffer(@Nullable ByteBuffer value, WritableByteChannel channel, ByteBuffer forLength) throws IOException {
        forLength.position(0);
        forLength.putInt((value == null) ? -1 : value.remaining());
        forLength.position(0);
        channel.write(forLength);
        if (value != null) {
            channel.write(value);
        }
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
