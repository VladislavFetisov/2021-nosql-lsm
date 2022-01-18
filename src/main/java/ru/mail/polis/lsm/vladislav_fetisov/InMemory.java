package ru.mail.polis.lsm.vladislav_fetisov;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.annotation.Nullable;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

public class InMemory implements DAO {
    private final String FILE_NAME = "DAO";
    private final String TEMP_NAME = "TEMP";
    private static Method CLEAN;

    static {
        try {
            Class<?> aClass = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = aClass.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    private static MappedByteBuffer nmap;
    private final NavigableMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private final DAOConfig config;

    public InMemory(DAOConfig config) {
        this.config = config;
        Path path = config.getDir().resolve(FILE_NAME);
        if (!Files.exists(path)) {
            nmap = null;
            return;
        }
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            nmap = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
            while (nmap.hasRemaining()) {
                ByteBuffer key = read(nmap);
                ByteBuffer value = read(nmap);
                storage.put(key, Record.of(key, value));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private ByteBuffer read(MappedByteBuffer from) {
        int length = from.getInt();
        ByteBuffer limit = from.slice().limit(length);
        from.position(from.position() + length);
        return limit.asReadOnlyBuffer();
    }

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
    public void close() throws IOException {
        Path path = config.getDir().resolve(FILE_NAME);
        Path temp = config.getDir().resolve(TEMP_NAME);
        Files.deleteIfExists(temp);
        ByteBuffer forLength = ByteBuffer.allocate(Integer.BYTES);
        try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            for (Record record : storage.values()) {
                write(record.getKey(), channel, forLength);
                write(record.getValue(), channel, forLength);
            }
            channel.force(false);
        }
        if (nmap != null) {
            try {
                CLEAN.invoke(null, nmap);
            } catch (InvocationTargetException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        Files.deleteIfExists(path);
        Files.move(temp, path, StandardCopyOption.ATOMIC_MOVE);
    }

    private void write(ByteBuffer value, WritableByteChannel channel, ByteBuffer forLength) throws IOException {
        forLength.position(0);
        forLength.putInt(value.remaining());
        forLength.position(0);
        channel.write(forLength);
        channel.write(value);
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
