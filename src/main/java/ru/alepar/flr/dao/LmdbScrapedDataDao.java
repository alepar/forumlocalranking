package ru.alepar.flr.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.lmdbjava.*;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import ru.alepar.flr.model.scraped.ScrapedUser;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.stream.StreamSupport;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;

public class LmdbScrapedDataDao implements ScrapedDataDao {

    private static final ByteBuffer completeValue = (ByteBuffer) allocateDirect(1).put((byte) 1).flip();

    private static final ByteBuffer dataStartKey = (ByteBuffer) allocateDirect(1).put((byte)1).flip();
    private static final ByteBuffer dataEndKey = (ByteBuffer) allocateDirect(1).put((byte)2).flip();


    private final ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());

    private final Env<ByteBuffer> lmdb;
    private final Dbi<ByteBuffer> dbi;
    private final String name;

    public LmdbScrapedDataDao(Env<ByteBuffer> lmdb, Dbi<ByteBuffer> dbi, String session) {
        this.lmdb = lmdb;
        this.dbi = dbi;
        this.name = session;
    }

    @Override
    public void save(ScrapedUser user) {
        try {
            final byte[] userBytes = user.getName().getBytes(UTF_8);
            final byte[] valueBytes = mapper.writeValueAsBytes(user);

            final ByteBuffer key = allocateDirect(1 + userBytes.length);
            key.put((byte) 1);
            key.put(userBytes);
            key.flip();

            try(Txn<ByteBuffer> wtx = lmdb.txnWrite()) {
                final ByteBuffer value = dbi.reserve(wtx, key, valueBytes.length);
                value.put(valueBytes);
                value.flip();
                wtx.commit();
            }
        } catch (Exception e) {
            throw new RuntimeException("failed to save user " + user.getName(), e);
        }
    }

    @Override
    public void markAsComplete() {
        final byte[] sessBytes = name.getBytes(UTF_8);
        final ByteBuffer key = allocateDirect(sessBytes.length+1);
        key.put((byte)0).put(sessBytes).flip();
        dbi.put(key, completeValue);

        final ByteBuffer byteBuffer = dbi.get(lmdb.txnRead(), key);
    }

    @Override
    public CloseableStream<ScrapedUser> getUsers() {
        final Txn<ByteBuffer> rtx = lmdb.txnRead();
        try {
            final CursorIterator<ByteBuffer> cursor = dbi.iterate(rtx, KeyRange.open(dataStartKey, dataEndKey));
            return new LmdbCloseableStream<>(
                    StreamSupport.stream(cursor.iterable().spliterator(), false)
                    .map(kv -> {
                        try {
                            final InputStream is = new ByteBufferBackedInputStream(kv.val());
                            return mapper.readValue(is, ScrapedUser.class);
                        } catch (Exception e) {
                            throw new RuntimeException("failed to read user", e);
                        }
                    }),
                    rtx
            );
        } catch (Exception e) {
            rtx.close();
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getName() {
        return name;
    }
}
