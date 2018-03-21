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

    private final ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());

    private final Env<ByteBuffer> lmdb;
    private final Dbi<ByteBuffer> dbi;
    private final String sessionName;
    private final byte[] sessionNameBytes;

    public LmdbScrapedDataDao(Env<ByteBuffer> lmdb, Dbi<ByteBuffer> dbi, String sessionName) {
        this.lmdb = lmdb;
        this.dbi = dbi;
        this.sessionName = sessionName;
        this.sessionNameBytes = this.sessionName.getBytes(UTF_8);
    }

    @Override
    public void save(ScrapedUser user) {
        try {
            final byte[] userBytes = user.getName().getBytes(UTF_8);
            final byte[] valueBytes = mapper.writeValueAsBytes(user);

            final ByteBuffer key = allocateDirect(2 + userBytes.length + sessionNameBytes.length);
            key.put((byte) 1);
            key.put(sessionNameBytes);
            key.put((byte) 0);
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
        final ByteBuffer key = allocateDirect(sessionNameBytes.length+1);
        key.put((byte)0).put(sessionNameBytes).flip();
        dbi.put(key, completeValue);
    }

    @Override
    public CloseableStream<ScrapedUser> getUsers() {
        final ByteBuffer dataStartKey = (ByteBuffer) allocateDirect(2 + sessionNameBytes.length)
                .put((byte)1).put(sessionNameBytes).put((byte)0)
                .flip();
        final ByteBuffer dataEndKey = (ByteBuffer) allocateDirect(2 + sessionNameBytes.length)
                .put((byte)1).put(sessionNameBytes).put((byte)1)
                .flip();

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
    public String getSessionName() {
        return sessionName;
    }
}
