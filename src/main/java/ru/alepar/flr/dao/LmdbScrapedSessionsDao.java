package ru.alepar.flr.dao;

import org.lmdbjava.*;

import java.nio.ByteBuffer;
import java.util.SortedSet;
import java.util.TreeSet;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.lmdbjava.DbiFlags.MDB_CREATE;

public class LmdbScrapedSessionsDao implements ScrapedSessionsDao {

    private static final ByteBuffer uncomplete = (ByteBuffer) allocateDirect(1).put((byte) 0).flip();

    private static final ByteBuffer listStart = (ByteBuffer) allocateDirect(1).put((byte)0).flip();
    private static final ByteBuffer listEnd = (ByteBuffer) allocateDirect(1).put((byte)1).flip();

    private final Env<ByteBuffer> lmdb;
    private final Dbi<ByteBuffer> dbi;

    public LmdbScrapedSessionsDao(Env<ByteBuffer> lmdb) {
        this.lmdb = lmdb;
        this.dbi = lmdb.openDbi("scraped", MDB_CREATE);
    }

    @Override
    public SortedSet<String> list() {
        final SortedSet<String> list = new TreeSet<>();

        try (Txn<ByteBuffer> rtx = lmdb.txnRead()) {
            try(CursorIterable<ByteBuffer> cursor = dbi.iterate(rtx, KeyRange.open(listStart, listEnd))) {
                for (CursorIterable.KeyVal<ByteBuffer> keyval : cursor) {
                    final ByteBuffer key = keyval.key();
                    key.position(1);
                    final byte[] buf = new byte[key.remaining()];
                    key.get(buf);
                    list.add(new String(buf, UTF_8));
                }
            }
        }

        return list;
    }

    @Override
    public LmdbScrapedDataDao open(String session) {
        return new LmdbScrapedDataDao(lmdb, dbi, session);
    }

    @Override
    public ScrapedDataDao createOrOpen(String session) {
        if (!list().contains(session)) {
            return create(session);
        }

        return open(session);
    }

    @Override
    public ScrapedDataDao create(String session) {
        final byte[] sessBytes = session.getBytes(UTF_8);
        final ByteBuffer key = allocateDirect(sessBytes.length+1);
        key.put((byte)0).put(sessBytes).flip();
        dbi.put(key, uncomplete);
        return open(session);
    }

    @Override
    public ScrapedDataDao openLatestComplete() {
        try (Txn<ByteBuffer> rtx = lmdb.txnRead()) {
            try(CursorIterable<ByteBuffer> cursor = dbi.iterate(rtx, KeyRange.openBackward(listEnd, listStart))) {
                for (CursorIterable.KeyVal<ByteBuffer> kv : cursor) {
                    if (kv.val().remaining() > 0 && kv.val().get() == (byte)1) {
                        final ByteBuffer key = kv.key();
                        key.position(1);

                        final byte[] buf = new byte[key.remaining()];
                        key.get(buf);

                        return open(new String(buf, UTF_8));
                    }
                }
            }
        }

        throw new IllegalStateException("no completed sessions found");
    }
}
