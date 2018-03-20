import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import java.io.File;
import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;

public class LmdbTutorialMain {

    private static final String DB_NAME = "my DB";

    public static void main(String[] args) {
        final File path = new File("lmdb.data");
        path.mkdirs();

        final Env<ByteBuffer> env = create()
                // LMDB also needs to know how large our DB might be. Over-estimating is OK.
                .setMapSize(10_485_760)
                // LMDB also needs to know how many DBs (Dbi) we want to store in this Env.
                .setMaxDbs(1)
                // Now let's open the Env. The same path can be concurrently opened and
                // used in different processes, but do not open the same path twice in
                // the same process at the same time.
                .open(path);

        // We need a Dbi for each DB. A Dbi roughly equates to a sorted map. The
        // MDB_CREATE flag causes the DB to be created if it doesn't already exist.
        final Dbi<ByteBuffer> db = env.openDbi(DB_NAME, MDB_CREATE);

        // We want to store some data, so we will need a direct ByteBuffer.
        // Note that LMDB keys cannot exceed maxKeySize bytes (511 bytes by default).
        // Values can be larger.
        final ByteBuffer key = allocateDirect(env.getMaxKeySize());
        final ByteBuffer val = allocateDirect(700);

        key.put("greeting".getBytes(UTF_8)).flip();
        val.put("Hello world".getBytes(UTF_8)).flip();
        final int valSize = val.remaining();

        // Now store it. Dbi.put() internally begins and commits a transaction (Txn).
        db.put(key, val);

        // To fetch any data from LMDB we need a Txn. A Txn is very important in
        // LmdbJava because it offers ACID characteristics and internally holds a
        // read-only key buffer and read-only value buffer. These read-only buffers
        // are always the same two Java objects, but point to different LMDB-managed
        // memory as we use Dbi (and Cursor) methods. These read-only buffers remain
        // valid only until the Txn is released or the next Dbi or Cursor call. If
        // you need data afterwards, you should copy the bytes to your own buffer.
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            final ByteBuffer found = db.get(txn, key);
            if (found == null) throw new IllegalStateException();

            // The fetchedVal is read-only and points to LMDB memory
            final ByteBuffer fetchedVal = txn.val();
            if (fetchedVal.remaining() != valSize) throw new IllegalStateException();

            // Let's double-check the fetched value is correct
            if (!UTF_8.decode(fetchedVal).toString().equals("Hello world")) throw new IllegalStateException();
        }

        // We can also delete. The simplest way is to let Dbi allocate a new Txn...
        db.delete(key);

        // Now if we try to fetch the deleted row, it won't be present
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            if(db.get(txn, key) != null) throw new IllegalStateException();
        }
    }

}
