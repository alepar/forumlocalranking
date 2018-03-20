package ru.alepar.flr;

import org.lmdbjava.Env;
import ru.alepar.flr.dao.CloseableStream;
import ru.alepar.flr.dao.LmdbScrapedSessionsDao;
import ru.alepar.flr.dao.ScrapedDataDao;
import ru.alepar.flr.dao.ScrapedSessionsDao;
import ru.alepar.flr.model.scraped.ScrapedUser;

import java.io.File;
import java.nio.ByteBuffer;

import static org.lmdbjava.Env.create;

public class ExtractTransformScrapedMain {

    public static void main(String[] args) {
        try(Env<ByteBuffer> lmdb = create()
                .setMapSize(1L << 32) // 4GiB
                .setMaxDbs(1)
                .open(new File("lmdb.scraped")))
        {
            final ScrapedSessionsDao sessions = new LmdbScrapedSessionsDao(lmdb);
            final ScrapedDataDao data = sessions.openLatestComplete();
            System.out.println("Opened session: " + data.getName());

            try(CloseableStream<ScrapedUser> usersCloseable = data.getUsers()) {
                System.out.println("Total: " + usersCloseable.stream().peek(u -> System.out.println(u.getName())).count());
            }
        }
    }

}
