package ru.alepar.flr;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.lmdbjava.Env;
import ru.alepar.flr.dao.LmdbScrapedSessionsDao;
import ru.alepar.flr.dao.ScrapedDataDao;
import ru.alepar.flr.dao.ScrapedSessionsDao;
import ru.alepar.flr.http.HttpClient;
import ru.alepar.flr.http.JsoupHttpClient;
import ru.alepar.flr.model.scraped.ScrapedUser;
import ru.alepar.flr.scraper.TaskExecutor;
import ru.alepar.flr.scraper.forumlocal.ForumLocalAsync;
import ru.alepar.flr.scraper.forumlocal.PooledForumLocalAsync;

import java.io.File;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.lmdbjava.Env.create;

public class ScrapeForumlocal {

    public static void main(String[] args) throws Exception {
        try(Env<ByteBuffer> lmdb = create()
                .setMapSize(1L << 32) // 4GiB
                .setMaxDbs(1)
                .open(createDirIfDoesntExistAlready("lmdb.scraped")))
        {
            final ScrapedSessionsDao sessions = new LmdbScrapedSessionsDao(lmdb);

            final String session = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss ZZZZZ").format(new Date());
            System.out.println("creating session " + session);
            final ScrapedDataDao data = sessions.create(session);

            final HttpClient http = new JsoupHttpClient();
            final ExecutorService ioPool = newFixedThreadPool(10);
            final ExecutorService cpuPool = newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
            try (TaskExecutor executor = new TaskExecutor(http, ioPool, cpuPool)) {
                final AtomicInteger usersCount = new AtomicInteger();
                final ForumLocalAsync forumLocalAsync = new PooledForumLocalAsync(executor);
                final ListenableFuture<SortedSet<String>> usernamesFuture = forumLocalAsync.listAllUsers();
                usernamesFuture.addListener(() -> {
                    try {
                        usersCount.set(usernamesFuture.get().size());
                    } catch (Exception ignored) {}
                }, cpuPool);
                final List<ListenableFuture<ScrapedUser>> scrapedUsers = Futures.transform(usernamesFuture, usernames ->
                        Futures.inCompletionOrder(usernames.stream().map(forumLocalAsync::scrapeUser).collect(Collectors.toList()))
                , cpuPool).get();

                int savedUsersCount = 0;
                for (ListenableFuture<ScrapedUser> userFuture : scrapedUsers) {
                    final ScrapedUser user = userFuture.get();
                    data.save(user);
                    savedUsersCount++;
                    final int localUsersCount = usersCount.get();
                    System.out.format("saved %3.1f%% (%d/%d)\n", 100.0/localUsersCount*savedUsersCount, savedUsersCount, localUsersCount);
                }
                System.out.println("Done");
                data.markAsComplete();
            }
        }
    }

    private static File createDirIfDoesntExistAlready(String path) {
        final File lmdbPath = new File(path);
        if (!lmdbPath.isDirectory()) {
            if (lmdbPath.exists()) {
                throw new IllegalStateException(path + " exists, but not a directory");
            } else {
                if (!lmdbPath.mkdirs()) {
                    throw new IllegalStateException("could not create " + path + " dir");
                }
            }
        }
        return lmdbPath;
    }
}
