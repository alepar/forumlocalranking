package ru.alepar.flr;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;
import org.lmdbjava.Env;
import static org.lmdbjava.Env.create;
import ru.alepar.flr.dao.CloseableStream;
import ru.alepar.flr.dao.LmdbScrapedSessionsDao;
import ru.alepar.flr.dao.ScrapedDataDao;
import ru.alepar.flr.dao.ScrapedSessionsDao;
import ru.alepar.flr.model.scraped.ScrapedUser;
import ru.alepar.flr.model.scraped.ScrapedUserDetails;
import ru.alepar.flr.model.scraped.ScrapedUserRatings;
import static ru.alepar.flr.model.scraped.ScrapedUserRatings.getSum;
import static ru.alepar.flr.model.scraped.ScrapedUserRatings.getTotal;
import tech.tablesaw.api.CategoryColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.Table;

public class DumpLatestToCsvMain {

    public static void main(String[] args) throws Exception {
        try(Env<ByteBuffer> lmdb = create()
                .setMapSize(1L << 32) // 4GiB
                .setMaxDbs(1)
                .open(new File("lmdb.scraped")))
        {
            final ScrapedSessionsDao sessions = new LmdbScrapedSessionsDao(lmdb);
            final ScrapedDataDao data = sessions.openLatestComplete();
            System.out.println("Opened session: " + data.getSessionName());

            final CategoryColumn uruRater = new CategoryColumn("rater");
            final CategoryColumn uruRatee = new CategoryColumn("ratee");
            final IntColumn uruSum = new IntColumn("sum");
            final IntColumn uruTot = new IntColumn("tot");
            final IntColumn uruRSum = new IntColumn("rsum");
            final IntColumn uruRTot = new IntColumn("rtot");
            final Table ratingsByUserTable = Table.create("user_ratings_byuser",
                    uruRater, uruRatee, uruSum, uruTot, uruRSum, uruRTot
            );

            final CategoryColumn udUser = new CategoryColumn("user");
            final CategoryColumn udGender = new CategoryColumn("gender");
            final IntColumn udPosts = new IntColumn("posts");
            final IntColumn udRating = new IntColumn("rating");
            final Table detailsTable = Table.create("user_details",
                    udUser, udGender, udPosts, udRating
            );

            final long startNanos = System.nanoTime();

            final Map<String, Rating> byUserRatingsIndex = new Object2ObjectOpenHashMap<>(3_000_000);
            try(CloseableStream<ScrapedUser> usersCloseable = data.getUsers()) {
                usersCloseable.stream()
                    .forEach(ratee -> {
                        final String rateeName = ratee.getName();
                        ratee.getRatings().getByUser().forEach((raterName, rating) -> {
                            int sum = getSum(rating);
                            if ("shaller".equals(raterName)) sum /= 3;
                            int tot = getTotal(rating);

                            byUserRatingsIndex.put(
                                    makeKey(raterName, rateeName),
                                    new Rating(raterName, rateeName, sum, tot)
                            );
                        });

                        final ScrapedUserDetails details = ratee.getDetails();
                        if (details != null) {
                            udUser.append(ratee.getName());
                            udGender.append(details.getGender());
                            udPosts.append(details.getTotalPosts());
                            udRating.append(details.getTotalRating());
                        }
                    });
            }

            byUserRatingsIndex.forEach((k, r) -> {
                uruRater.append(r.rater);
                uruRatee.append(r.ratee);
                uruSum.append(r.sum);
                uruTot.append(r.tot);

                final Rating rr = byUserRatingsIndex.get(makeKey(r.ratee, r.rater));
                if (rr != null) {
                    uruRSum.append(rr.sum);
                    uruRTot.append(rr.tot);
                } else {
                    uruRSum.append(0);
                    uruRTot.append(0);

                    uruRater.append(r.ratee);
                    uruRatee.append(r.rater);
                    uruSum.append(0);
                    uruTot.append(0);
                    uruRSum.append(r.sum);
                    uruRTot.append(r.tot);
                }
            });

            final long endNanos = System.nanoTime();
            System.out.format("Took %.1fs\n", (endNanos-startNanos)/1_000_000_00/10.0);

            ratingsByUserTable.write().csv("user_ratings_byuser.csv");
            detailsTable.write().csv("user_details.csv");
        }
    }

    private static String makeKey(String src, String dst) {
        return src + "=>" + dst;
    }

    private static class Rating {
        private final String rater;
        private final String ratee;
        private final int sum;
        private final int tot;

        private Rating(String rater, String ratee, int sum, int tot) {
            this.rater = rater;
            this.ratee = ratee;
            this.sum = sum;
            this.tot = tot;
        }
    }
}
