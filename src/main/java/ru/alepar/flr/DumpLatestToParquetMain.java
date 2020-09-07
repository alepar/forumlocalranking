package ru.alepar.flr;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.lmdbjava.Env;
import ru.alepar.flr.dao.CloseableStream;
import ru.alepar.flr.dao.LmdbScrapedSessionsDao;
import ru.alepar.flr.dao.ScrapedDataDao;
import ru.alepar.flr.dao.ScrapedSessionsDao;
import ru.alepar.flr.model.scraped.ScrapedUser;
import ru.alepar.flr.model.scraped.ScrapedUserDetails;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.lmdbjava.Env.create;
import static ru.alepar.flr.model.scraped.ScrapedUserRatings.getSum;
import static ru.alepar.flr.model.scraped.ScrapedUserRatings.getTotal;

public class DumpLatestToParquetMain {

    public static void main(String[] args) throws Exception {
        final Schema usersSchema = loadSchema("src/main/avro/users.avsc");
        final ParquetWriter<GenericData.Record> usersWriter = openParquetFile("users.parquet", usersSchema);

        final Schema userRatingsSchema = loadSchema("src/main/avro/user_ratings.avsc");
        final ParquetWriter<GenericData.Record> userRatingsWriter = openParquetFile("user_ratings.parquet", userRatingsSchema);

        try(Env<ByteBuffer> lmdb = create()
                .setMapSize(1L << 32) // 4GiB
                .setMaxDbs(1)
                .open(new File("lmdb.scraped")))
        {
            final ScrapedSessionsDao sessions = new LmdbScrapedSessionsDao(lmdb);
            final ScrapedDataDao data = sessions.openLatestComplete();
            System.out.println("Opened session: " + data.getSessionName());

            final long startNanos = System.nanoTime();
            try(CloseableStream<ScrapedUser> usersCloseable = data.getUsers()) {
                usersCloseable.stream()
                    .forEach(ratee -> {
                        final String rateeName = ratee.getName();
                        try {
                            ratee.getRatings().getByUser().forEach((raterName, rating) -> {
                                try {
                                    int sum = getSum(rating);
                                    if ("shaller".equals(raterName)) sum /= 3;
                                    int tot = getTotal(rating);

                                    final GenericData.Record record = new GenericData.Record(userRatingsSchema);
                                    record.put("rater", raterName);
                                    record.put("ratee", rateeName);
                                    record.put("total", tot);
                                    record.put("sum", sum);
                                    userRatingsWriter.write(record);
                                } catch (IOException e) {
                                    throw new RuntimeException("failed to dump " + rateeName, e);
                                }
                            });

                            final ScrapedUserDetails details = ratee.getDetails();
                            if (details != null) {
                                final GenericData.Record record = new GenericData.Record(usersSchema);
                                record.put("name", rateeName);
                                record.put("gender", details.getGender());
                                record.put("posts", details.getTotalPosts());
                                record.put("rating", details.getTotalRating());
                                usersWriter.write(record);
                            }
                        } catch (IOException e) {
                            throw new RuntimeException("failed to dump " + rateeName, e);
                        }
                    });
            }

            usersWriter.close();
            userRatingsWriter.close();

            final long endNanos = System.nanoTime();
            System.out.format("Took %.1fs\n", (endNanos-startNanos)/1_000_000_00/10.0);
        }
    }

    public static ParquetWriter<GenericData.Record> openParquetFile(String filePath, Schema schema) throws IOException {
        return AvroParquetWriter.
                <GenericData.Record>builder(new Path(filePath))
                .withSchema(schema)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();
    }

    public static Schema loadSchema(String filename) throws IOException {
        return new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream(filename));
    }

}
