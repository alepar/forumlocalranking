package io.alepar.flocal.download

import UserBoardStats
import UserProfile
import UserRating
import io.alepar.parquet.createParquetFile
import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.kotlin.subscribeBy
import io.reactivex.rxjava3.schedulers.Schedulers
import org.asynchttpclient.AsyncHttpClient
import org.asynchttpclient.Dsl
import org.slf4j.LoggerFactory
import java.net.URLEncoder
import java.util.concurrent.Semaphore

const val concurrency = 40

val log = LoggerFactory.getLogger("io.alepar.flocal.download.DownloadToParquet")

fun main() {
    //TODO user per board rating is from rating stats

    val userRatings = createParquetFile("user_ratings.parquet", UserRating::class)
    val userProfiles = createParquetFile("user_profiles.parquet", UserProfile::class)
    val userBoardStats = createParquetFile("user_board_stats.parquet", UserBoardStats::class)

    val progress = GlobalProgress {
        swallow { Schedulers.shutdown() }
        swallow { httpClient.close() }
    }

    Thread {
        val urls = userListUrls()
        progress.userListProgress.max = urls.size
        val userNames = Flowable.fromIterable(urls)
                .parallel(concurrency)

                .runOn(Schedulers.io())
                .map(::download)
                .doOnNext { progress.userListProgress.increment() }

                .runOn(Schedulers.computation())
                .flatMapIterable(::extractUserNames)

                .sequential()
                .doOnComplete { progress.userListProgress.done = true }
                .doOnError { log.error("User List flowable error", it) }
                .toList().blockingGet().toSet()

        progress.userRatingsProgress.max = userNames.size
        Flowable.fromIterable(userNames)
                .map { UserContext(it, "https://forumlocal.ru/ratingdetails.php?full_users=1&showlite=&username=${urlEncode(it)}") }

                .parallel(concurrency)
                .runOn(Schedulers.io())
                .map { it.transform(::download) }
                .doOnNext { progress.userRatingsProgress.increment() }

                .runOn(Schedulers.computation())
                .flatMapIterable(::extractUserRatings)

                .sequential().observeOn(Schedulers.single())
                .subscribeBy(
                        onNext = { userRatings.write(it) },
                        onComplete = {
                            swallow { userRatings.close() }
                            progress.userRatingsProgress.done = true
                        },
                        onError = { log.error("User Ratings flowable error", it) }
                )

        progress.userProfilesProgress.max = userNames.size
        Flowable.fromIterable(userNames)
                .map { UserContext(it, "https://forumlocal.ru/showprofile.php?User=${urlEncode(it)}") }

                .parallel(concurrency)
                .runOn(Schedulers.io())
                .map { it.transform(::download) }
                .doOnNext { progress.userProfilesProgress.increment() }

                .runOn(Schedulers.computation())
                .map(::extractUserProfile)

                .sequential().observeOn(Schedulers.single())
                .subscribeBy(
                        onNext = { userProfiles.write(it) },
                        onComplete = {
                            swallow { userProfiles.close() }
                            progress.userProfilesProgress.done = true
                        },
                        onError = { log.error("User Profiles flowable error", it) }
                )

        progress.userBoardStatsProgress.max = userNames.size
        Flowable.fromIterable(userNames)
                .map { UserContext(it, "https://forumlocal.ru/userstats.php?arch=1&Username=${urlEncode(it)}") }

                .parallel(concurrency)
                .runOn(Schedulers.io())
                .map { it.transform(::download) }
                .doOnNext { progress.userBoardStatsProgress.increment() }

                .runOn(Schedulers.computation())
                .flatMapIterable(::extractUserBoardStats)

                .sequential().observeOn(Schedulers.single())
                .subscribeBy(
                        onNext = { userBoardStats.write(it) },
                        onComplete = {
                            swallow { userBoardStats.close() }
                            progress.userBoardStatsProgress.done = true
                        },
                        onError = { log.error("User Board Stats flowable error", it) }
                )
    }.start()

    LanternaUI(progress).watch()
}

fun swallow(function: () -> Unit) {
    try {
        function.invoke()
    } catch (e: Exception) {
        log.warn("swallowed", e)
    }
}

class UserContext<T>(val name: String, val data: T) {
    fun <R> transform(f: (T) -> R): UserContext<R> =
            UserContext(name, f.invoke(data))
}

fun urlEncode(str: String): String =
    URLEncoder.encode(str, "windows-1251")

fun userListUrls(): List<String> {
    val urls = ArrayList<String>()

    for (i in 'a'..'z') {
        urls.add(i.toString())
    }
    urls.add("other")

    return urls.map { str -> "https://forumlocal.ru/showmembers.php?Cat=&sb=1&b=${str}" }
}

val httpClient: AsyncHttpClient = Dsl.asyncHttpClient(Dsl.config()
        .setMaxRequestRetry(3)
)
val httpSemaphore = Semaphore(concurrency)
fun download(url: String): String {
    httpSemaphore.acquire()
    try {
        return httpClient.prepareGet(url).execute().get().responseBody
    } finally {
        httpSemaphore.release()
    }
}
