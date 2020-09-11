package io.alepar.flocal.download

import UserBoardPosts
import UserBoardRating
import UserProfile
import UserRating
import io.alepar.parquet.createParquetFile
import io.reactivex.rxjava3.schedulers.Schedulers
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.URLEncoder

const val concurrency = 40

val log: Logger = LoggerFactory.getLogger("io.alepar.flocal.download.DownloadToParquet")

fun main() {
    val userRatings = createParquetFile("user_ratings.parquet", UserRating::class)
    val userProfiles = createParquetFile("user_profiles.parquet", UserProfile::class)
    val userBoardPosts = createParquetFile("user_board_posts.parquet", UserBoardPosts::class)
    val userBoardRatings = createParquetFile("user_board_ratings.parquet", UserBoardRating::class)

    val progress = GlobalProgress {
        swallow { Schedulers.shutdown() }
        swallow { httpClient.close() }
    }

    Thread {
        val userNames = createFetchUserListFlowable(progress.userListProgress)
                .toList().blockingGet().toSet()

        progress.userRatingsProgress.max = userNames.size
        progress.userProfilesProgress.max = userNames.size
        progress.userBoardPostsProgress.max = userNames.size
        progress.userBoardRatingsProgress.max = userNames.size

        val userRatingsFlowable = publishRatingsPageFlowable(userNames)
        writeUserToUserRatings(userRatingsFlowable, userRatings, progress.userRatingsProgress)
        writeUserBoardRatings(userRatingsFlowable, userBoardRatings, progress.userBoardRatingsProgress)
        userRatingsFlowable.connect()

        writeUserProfiles(userNames, userProfiles, progress.userProfilesProgress)
        writeUserBoardStats(userNames, userBoardPosts, progress.userBoardPostsProgress)
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

