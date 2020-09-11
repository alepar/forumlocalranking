package io.alepar.flocal.download

import UserBoardPosts
import UserBoardRating
import UserProfile
import UserRating
import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.flowables.ConnectableFlowable
import io.reactivex.rxjava3.kotlin.subscribeBy
import io.reactivex.rxjava3.schedulers.Schedulers
import org.apache.parquet.hadoop.ParquetWriter
import org.jsoup.Jsoup
import org.jsoup.nodes.Document

fun createFetchUserListFlowable(progress: FlowProgress): Flowable<String> {
    val urls = userListUrls()
    progress.max = urls.size

    return Flowable.fromIterable(urls)
            .parallel(concurrency)

            .runOn(Schedulers.io())
            .map(::download)
            .doOnNext { progress.increment() }

            .runOn(Schedulers.computation())
            .flatMapIterable(::extractUserNames)

            .sequential()
            .doOnComplete { progress.done = true }
            .doOnError { log.error("User List flowable error", it) }
}

fun publishRatingsPageFlowable(userNames: Set<String>): ConnectableFlowable<UserContext<Document>> {
    return Flowable.fromIterable(userNames)
            .map { UserContext(it, "https://forumlocal.ru/ratingdetails.php?full_users=1&showlite=&username=${urlEncode(it)}") }

            .parallel(concurrency)
            .runOn(Schedulers.io())
            .map { it.transform(::download) }
            
            .runOn(Schedulers.computation())
            .map { ctx -> ctx.transform { Jsoup.parse(it) }}

            .sequential().publish()
}

fun writeUserBoardStats(userNames: Set<String>, writer: ParquetWriter<UserBoardPosts>, progress: FlowProgress) {
    Flowable.fromIterable(userNames)
            .map { UserContext(it, "https://forumlocal.ru/userstats.php?arch=1&Username=${urlEncode(it)}") }

            .parallel(concurrency)
            .runOn(Schedulers.io())
            .map { it.transform(::download) }
            .doOnNext { progress.increment() }

            .runOn(Schedulers.computation())
            .flatMapIterable(::extractUserBoardStats)

            .sequential().observeOn(Schedulers.single())
            .subscribeBy(
                    onNext = { writer.write(it) },
                    onComplete = {
                        swallow { writer.close() }
                        progress.done = true
                    },
                    onError = { log.error("User Board Stats flowable error", it) }
            )
}

fun writeUserProfiles(userNames: Set<String>, writer: ParquetWriter<UserProfile>, progress: FlowProgress) {
    Flowable.fromIterable(userNames)
            .map { UserContext(it, "https://forumlocal.ru/showprofile.php?User=${urlEncode(it)}") }

            .parallel(concurrency)
            .runOn(Schedulers.io())
            .map { it.transform(::download) }
            .doOnNext { progress.increment() }

            .runOn(Schedulers.computation())
            .map(::extractUserProfile)

            .sequential().observeOn(Schedulers.single())
            .subscribeBy(
                    onNext = { writer.write(it) },
                    onComplete = {
                        swallow { writer.close() }
                        progress.done = true
                    },
                    onError = { log.error("User Profiles flowable error", it) }
            )
}

fun writeUserToUserRatings(userRatingsFlowable: ConnectableFlowable<UserContext<Document>>, writer: ParquetWriter<UserRating>, progress: FlowProgress) {
    userRatingsFlowable
            .observeOn(Schedulers.computation())
            .doOnNext { progress.increment() }
            .flatMapIterable(::extractUserRatings)
            .observeOn(Schedulers.single())
            .subscribeBy(
                    onNext = { writer.write(it) },
                    onComplete = {
                        swallow { writer.close() }
                        progress.done = true
                    },
                    onError = { log.error("User to User Ratings flowable error", it) }
            )
}

fun writeUserBoardRatings(userRatingsFlowable: ConnectableFlowable<UserContext<Document>>, writer: ParquetWriter<UserBoardRating>, progress: FlowProgress) {
    userRatingsFlowable
            .observeOn(Schedulers.computation())
            .doOnNext { progress.increment() }
            .flatMapIterable(::extractUserBoardRatings)
            .observeOn(Schedulers.single())
            .subscribeBy(
                    onNext = {
                        writer.write(it)
                    },
                    onComplete = {
                        swallow { writer.close() }
                        progress.done = true
                    },
                    onError = { log.error("User Board Ratings flowable error", it) }
            )
}
