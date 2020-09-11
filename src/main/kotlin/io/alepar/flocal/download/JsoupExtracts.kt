package io.alepar.flocal.download

import UserBoardPosts
import UserBoardRating
import UserProfile
import UserRating
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.nodes.Element
import org.jsoup.select.Elements

fun extractUserProfile(ctx: UserContext<String>): UserProfile {
    val doc = Jsoup.parse(ctx.data)

    return UserProfile(
            ctx.name,
            selectValueForRow(doc, "gender"),
            toIntOrZero(selectValueForRow(doc, "сообщений")),
            toIntOrZero(selectValueForRow(doc, "Рейтинг"))
    )
}

fun extractUserBoardStats(ctx: UserContext<String>): Iterable<UserBoardPosts> {
    val stats = ArrayList<UserBoardPosts>()

    val doc = Jsoup.parse(ctx.data)
    val noPostsElements = doc.select("i:containsOwn(No posts)")
    if (noPostsElements.size == 0) {
        val statsBody = singleElement(doc.select("td.tdheader:matchesOwn(^Board$)")).parent().parent()
        val trElements = statsBody.select("tr.lighttable, tr.darktable")
        for (trElement in trElements) {
            val board = singleElement(trElement.select("> td:eq(0)")).text()
            val posts = Integer.valueOf(singleElement(trElement.select("> td:eq(1)")).ownText())
            stats.add(UserBoardPosts(ctx.name, board, posts))
        }
    }

    return stats
}

fun extractUserRatings(ctx: UserContext<Document>): Iterable<UserRating> {
    val ratings = ArrayList<UserRating>()

    val doc = ctx.data
    val byUsersBody = singleElement(doc.select("table td.tdheader:matchesOwn(^User$)")).parent().parent()
    for (trElement in byUsersBody.select("tr.lighttable, tr.darktable")) {
        val tdElements = trElement.select("td")
        val linkElements = tdElements[0].select("a")
        if (linkElements.size == 0) continue  // skip 'Total'

        val rater = singleElement(linkElements).ownText()
        val votes = Integer.valueOf(tdElements[1].ownText())
        val sum = Integer.valueOf(tdElements[2].ownText())

        ratings.add(UserRating(rater, ctx.name, votes, sum))
    }

    return ratings
}

fun extractUserBoardRatings(ctx: UserContext<Document>): Iterable<UserBoardRating> {
    val ratings = ArrayList<UserBoardRating>()

    val doc = ctx.data
    val byBoardBody = singleElement(doc.select("td.tdheader:matchesOwn(^Board$) + td.tdheader:matchesOwn(^Rating$)")).parent().parent()
    for (trElement in byBoardBody.select("tr.lighttable, tr.darktable")) {
        val board = singleElement(trElement.select("> td:eq(0)")).text()
        val rating = Integer.valueOf(singleElement(trElement.select("> td:eq(1)")).ownText())
        ratings.add(UserBoardRating(ctx.name, board, rating))
    }

    return ratings
}

fun extractUserNames(html: String): Iterable<String> =
    Jsoup.parse(html)
            .select("tr.lighttable td:eq(0) a, tr.darktable td:eq(0) a")
            .map { it.text().trim() }

fun singleElement(elements: Elements): Element {
    check(elements.size == 1) { "expected 1 element, but got: " + elements.size }
    return elements.first()
}

private fun toIntOrZero(str: String): Int {
    return try {
        Integer.valueOf(str)
    } catch (e: NumberFormatException) {
        -1
    }
}

private fun selectValueForRow(doc: Document, rowName: String): String {
    return singleElement(
            singleElement(doc.select("table td.darktable:containsOwn($rowName)"))
                    .parent().select("> td:eq(1)")
    ).text()
}

