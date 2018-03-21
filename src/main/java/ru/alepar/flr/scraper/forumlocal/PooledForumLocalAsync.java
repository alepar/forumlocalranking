package ru.alepar.flr.scraper.forumlocal;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import ru.alepar.flr.model.scraped.ScrapedUser;
import ru.alepar.flr.model.scraped.ScrapedUserDetails;
import ru.alepar.flr.model.scraped.ScrapedUserPostsStats;
import ru.alepar.flr.model.scraped.ScrapedUserRatings;
import ru.alepar.flr.scraper.TaskExecutor;

import java.net.URLEncoder;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;

public class PooledForumLocalAsync implements ForumLocalAsync {

    private final TaskExecutor executor;

    public PooledForumLocalAsync(TaskExecutor executor) {
        this.executor = executor;
    }

    @Override
    public ListenableFuture<SortedSet<String>> listAllUsers() {
        final List<String> userListPages = new ArrayList<>();
        for (char i='a'; i<='z'; i++) {
            userListPages.add(""+i);
        }
        userListPages.add("other");

        final List<ListenableFuture<SortedSet<String>>> futures = userListPages.stream()
                .map(page -> executor.submit(http -> {
                    try {
                        return http.get("https://forumlocal.ru/showmembers.php?Cat=&sb=1&b=" + page,
                                emptyMap(), emptyMap(), emptyMap()
                        );
                    } catch (Exception e) {
                        throw new RuntimeException("failed to get userlist page: " + page, e);
                    }
                }))
                .map(f -> Futures.transform(f, html -> {
                    final Document doc = Jsoup.parse(html);
                    final Elements userLinkElements = doc.select("tr.lighttable td:eq(0) a, tr.darktable td:eq(0) a");
                    final SortedSet<String> userNames = new TreeSet<>();
                    for (Element userLink : userLinkElements) {
                        userNames.add(userLink.text().trim());
                    }
                    return userNames;
                }, executor.cpuPool()))
                .collect(Collectors.toList());

        return Futures.transform(Futures.allAsList(futures), listOfSets ->
            listOfSets.stream().flatMap(Collection::stream).collect(Collectors.toCollection(TreeSet::new))
        , executor.cpuPool());
    }

    @Override
    public ListenableFuture<ScrapedUser> scrapeUser(String username) {
        try {
            final String encode = URLEncoder.encode(username, "windows-1251");

            final List<ListenableFuture<String>> futures = Stream.of(
                    "https://forumlocal.ru/ratingdetails.php?full_users=1&showlite=&username=" + encode,
                    "https://forumlocal.ru/userstats.php?arch=1&Username=" + encode,
                    "https://forumlocal.ru/showprofile.php?User=" + encode
                    ).map(url ->
                    executor.submit(http -> {
                        try {
                            return http.get(url, emptyMap(), emptyMap(), emptyMap());
                        } catch (Exception e) {
                            throw new RuntimeException("failed to scrape user rating: " + encode + " //" + url, e);
                        }
                    })
            ).collect(Collectors.toList());

            final ListenableFuture<List<String>> future = Futures.allAsList(futures);
            return Futures.transform(future, list -> {
                final ScrapedUser user = new ScrapedUser();

                final String ratingPage = list.get(0);
                final String statsPage = list.get(1);
                final String detailsPage = list.get(2);

                user.setName(username);
                user.setPostsStats(extractStats(statsPage));
                user.setRatings(extractRatings(ratingPage));
                user.setDetails(extractDetails(detailsPage));

                return user;
            }, executor.cpuPool());
        } catch (Exception e) {
            throw new RuntimeException("failed to scrape user " + username, e);
        }
    }

    private ScrapedUserDetails extractDetails(String html) {
        final ScrapedUserDetails details = new ScrapedUserDetails();

        final Document doc = Jsoup.parse(html);

        details.setTotalPosts(toIntOrZero(selectValueForRow(doc, "сообщений")));
        details.setTotalRating(toIntOrZero(selectValueForRow(doc, "Рейтинг")));
        details.setGender(selectValueForRow(doc, "gender"));

        return details;
    }

    private static int toIntOrZero(String str) {
        try {
            return Integer.valueOf(str);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static String selectValueForRow(Document doc, String rowName) {
        return single(
            single(doc.select("table td.darktable:containsOwn(" + rowName + ")"))
            .parent().select("> td:eq(1)")
        ).text();
    }

    private ScrapedUserRatings extractRatings(String html) {
        final ScrapedUserRatings ratings = new ScrapedUserRatings();
        final Map<String, Long> byUser = new HashMap<>();
        ratings.setByUser(byUser);
        final Map<String, Integer> byBoard = new HashMap<>();
        ratings.setByBoard(byBoard);

        final Document doc = Jsoup.parse(html);

        final Element byUsersBody = single(doc.select("table td.tdheader:matchesOwn(^User$)")).parent().parent();
        for (Element trElement : byUsersBody.select("tr.lighttable, tr.darktable")) {
            final Elements tdElements = trElement.select("td");
            final Elements linkElements = tdElements.get(0).select("a");
            if (linkElements.size() == 0) continue; // skip 'Total'

            final String username = single(linkElements).ownText();

            final int votes = Integer.valueOf(tdElements.get(1).ownText());
            final int sum = Integer.valueOf(tdElements.get(2).ownText());
            byUser.put(username, ScrapedUserRatings.toLong(sum, votes));
        }

        final Element byBoardBody = single(doc.select("td.tdheader:matchesOwn(^Board$) + td.tdheader:matchesOwn(^Rating$)")).parent().parent();
        for (Element trElement : byBoardBody.select("tr.lighttable, tr.darktable")) {
            final String board = single(trElement.select("> td:eq(0)")).text();
            final Integer posts = Integer.valueOf(single(trElement.select("> td:eq(1)")).ownText());

            byBoard.put(board, posts);
        }

        return ratings;
    }

    private static Element single(Elements elements) {
        if (elements.size() != 1) {
            throw new IllegalStateException("expected 1 element, but got: " + elements.size());
        }
        return elements.first();
    }

    private ScrapedUserPostsStats extractStats(String html) {
        final ScrapedUserPostsStats stats = new ScrapedUserPostsStats();
        final Map<String, Integer> byBoard = new HashMap<>();
        stats.setPostsInBoards(byBoard);

        final Document doc = Jsoup.parse(html);
        final Elements noPostsElements = doc.select("i:containsOwn(No posts)");
        if (noPostsElements.size() == 0){
            final Element statsBody = single(doc.select("td.tdheader:matchesOwn(^Board$)")).parent().parent();
            final Elements trElements = statsBody.select("tr.lighttable, tr.darktable");
            for (Element trElement : trElements) {
                final String board = single(trElement.select("> td:eq(0)")).text();
                final Integer posts = Integer.valueOf(single(trElement.select("> td:eq(1)")).ownText());

                byBoard.put(board, posts);
            }
        }

        return stats;
    }
}
