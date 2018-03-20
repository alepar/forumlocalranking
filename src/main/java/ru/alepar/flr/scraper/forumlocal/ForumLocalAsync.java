package ru.alepar.flr.scraper.forumlocal;

import com.google.common.util.concurrent.ListenableFuture;
import ru.alepar.flr.model.scraped.ScrapedUser;

import java.util.SortedSet;

public interface ForumLocalAsync {

    ListenableFuture<SortedSet<String>> listAllUsers();
    ListenableFuture<ScrapedUser> scrapeUser(String username);

}
