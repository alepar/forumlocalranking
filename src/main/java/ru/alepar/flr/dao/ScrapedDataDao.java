package ru.alepar.flr.dao;

import ru.alepar.flr.model.scraped.ScrapedUser;

import java.util.stream.Stream;

public interface ScrapedDataDao {
    String getSessionName();
    void markAsComplete();

    void save(ScrapedUser user);
    CloseableStream<ScrapedUser> getUsers();
}
