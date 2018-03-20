package ru.alepar.flr.dao;

import java.util.SortedSet;

public interface ScrapedSessionsDao {

    SortedSet<String> list();
    ScrapedDataDao createOrOpen(String session);
    ScrapedDataDao open(String session);
    ScrapedDataDao create(String session);
    ScrapedDataDao openLatestComplete();

}
