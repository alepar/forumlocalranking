package ru.alepar.flr.scraper;

import ru.alepar.flr.http.HttpClient;

public interface IoTask<T> {

    T call(HttpClient httpClient);

}
