package ru.alepar.flr.http;

import org.jsoup.Connection;
import org.jsoup.Jsoup;

import java.io.IOException;
import java.util.Map;

public class JsoupHttpClient implements HttpClient {

    @Override
    public String post(String url, Map<String, String> headers, Map<String, String> cookies, Map<String, String> data) throws IOException {
        final Connection conn = Jsoup.connect(url);

        for (Map.Entry<String, String> e : cookies.entrySet()) {
            conn.cookie(e.getKey(), e.getValue());
        }

        for (Map.Entry<String, String> e : data.entrySet()) {
            conn.data(e.getKey(), e.getValue());
        }

        return conn.post().toString();
    }

    @Override
    public String get(String url, Map<String, String> headers, Map<String, String> cookies, Map<String, String> data) throws IOException {
        final Connection conn = Jsoup.connect(url);

        for (Map.Entry<String, String> e : cookies.entrySet()) {
            conn.cookie(e.getKey(), e.getValue());
        }

        for (Map.Entry<String, String> e : data.entrySet()) {
            conn.data(e.getKey(), e.getValue());
        }

        return conn.get().toString();
    }

    @Override
    public byte[] execute(String url, Map<String, String> headers, Map<String, String> cookies, Map<String, String> data) throws IOException {
        final Connection conn = Jsoup.connect(url);

        for (Map.Entry<String, String> e : cookies.entrySet()) {
            conn.cookie(e.getKey(), e.getValue());
        }

        for (Map.Entry<String, String> e : data.entrySet()) {
            conn.data(e.getKey(), e.getValue());
        }

        final Connection.Response response = conn.ignoreContentType(true).execute();
        if (response.statusCode() != 200) {
            throw new RuntimeException("http server returned error: " + response.statusCode() + " - " + response.statusMessage());
        }

        return response.bodyAsBytes();
    }
}