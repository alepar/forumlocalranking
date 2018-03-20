package ru.alepar.flr.http;

import java.io.IOException;
import java.util.Map;

public interface HttpClient {
    String post(String url, Map<String, String> headers, Map<String, String> cookies, Map<String, String> data) throws IOException;

    String get(String url, Map<String, String> headers, Map<String, String> cookies, Map<String, String> data) throws IOException;

    byte[] execute(String url, Map<String, String> headers, Map<String, String> cookies, Map<String, String> data) throws IOException;
}