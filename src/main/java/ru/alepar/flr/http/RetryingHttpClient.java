package ru.alepar.flr.http;

import java.io.IOException;
import java.util.Map;

public class RetryingHttpClient implements HttpClient {

    private final HttpClient delegate;
    private final int retries = 3;

    public RetryingHttpClient(HttpClient delegate) {
        this.delegate = delegate;
    }

    @Override
    public String post(String url, Map<String, String> headers, Map<String, String> cookies, Map<String, String> data) throws IOException {
        Exception exc = null;
        for (int i=0; i<retries; i++) {
            try {
                return delegate.post(url, headers, cookies, data);
            } catch (IOException e) {
                exc = e;
            }
        }

        throw new IOException("tried " + retries + " times, but still failed", exc);
    }

    @Override
    public String get(String url, Map<String, String> headers, Map<String, String> cookies, Map<String, String> data) throws IOException {
        Exception exc = null;
        for (int i=0; i<retries; i++) {
            try {
                return delegate.get(url, headers, cookies, data);
            } catch (IOException e) {
                exc = e;
            }
        }

        throw new IOException("tried " + retries + " times, but still failed", exc);
    }

    @Override
    public byte[] execute(String url, Map<String, String> headers, Map<String, String> cookies, Map<String, String> data) throws IOException {
        Exception exc = null;
        for (int i=0; i<retries; i++) {
            try {
                return delegate.execute(url, headers, cookies, data);
            } catch (IOException e) {
                exc = e;
            }
        }

        throw new IOException("tried " + retries + " times, but still failed", exc);
    }
}