package ru.alepar.flr.dao;

import java.util.stream.Stream;

public interface CloseableStream<T> extends AutoCloseable {
    Stream<T> stream();

    @Override
    void close();
}
