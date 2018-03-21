package ru.alepar.flr.dao;

import org.lmdbjava.Txn;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.BaseStream;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class LmdbCloseableStream<T> implements CloseableStream<T> {

    private final Stream<T> delegate;
    private final Txn<?> txn;

    public LmdbCloseableStream(Stream<T> delegate, Txn<?> txn) {
        this.delegate = delegate;
        this.txn = txn;
    }

    @Override
    public void close() {
        txn.close();
    }

    @Override
    public Stream<T> stream() {
        return delegate;
    }
}
