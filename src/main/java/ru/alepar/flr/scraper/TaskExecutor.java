package ru.alepar.flr.scraper;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import ru.alepar.flr.http.HttpClient;

import java.util.concurrent.ExecutorService;

public class TaskExecutor implements AutoCloseable {

    private final HttpClient client;
    private final ListeningExecutorService ioPool;
    private final ListeningExecutorService cpuPool;

    public TaskExecutor(HttpClient client, ExecutorService ioPool, ExecutorService cpuPool) {
        this.client = client;
        this.ioPool = MoreExecutors.listeningDecorator(ioPool);
        this.cpuPool = MoreExecutors.listeningDecorator(cpuPool);
    }

    public <T> ListenableFuture<T> submit(IoTask<T> task) {
        return ioPool.submit(() -> task.call(client));
    }

    public ListeningExecutorService cpuPool() {
        return cpuPool;
    }
    public ListeningExecutorService ioPool() {
        return ioPool;
    }

    @Override
    public void close() {
        ioPool.shutdown();
        cpuPool.shutdown();
    }
}
