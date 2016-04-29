package ru.chemist.callablequeue;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class DelayedTask<V> implements Delayed {
    public final Callable<V> callable;
    public final CompletableFuture<V> resultFuture;
    private final long runAtTimeMs;

    public DelayedTask(Callable<V> callable, long runAtTimeMs, CompletableFuture<V> resultFuture) {
        this.callable = callable;
        this.runAtTimeMs = runAtTimeMs;
        this.resultFuture = resultFuture;
    }

    public long getDelay(TimeUnit unit) {
        return unit.convert(runAtTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    public int compareTo(Delayed o) {
        return (int) (getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS));
    }
}
