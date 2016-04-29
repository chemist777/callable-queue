package ru.chemist.callablequeue;

import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.*;

public class CallableQueueTest {
    /**
     * Must print 1,2,3.
     * @throws Exception
     */
    @Test
    public void testCallableQueue() throws Exception {
        CallableQueue queue = new CallableQueue(/* backlog */ 3, /* workers */ 2);
        Instant now = Instant.now();

        CompletableFuture<Integer> task3 = queue.submit(now.plus(3, ChronoUnit.SECONDS), () -> 3);
        CompletableFuture<Integer> task1 = queue.submit(now.plus(1, ChronoUnit.SECONDS), () -> 1);
        CompletableFuture<Integer> task2 = queue.submit(now.plus(2, ChronoUnit.SECONDS), () -> 2);

        CompletableFuture[] results = new CompletableFuture[3];

        AtomicInteger ticker = new AtomicInteger();

        results[0] = task3.thenAccept(n -> {
            System.out.println("task 3 completed");
            assertThat(n, is(ticker.incrementAndGet()));
            assertThat(now.plus(3, ChronoUnit.SECONDS).toEpochMilli(), lessThanOrEqualTo(System.currentTimeMillis()));
        });

        results[1] = task1.thenAccept(n -> {
            System.out.println("task 1 completed");
            assertThat(n, is(ticker.incrementAndGet()));
            assertThat(now.plus(1, ChronoUnit.SECONDS).toEpochMilli(), lessThanOrEqualTo(System.currentTimeMillis()));
        });

        results[2] = task2.thenAccept(n -> {
            System.out.println("task 2 completed");
            assertThat(n, is(ticker.incrementAndGet()));
            assertThat(now.plus(2, ChronoUnit.SECONDS).toEpochMilli(), lessThanOrEqualTo(System.currentTimeMillis()));
        });

        queue.startWorkers();

        CompletableFuture.allOf(results).join();
        queue.shutdown();
    }

    /**
     * Must print 1,2,3
     * @throws Exception
     */
    @Test
    public void testOverloaded() throws Exception {
        CallableQueue queue = new CallableQueue(/* backlog */ 1, /* workers */ 1);
        Instant now = Instant.now();

        CompletableFuture<Integer> task3 = queue.submit(now.plus(3, ChronoUnit.SECONDS), () -> 3);
        CompletableFuture<Integer> task1 = queue.submit(now.plus(1, ChronoUnit.SECONDS), () -> 1);
        CompletableFuture<Integer> task2 = queue.submit(now.plus(2, ChronoUnit.SECONDS), () -> 2);

        CompletableFuture[] results = new CompletableFuture[3];

        AtomicInteger ticker = new AtomicInteger();

        results[0] = task3.thenAccept(n -> {
            System.out.println("task 3 completed as planned");
            assertThat(n, is(ticker.incrementAndGet()));
            assertThat(now.plus(3, ChronoUnit.SECONDS).toEpochMilli(), lessThanOrEqualTo(System.currentTimeMillis()));
        });

        results[1] = task1.thenAccept(n -> {
            System.out.println("task 1 completed fast");
            assertThat(n, is(ticker.incrementAndGet()));
            assertThat(now.plus(1, ChronoUnit.SECONDS).toEpochMilli(), greaterThan(System.currentTimeMillis()));
        });

        results[2] = task2.thenAccept(n -> {
            System.out.println("task 2 completed fast");
            assertThat(n, is(ticker.incrementAndGet()));
            assertThat(now.plus(2, ChronoUnit.SECONDS).toEpochMilli(), greaterThan(System.currentTimeMillis()));
        });

        queue.startWorkers();

        CompletableFuture.allOf(results).join();
        queue.shutdown();
    }
}