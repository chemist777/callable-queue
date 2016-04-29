package ru.chemist.callablequeue;

import java.time.Instant;
import java.util.concurrent.*;

/**
 * Class are thread safe.
 */
public class CallableQueue {

    private final DelayQueue<DelayedTask> queue = new DelayQueue<>();
    private final int backlog;
    private final Thread[] threads;

    /**
     *
     * @param backlog maximum queue size for normal processing
     * @param workerThreads worker threads count
     */
    public CallableQueue(int backlog, int workerThreads) {
        this.backlog = backlog;
        this.threads = new Thread[workerThreads];
        for(int i=0;i<workerThreads;i++) {
            this.threads[i] = new WorkerThread();
        }
    }

    public void startWorkers() {
        for(int i=0;i<threads.length;i++) {
            this.threads[i].start();
        }
    }

    /**
     * Submit new job to queue
     *
     * @param dateTime
     * @param callable
     * @param <V>
     * @return
     */
    public <V> CompletableFuture<V> submit(Instant dateTime, Callable<V> callable) {
        CompletableFuture<V> future = new CompletableFuture<>();
        queue.add(new DelayedTask<>(callable, dateTime.toEpochMilli(), future));
        return future;
    }

    public void shutdown() throws InterruptedException {
        for (Thread thread : threads) {
            thread.interrupt();
        }
        for (Thread thread : threads) {
            thread.join();
        }
    }


    private class WorkerThread extends Thread {
        @Override
        public void run() {
            try {
                for (;;) {
                    DelayedTask task;
                    if (queue.size() > backlog) {
                        task = queue.peek();
                        if (!queue.remove(task)) task = null; //already removed by other thread
                    } else {
                        task = queue.poll(1000, TimeUnit.MILLISECONDS);
                    }
                    if (task != null) {
                        try {
                            //noinspection unchecked
                            task.resultFuture.complete(task.callable.call());
                        } catch (Exception e) {
                            task.resultFuture.completeExceptionally(e);
                        }
                    }
                }
            } catch (InterruptedException e) {
                //quit
            }
        }
    }
}
