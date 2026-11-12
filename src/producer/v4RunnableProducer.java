package producer;

import manager.Message;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class v4RunnableProducer implements Runnable {
    BlockingQueue<Message> queue;
    private final int capacity;
    private volatile boolean producing = true;
    private static AtomicInteger counter=new AtomicInteger(0);

    public v4RunnableProducer(int capacity, BlockingQueue<Message> queue) {
        this.capacity = capacity;
        this.queue = queue;
    }

    public void finishProducing() {
        producing = false;
        synchronized (queue) {
            queue.notifyAll();
        }
    }

    @Override
    public void run() {
        while (producing) {
            synchronized (queue) {
                try {
                    while (capacity <= queue.size()) {
                        queue.wait();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            synchronized (queue) {
                int id=counter.incrementAndGet();
                queue.add(new Message(id, " Das ist eine Nachricht " + id));
                queue.notifyAll();
            }


        }

    }
}
