package consumer;

import manager.Message;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class v4RunnableConsumer implements Runnable {

    private final BlockingQueue<Message> queue;
    private volatile boolean consuming = true;
    private static final AtomicInteger counter=new AtomicInteger(0);

    public v4RunnableConsumer(BlockingQueue<Message> queue) {
        this.queue = queue;
    }

    public void finishConsuming() {
        consuming = false;
        synchronized (queue) {
            queue.notifyAll();
        }
    }


    @Override
    public void run() {
        while (true) {
            try {
                Message msg = queue.take();
                process(msg);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void process(Message m) {
        Integer n=(counter.incrementAndGet());
      System.out.println("This is a Message to the Channel "+" Message-"+n+" This was Message "+m.getData()+" with following id "+m.getId());
    }
}