package manager;

import consumer.v4RunnableConsumer;
import producer.v4RunnableProducer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class v5_Manager  {
    public static void main(String[] args) throws InterruptedException {
        BlockingQueue<Message> queue = new ArrayBlockingQueue<>(100);
        int capacity=100;
        int numberOfProducers=5;
        int numberOfConsumers=5;

        ExecutorService executor= Executors.newFixedThreadPool(100);

        List<v4RunnableProducer>producers=new ArrayList<>();
        List<v4RunnableConsumer>consumers=new ArrayList<>();

        for (int i = 0; i < numberOfProducers; i++) {
           v4RunnableProducer p= new v4RunnableProducer(capacity,queue);
           producers.add(p);
           executor.submit(p);

        }

        for(int i=0;i< numberOfConsumers;i++){
           v4RunnableConsumer c=  new v4RunnableConsumer(queue);
           consumers.add(c);
           executor.submit(c);
        }

        Thread.sleep(2000);

        producers.forEach(v4RunnableProducer::finishProducing);
        consumers.forEach(v4RunnableConsumer::finishConsuming);
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

    }
}
