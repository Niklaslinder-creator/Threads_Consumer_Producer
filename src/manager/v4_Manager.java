package manager;

import consumer.v4RunnableConsumer;
import producer.v4RunnableProducer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class v4_Manager {
    public static void main (String[] args)throws InterruptedException{
        BlockingQueue<Message> queue = new ArrayBlockingQueue<>(100);
        List<v4RunnableConsumer>Consumers= new ArrayList<>();
        List<v4RunnableProducer>Producers= new ArrayList<>();

        for (int i = 0; i < 200; i++) {
            Producers.add(new v4RunnableProducer(100,queue));
            Consumers.add(new v4RunnableConsumer(queue));
        }

        for(v4RunnableProducer producer:Producers){
            new Thread(producer).start();
        }
        for(v4RunnableConsumer consumer:Consumers){
            new Thread(consumer).start();
        }

        Thread.sleep(100);

        for (v4RunnableProducer producer:Producers){
            producer.finishProducing();
        }

        for (v4RunnableConsumer consumer:Consumers){
            consumer.finishConsuming();
        }


    }
}

