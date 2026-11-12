package manager;

import consumer.v1RunnableConsumer;
import consumer.v4RunnableConsumer;
import producer.v1RunnableProducer;
import producer.v4RunnableProducer;

import java.util.ArrayList;
import java.util.List;

public class v1_Manager {
    public static void main (String[] args)throws InterruptedException{
        ArrayList<Message> queue = new ArrayList();
        List<v1RunnableConsumer>Consumers= new ArrayList<>();
        List<v1RunnableProducer>Producers= new ArrayList<>();

        for (int i = 0; i < 200; i++) {
            Producers.add(new v1RunnableProducer(queue,"Producer"+i,100));
            Consumers.add(new v1RunnableConsumer(queue));
        }

        for(v1RunnableProducer producer:Producers){
            new Thread(producer).start();
        }
        for(v1RunnableConsumer consumer:Consumers){
            new Thread(consumer).start();
        }

        Thread.sleep(100);

        for (v1RunnableProducer producer:Producers){
            producer.finishProducing();
        }

        for (v1RunnableConsumer consumer:Consumers){
            consumer.finishConsuming();
        }


    }
}
