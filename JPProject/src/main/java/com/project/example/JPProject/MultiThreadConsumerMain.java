package com.project.example.JPProject;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MultiThreadConsumerMain {
    
    public static void main(String[] args) throws InterruptedException {
           
           List<IncomingMessage> incomingMessageQueue = new LinkedList<IncomingMessage>();
           List<IncomingMessage> adjustmentQueue = new LinkedList<IncomingMessage>();
           List<ProcessedMessage> processedQueue = new LinkedList<ProcessedMessage>();
           ExecutorService executorService = null;
 try {
     Lock lock = new ReentrantLock();
     //producerCondition
     Condition isFull = lock.newCondition();
     //consumerCondition
     Condition isEmpty = lock.newCondition();
     
     Lock adjLock = new ReentrantLock();
     //adjustment Condition
     Condition adjWait = adjLock.newCondition();
     Condition consumerWait = adjLock.newCondition();
     
       Producer producer=new Producer(incomingMessageQueue,lock,isFull,isEmpty);
       Consumer consumer=new Consumer(incomingMessageQueue,adjustmentQueue,processedQueue,lock,isFull,isEmpty,adjLock,adjWait,consumerWait);
      AdjustmentProcessor processor = new AdjustmentProcessor(processedQueue, adjustmentQueue, adjLock, adjWait, consumerWait);
      List<Callable<Object>> threadList = new LinkedList<Callable<Object>>();
      threadList.add(producer);
      threadList.add(consumer);
      threadList.add(processor);
      
		/*
		 * Thread producerThread = new Thread(producer, "ProducerThread"); Thread
		 * consumerThread = new Thread(consumer, "ConsumerThread"); Thread
		 * processorThread = new Thread(processor, "ProcessorThread");
		 */
 executorService = Executors.newFixedThreadPool(3);
executorService.invokeAll(threadList);
 }finally {
	 executorService.shutdown();
}
/*
 * producerThread.start(); consumerThread.start(); processorThread.start();
 */
           
    }
 
 
}
