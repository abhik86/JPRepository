package com.project.example.JPProject;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;


public class AdjustmentTask implements Runnable {

    private final List<ConsumerRecord<String, IncomingMessage>> records;
    List<ProcessedMessage> processedList;
    List<IncomingMessage> adjustmentQueue;
	List<IncomingMessage> processedAdjList = new LinkedList<IncomingMessage>();

    private volatile boolean taskStopped = false;

    private volatile boolean taskStarted = false;

    private volatile boolean taskFinished = false;

    private final CompletableFuture<Long> completion = new CompletableFuture<>();

    private final ReentrantLock lock = new ReentrantLock();

    private final AtomicLong currentOffset = new AtomicLong();

    private Logger log = LoggerFactory.getLogger(AdjustmentTask.class);

    public AdjustmentTask(List<ConsumerRecord<String, IncomingMessage>> records,
    		List<ProcessedMessage> processedList, List<IncomingMessage> adjustmentQueue) {
    	this.processedList= processedList;
        this.records = records;
        this.adjustmentQueue = adjustmentQueue;
    }


    public void run() {
        lock.lock();
        if (taskStopped){
            return;
        }
        taskStarted = true;
        lock.unlock();

        
        
        for (IncomingMessage adjustmentMessage : adjustmentQueue) {
            if (taskStopped)
                break;
			processedList.forEach((processedMessage) -> {

				if (processedMessage.getProductType().equalsIgnoreCase(adjustmentMessage.getProductType())) {
					if (adjustmentMessage.getOperationType().equalsIgnoreCase("ADD")) {
						processedMessage.setPrice(processedMessage.getPrice() + adjustmentMessage.getPrice());
					} else if (adjustmentMessage.getOperationType().equalsIgnoreCase("Substract")) {
						processedMessage.setPrice(processedMessage.getPrice() - adjustmentMessage.getPrice());
					} else if (adjustmentMessage.getOperationType().equalsIgnoreCase("Multiply")) {
						processedMessage.setPrice(processedMessage.getPrice() * adjustmentMessage.getPrice());
					}
					processedMessage.setTotal(processedMessage.getPrice() * processedMessage.getNoOfSale());
				}
			});
			processedAdjList.add(adjustmentMessage);


            
            
            
            
        }
        for (ConsumerRecord<String, IncomingMessage> record : records) {
            currentOffset.set(record.offset() + 1);
        }
        
        taskFinished = true;
        completion.complete(currentOffset.get());
    }

    public long getCurrentOffset() {
        return currentOffset.get();
    }

    public void stop() {
        lock.lock();
        this.taskStopped = true;
        if (!taskStarted) {
            taskFinished = true;
            completion.complete(currentOffset.get());
        }
        lock.unlock();
    }

    public long waitForCompletion() {
        try {
            return completion.get();
        } catch (InterruptedException | ExecutionException e) {
            return -1;
        }
    }

    public boolean isFinished() {
        return taskFinished;
    }

}

