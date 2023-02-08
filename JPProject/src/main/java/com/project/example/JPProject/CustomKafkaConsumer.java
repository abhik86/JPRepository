package com.project.example.JPProject;


import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




public class CustomKafkaConsumer implements Runnable, ConsumerRebalanceListener {

    private final KafkaConsumer<String, IncomingMessage> consumer;
    private final ExecutorService executor = Executors.newFixedThreadPool(1);
    private final Map<TopicPartition, AdjustmentTask> activeTasks = new HashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private long lastCommitTime = System.currentTimeMillis();
    private final Logger logger = LoggerFactory.getLogger(CustomKafkaConsumer.class);
    List<TopicPartition> partitionsToPause = new ArrayList<>();
    int readCount = 0;
	List<IncomingMessage> exceptionList = new LinkedList<IncomingMessage>();
	BiConsumer<String, Integer> loggerConsumer = (productType, total) -> {
		logger.info("For " + productType + " total sale is  " + total);
	};

	private List<ConsumerRecord<String,IncomingMessage>> consumerRecordToAdjust = new LinkedList<>();

	private List<IncomingMessage> adjustmentQueue;
	private List<IncomingMessage> errorQueue = new LinkedList<>();
	List<ProcessedMessage> processedList = new LinkedList<>(); 
	public CustomKafkaConsumer(String topic) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-consumer");
        consumer = new KafkaConsumer<String, IncomingMessage>(config);
        new Thread(this).start();
    }


    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singleton("topic-name"), this);
            while (!stopped.get()) {
            	

                ConsumerRecords<String, IncomingMessage> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                handleFetchedRecords(records);
                checkActiveTasks();
                commitOffsets();
            }
            
        } catch (WakeupException we) {
            if (!stopped.get())
                throw we;
        } finally {
            consumer.close();
        }
    }

 

		
		private void logCurrentStat() {
			Map<String, Integer> logMap = processedList.stream()
					.collect(Collectors.groupingBy(m -> m.getProductType(), Collectors.summingInt(m -> m.getTotal())));
			logMap.forEach(loggerConsumer);
		}

		private void processMessage(IncomingMessage message) throws InterruptedException {

			try {
			if (message.getMessageType().equalsIgnoreCase("Adjustment")) {
				if (!isValidMessage(message)) {
					pushToErrorQueue(message);

				} else {
					pushToAdjustmentQueue(message);
				}

			} else {
				if (!isValidMessage(message)) {
					pushToErrorQueue(message);

				} else {
					processedList.add(new ProcessedMessage(message.getProductType(), message.getNoOfSale(),
							message.getPrice(), message.getNoOfSale() * message.getPrice()));
				}
			}
			}catch(Exception e) {
				pushToErrorQueue(message);
			}
		}

		private void pushToAdjustmentQueue(IncomingMessage message) {
			adjustmentQueue.add(message);

		}

		private void pushToErrorQueue(IncomingMessage message) {
			errorQueue.add(message);

		}

		private boolean isValidMessage(IncomingMessage message) throws InterruptedException {
			if (!message.getMessageType().equalsIgnoreCase("Adjustment")) {
				if (message.getNoOfSale() == 0 || message.getPrice() == 0 || message.getProductType() == null)

					return false;

				else
					return true;
			} else {

				if (message.getOperationType() == null || message.getPrice() == 0 || message.getProductType() == null)

					return false;

				else
					return true;
			}
		}


    private void handleFetchedRecords(ConsumerRecords<String, IncomingMessage> records){
     

    	if (records.count() > 0) {
             records.partitions().forEach(partition -> {
                 List<ConsumerRecord<String, IncomingMessage>> partitionRecords = records.records(partition);
                 ConsumerRecord<String, IncomingMessage> consumerRecords = partitionRecords.remove(0);
                 IncomingMessage incomingMessage = consumerRecords.value();
                 partitionsToPause.add(partition);
                 incomingMessage.setCounter(readCount++);
                 consumerRecordToAdjust.add(consumerRecords);
                 try {
					processMessage(incomingMessage);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
                 if(readCount%10==0) {
                	 
     				logCurrentStat();

                 }
                 AdjustmentTask task = null;
                 if(readCount%50==0) {
                     consumer.pause(partitionsToPause);
                     task = new AdjustmentTask(consumerRecordToAdjust,processedList,adjustmentQueue);

                     partitionsToPause = new LinkedList<>();
                     consumerRecordToAdjust = new LinkedList<>();
 					
                    executor.submit(task);
                    activeTasks.put(partition, task);

                  }
                 

             });
        }
    }

    private void commitOffsets() {
        try {
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis - lastCommitTime > 5000) {
                if(!offsetsToCommit.isEmpty()) {
                    consumer.commitSync(offsetsToCommit);
                    offsetsToCommit.clear();
                }
                lastCommitTime = currentTimeMillis;
            }
        } catch (Exception e) {
            logger.error("Failed to commit offsets!", e);
        }
    }


    private void checkActiveTasks() {
        List<TopicPartition> finishedTasksPartitions = new ArrayList<>();
        activeTasks.forEach((partition, task) -> {
            if (task.isFinished())
                finishedTasksPartitions.add(partition);
            long offset = task.getCurrentOffset();
            if (offset > 0)
                offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
        });
        finishedTasksPartitions.forEach(partition -> activeTasks.remove(partition));
        consumer.resume(finishedTasksPartitions);
    }


      @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        consumer.resume(partitions);
    }


    public void stopConsuming() {
        stopped.set(true);
        consumer.wakeup();
    }


	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		// TODO Auto-generated method stub
		
	}

}
