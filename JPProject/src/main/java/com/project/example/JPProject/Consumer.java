package com.project.example.JPProject;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

class Consumer implements Callable {
	Logger logger = Logger.getLogger(Consumer.class.getName());
	private List<IncomingMessage> incomingMessageQueue;
	private List<IncomingMessage> adjustmentQueue;
	private List<IncomingMessage> errorQueue = new LinkedList<>();

	Lock lock;
	Lock adjLock;
	Condition isFull;
	Condition isEmpty;
	Condition adjWait;
	Condition consumerWait;
	List<ProcessedMessage> processedList;

	BiConsumer<String, Integer> loggerConsumer = (productType, total) -> {
		logger.log(Level.INFO, "For " + productType + " total sale is  " + total);
	};

	public Integer call() throws TimeoutException, InterruptedException {
		int i = 0;
		while (true) {

			i++;
			consume(i);

		}
	}

	public void consume(int readCounter) throws TimeoutException, InterruptedException {
		lock.lock();
		try {
			if (incomingMessageQueue.size() == 0) {
				if (!isEmpty.await(10, TimeUnit.MINUTES)) {
					throw new TimeoutException("Consumer TimeOut");
				}
			}
			IncomingMessage message =  incomingMessageQueue.remove(0);
			message.setCounter(readCounter);
			processMessage(message);

			logger.log(Level.INFO, "Consumed " + readCounter);
			isFull.signal();

			if (readCounter % 10 == 0) {
				logCurrentStat();
			}
			if (readCounter % 50 == 0) {
				adjLock.lock();
				adjWait.signal();
				consumerWait.await();
				logger.log(Level.INFO, "After adjustment processing");
				logCurrentStat();
			}
		} finally {
			lock.unlock();

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

	public Consumer(List<IncomingMessage> incomingMessageQueue, List<IncomingMessage> adjustmentQueue,
			List<ProcessedMessage> processedList, Lock lock, Condition isFull, Condition isEmpty, Lock adjLock,
			Condition adjWait, Condition consumerWait) {
		this.incomingMessageQueue = incomingMessageQueue;
		this.adjustmentQueue = adjustmentQueue;
		this.processedList = processedList;
		this.lock = lock;
		this.isFull = isFull;
		this.isEmpty = isEmpty;
		this.adjLock = adjLock;
		this.adjWait = adjWait;
		this.consumerWait = consumerWait;

	}

}
