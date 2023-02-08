package com.project.example.JPProject;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AdjustmentProcessor implements Callable {
	Lock adjLock;
	Condition adjWait;
	Condition consumerWait;
	Logger logger = Logger.getLogger(AdjustmentProcessor.class.getName());
	private List<IncomingMessage> errorQueue = new LinkedList<>();

	List<ProcessedMessage> processedList;
	List<IncomingMessage> adjustmentQueue;
	List<IncomingMessage> processedAdjList = new LinkedList<>();

	public AdjustmentProcessor(List<ProcessedMessage> processedList, List<IncomingMessage> adjustmentQueue,
			Lock adjLock, Condition adjWait, Condition consumerWait) {
		this.adjLock = adjLock;
		this.processedList = processedList;
		this.adjustmentQueue = adjustmentQueue;
		this.adjWait = adjWait;
		this.consumerWait = consumerWait;
	}

	public Object call() throws InterruptedException {

		processAdjustment();

		return null;

	}

	private void processAdjustment() throws InterruptedException {
		logger.log(Level.INFO, "Stopping Processing for adjustment processing");
		int i = 0;
		while (true) {

			adjLock.lock();
			if (i == 0) {
				i++;
				adjWait.await();

			}
			if (adjustmentQueue.size() == 0) {
				consumerWait.signal();
				adjWait.await();
			}

			IncomingMessage adjMessage = adjustmentQueue.remove(0);
			if (adjMessage == null) {
				logger.log(Level.INFO, "No Adj Message to process");
				consumerWait.signal();
				adjWait.await();
			}
			try {
				processedList.forEach((processedMessage) -> {

					if (processedMessage.getProductType().equalsIgnoreCase(adjMessage.getProductType())) {
						if (adjMessage.getOperationType().equalsIgnoreCase("ADD")) {
							processedMessage.setPrice(processedMessage.getPrice() + adjMessage.getPrice());
						} else if (adjMessage.getOperationType().equalsIgnoreCase("Substract")) {
							processedMessage.setPrice(processedMessage.getPrice() - adjMessage.getPrice());
						} else if (adjMessage.getOperationType().equalsIgnoreCase("Multiply")) {
							processedMessage.setPrice(processedMessage.getPrice() * adjMessage.getPrice());
						}
						processedMessage.setTotal(processedMessage.getPrice() * processedMessage.getNoOfSale());

					}
				});
				processedAdjList.add(adjMessage);
				adjLock.unlock();

			} catch (Exception e) {
				errorQueue.add(adjMessage);
			}
		}

	}

}
