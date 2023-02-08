package com.project.example.JPProject;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.springframework.core.io.ClassPathResource;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JPSingleThreadConsumer {
	Logger logger = Logger.getLogger(Consumer.class.getName());

	public void init()
			throws InterruptedException, StreamReadException, DatabindException, FileNotFoundException, IOException {

		ObjectMapper mapper = new ObjectMapper();
		List<IncomingMessage> list = mapper.readValue(new ClassPathResource("input.json").getFile(),
				new TypeReference<List<IncomingMessage>>() {
				});
		list.forEach(i -> queue.offer(i));

	}

	public void produce(IncomingMessage message) {

		try {
			if (!queue.offer(message, 100, TimeUnit.SECONDS)) {
				exceptionList.add(message);
			}

		} catch (InterruptedException e) {
			exceptionList.add(message);
		}
	}

	BiConsumer<String, Integer> loggerConsumer = (productType, total) -> {
		logger.log(Level.INFO, "For " + productType + " total sale is  " + total);
	};
	BlockingQueue<IncomingMessage> adjustmentQueue = new ArrayBlockingQueue<>(50);;
	BlockingQueue<IncomingMessage> queue = new ArrayBlockingQueue<>(1050);
	List<ProcessedMessage> processedList = new LinkedList<ProcessedMessage>();
	List<IncomingMessage> processedAdjList = new LinkedList<IncomingMessage>();
	List<IncomingMessage> exceptionList = new LinkedList<IncomingMessage>();

	public void consume() {
		int readCounter = 0;
		IncomingMessage message = null;
		while (true) {
			readCounter++;
			try {
				message = queue.poll();
				message.setCounter(readCounter);
				if (message.getMessageType().equalsIgnoreCase("Adjustment")) {
					if (!isValidMessage(message)) {
						exceptionList.add(message);
					} else {
						queueAdjustment(message);
					}
				} else {
					if (!isValidMessage(message)) {
						exceptionList.add(message);
					} else {
						processMessage(message);
					}
				}
				if (readCounter % 50 == 0) {
					processAdjustment();
				}
				if (readCounter % 10 == 0) {
					logCurrentStat();
				}

			} catch (InterruptedException ie) {
				exceptionList.add(message);
			} catch (Exception e) {
				exceptionList.add(message);
			}
		}

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

	private void processAdjustment() {
		logger.log(Level.INFO, "Stopping Processing for adjustment processing");
		while (true) {
			IncomingMessage adjMessage = adjustmentQueue.poll();
			if (adjMessage == null) {
				logger.log(Level.INFO, "No Adj Message to process");
				logCurrentStat();
				break;
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
			}catch(Exception e) {
				exceptionList.add(adjMessage);
			}
		}

	}

	private void queueAdjustment(IncomingMessage message) throws InterruptedException {
		if (!adjustmentQueue.offer(message, 100, TimeUnit.SECONDS)) {
			exceptionList.add(message);
		}

	}

	private void logCurrentStat() {
		Map<String, Integer> logMap = processedList.stream()
				.collect(Collectors.groupingBy(m -> m.getProductType(), Collectors.summingInt(m -> m.getTotal())));
		logMap.forEach(loggerConsumer);
	}

	private void processMessage(IncomingMessage message) {

		processedList.add(new ProcessedMessage(message.getProductType(), message.getNoOfSale(), message.getPrice(),
				message.getNoOfSale() * message.getPrice()));
	}

}
