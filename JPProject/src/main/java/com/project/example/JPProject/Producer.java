package com.project.example.JPProject;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import java.util.logging.Logger;

class Producer implements Callable<Object> {
	Logger logger = Logger.getLogger(Producer.class.getName());

	private List<IncomingMessage> incomingMessageQueue;
	private int maxSize = 1000;

	Lock lock;
	Condition isFull;
	Condition isEmpty;

	public Producer(List<IncomingMessage> incomingMessageQueue, Lock lock, Condition isFull, Condition isEmpty) {
		this.incomingMessageQueue = incomingMessageQueue;
		this.lock = lock;
		this.isFull = isFull;
		this.isEmpty = isEmpty;
	}

	public Boolean call() throws InterruptedException {
		for (int i = 1; i <= 160; i++) {
			

				dummyProduce(i);

			
		}
		return true;
	}

	public boolean produce(IncomingMessage message) throws InterruptedException {
		try {

			lock.lock();

			// if sharedQuey is full producer await until consumer consumes.
			if (incomingMessageQueue.size() == maxSize) {
				isFull.await();
			}

			incomingMessageQueue.add(message);

			isEmpty.signal();
		} finally {
			lock.unlock();
		}
		return true;
	}

	public void dummyProduce(int i) throws InterruptedException {
		try {
		lock.lock();

		// if sharedQuey is full producer await until consumer consumes.
		if (incomingMessageQueue.size() == maxSize) {
			isFull.await();
		}

		logger.log(Level.INFO, "Produced dummy data: " + i);
		if (i == 5 || i == 55 || i == 106) {
			incomingMessageQueue.add(new IncomingMessage("Adjustment", "ADD", "iphone", 0, 20));

		} else {
			if (i % 2 == 0) {
				incomingMessageQueue.add(new IncomingMessage("Generic", null, "iphone", 1, 20));
			} else {
				incomingMessageQueue.add(new IncomingMessage("Generic", null, "ipad", 2, 20));
			}
		}

		isEmpty.signal();
		}finally {
		lock.unlock();
		}

	}

}