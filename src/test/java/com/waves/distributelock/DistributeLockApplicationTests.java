package com.waves.distributelock;

import com.waves.distributelock.mongodb.MongoDistributeLock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.CountDownLatch;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DistributeLockApplicationTests {

	@Test
	public void contextLoads() {
	}

	@Autowired
	private MongoTemplate mongoTemplate;

	@Test
	public void testMongo() {
		mongoTemplate.getCollectionNames().forEach(System.out::println);
	}

	@Test
	public void testMongoLock() {
		MongoDistributeLock lock = new MongoDistributeLock("lock0", mongoTemplate);
		try {
			lock.lock();
		} catch (Exception e) {
			e.printStackTrace();
		}  finally {
			lock.unlock();
		}
	}

	@Test
	public void testMongoLockWithMultiThread() throws InterruptedException {
		CountDownLatch count = new CountDownLatch(3);
		MongoDistributeLock lock = new MongoDistributeLock("lock1", mongoTemplate);
		for (int i=0; i<3; i++) {
			new Thread(() -> {
				try {
					count.await();
					lock.lock();
					Thread.sleep(100);
				} catch (Exception e) {
					e.printStackTrace();
				}  finally {
					lock.unlock();
				}
			}).start();
			count.countDown();
		}
		Thread.sleep(1000);
	}

}
