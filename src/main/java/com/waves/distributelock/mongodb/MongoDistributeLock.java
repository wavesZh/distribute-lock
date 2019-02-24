package com.waves.distributelock.mongodb;

import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * mongo分布式锁
 */
public class MongoDistributeLock implements Lock {

	private String id;

	private final static Integer ACQUIRED = 1;

	private final static Integer RELEASE = 0;

	private MongoTemplate mongoTemplate;

	public MongoDistributeLock(String id, MongoTemplate mongoTemplate) {
		this.mongoTemplate = mongoTemplate;
		this.id = id;
		init();
	}

	synchronized private void init() {
		LockTableEntity entity = new LockTableEntity();
		Query query = new Query(Criteria.where("id").is(this.id));
		if (!mongoTemplate.exists(query, LockTableEntity.class)) {
			entity.setId(id);
			entity.setState(RELEASE);
			entity.setThread(0L);
			mongoTemplate.insert(entity);
		}
	}

	@Override
	public void lock() {
		while (!tryLock()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void lockInterruptibly() throws InterruptedException {

	}

	@Override
	public boolean tryLock() {
		if (compareAndSwap(RELEASE, ACQUIRED, 0L, Thread.currentThread().getId())) {
			System.out.println(Thread.currentThread().getName() + " lock success!");
			return true;
		} else {
			System.out.println(Thread.currentThread().getName() + " lock fail!");
			return false;
		}
	}

	@Override
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
		return false;
	}

	@Override
	public void unlock() {
		if (compareAndSwap(ACQUIRED, RELEASE, Thread.currentThread().getId(), 0L)) {
			System.out.println(Thread.currentThread().getName() + " lock release!");
		} else {
			System.out.println(Thread.currentThread().getName() + " lock release fail!");
		}
	}

	private boolean compareAndSwap(int oldState, int newState, long oldThread, long newThread) {
		Query query = new Query(Criteria.where("id").is(this.id).and("state").is(oldState).and("thread").is(oldThread));
		Update update = new Update();
		update.set("state", newState);
		update.set("thread", newThread);
		FindAndModifyOptions options = new FindAndModifyOptions();
		options.upsert(false);
		options.returnNew(true);
		LockTableEntity lock = mongoTemplate.findAndModify(query, update, options, LockTableEntity.class);
		if (lock == null) {
			System.out.println(Thread.currentThread().getName() + " dont hold the lock!");
			return false;
		}
		if (lock.getState() == newState && lock.getThread() == newThread) {
			return true;
		}
		return false;
	}

	@Override
	public Condition newCondition() {
		return null;
	}
}
