package com.pragmafs.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.util.Random;

import static com.pragmafs.demo.Item.Source.SNAP;
import static com.pragmafs.demo.Item.Source.UPDATE;

@SpringBootApplication
public class DemoApplication implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(DemoApplication.class);

	final Random random = new Random();
	final int COUNT = 200_000;
	final static String[] IDS = new String[26];
	static {
		for (int i = 0; i < IDS.length; i++) {
			IDS[i] = Character.toString('a' + i);
		}
	}

	long startTime;
	Item lastUpdate;
	Item lastSnapshot;

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	private String randomId() {
		return IDS[Math.abs(random.nextInt()) % IDS.length];
	}

	@Override
	public void run(String... args) {

		// Create a fan-out hot source
		Sinks.Many<Item> sink = Sinks.many().multicast().directBestEffort();

		// Pump items into the hot source as fast as we can
		Flux<Item> items = Flux.range(1, COUNT)
			.map(i -> new Item(randomId(), i, UPDATE))
			.doOnComplete(() -> log.info("Source completed"))
			.subscribeOn(Schedulers.newSingle("source"));
		items.subscribe(sink::tryEmitNext);

		// Feed the cache from the hot flux
		InMemoryTableStore cache = new InMemoryTableStore();
		sink.asFlux()
			.subscribeOn(Schedulers.newSingle("cacher"))
			.subscribe(cache::upsert);


		// Pause to let the cache fill up - we start subscription sequence at some arbitrary point in the stream
		Random random = new Random();
		long sleepMillis = random.nextLong(1, 5);
		nap(sleepMillis);

		log.info("Cache has {} items after {} millis sleep", cache.size(), sleepMillis);

		Flux<Item> snapshot = cache.select();

		Flux<Item> updates = sink.asFlux();

		startTime = System.nanoTime();

		Flux<Item> merged = new SnapshotPrependerWithComposition(snapshot, updates, true).asFlux().doOnNext(this::checkForErrors);

		var subscription = merged.subscribe();

		nap(500);
		subscription.dispose();
		nap(100);
		System.exit(0);
	}

	/**
	 * Checks for invalid output:
	 * - a snapshot item coming after any update item
	 * - an update whose value isn't exactly 1 higher than the previous update's value
	 * - an update for a given id with value
	 * Also logs progress (every 10,000th emitted item), and check
	 */
	private void checkForErrors(Item item) {
		if (item.source() == SNAP) {
			if (lastSnapshot == null) {
				log.info("First snapshot is {}", item);
			}

			if (lastUpdate != null) {
				log.warn(">>> Received snapshot {} after update {}", item, lastUpdate);
			}

			if (lastSnapshot == null || (lastSnapshot.value() < item.value())) {
				lastSnapshot = item;
			}
		} else {
			// UPDATE
			//log.info("Update is {}", item);
			// First update after snapshot
			if (lastUpdate == null && lastSnapshot != null) {
				log.info("Snapshot (last) is {}", lastSnapshot);
				log.info("Update (first) is {}", item);
				if (item.value() > lastSnapshot.value() + 1) {
					log.warn(">>> GAP between last snapshot {} and first update {}", lastSnapshot, item);
				}
 			}  else if (lastUpdate == null) {	// first update, no snapshot
				log.info("First update is {}", item);
			} else if (item.value() != lastUpdate.value() + 1) {
				log.warn(">>> GAP between update {} and {}", lastUpdate, item);
			}

			lastUpdate = item;
		}
		if (item.value() % 10_000 == 0) {
			log.info("Merged {}", item);
		}
		if (item.value() == COUNT) {
			long endTime = System.nanoTime();
			log.info("Elapsed = {} ms", (endTime - startTime)/1e6);
			log.info("Last item was {}", lastUpdate);
		}
	}

	private void nap(long ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			log.error("Interrupted", e);
			Thread.currentThread().interrupt();
		}
	}
}
