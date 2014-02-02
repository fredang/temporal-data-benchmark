package com.chimpler.example.temporal;

import java.io.IOException;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.ConversionException;

public class RiakLoader implements LoaderListener {
	private final static Logger logger = LoggerFactory.getLogger(QuoteFileLoader.class);

	public final static int BATCH_SIZE = 100;
	
	private Bucket currentBucket;
	private IRiakClient riakClient;
	private int entryCount;
	private long cassandraTime;
	private String currentCurrency1, currentCurrency2;
	
	public RiakLoader() throws RiakException {
		riakClient = RiakFactory.pbcClient();

	}
	
	public void loadData() throws IOException {
		QuoteFileLoader loader = new QuoteFileLoader();
		long startTime = System.currentTimeMillis();
		loader.load("data", this);
		long endTime = System.currentTimeMillis();
		
		long totalTime = endTime - startTime;
		
		logger.info("Read {} entries", entryCount);
		logger.info("Total time: {} ms", totalTime);
		logger.info("Cassandra time: {} ms", cassandraTime);
		riakClient.shutdown();
	}
	
	@Override
	public void startQuote(String currency1, String currency2) {
		currentCurrency1 = currency1;
		currentCurrency2 = currency1;
		// create a new bucket
		try {
			currentBucket = riakClient.createBucket("fxrate_" + currency1 + currency2).execute();
		} catch (RiakRetryFailedException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}

        entryCount = 0;
	}



	@Override
	public void insertQuoteValue(Date date, float open, float high, float low, float close) {
		try {
			currentBucket.store(new QuoteValue(date.getTime(), open, high, low, close)).execute();
		} catch (RiakRetryFailedException|UnresolvedConflictException|ConversionException e) {
			e.printStackTrace();
		}
		entryCount++;
	}

	@Override
	public void endQuote(String currency1, String currency2) {
	}

	public static void main(String[] args) throws Exception {
		RiakLoader loader = new RiakLoader();
		loader.loadData();
	}
}
