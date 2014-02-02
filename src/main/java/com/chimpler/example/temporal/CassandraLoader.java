package com.chimpler.example.temporal;

import java.io.IOException;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.Session;

public class CassandraLoader implements LoaderListener {
	private final static Logger logger = LoggerFactory.getLogger(QuoteFileLoader.class);

	public final static int BATCH_SIZE = 1000;
	
	private Cluster cluster;
	private Session session;
	private PreparedStatement preparedStatement;
	private BatchStatement batchStatement = new BatchStatement();
	private int entryCount;
	private long cassandraTime;
	private String currentCurrency1, currentCurrency2;
	
	public CassandraLoader() {
		cluster = Cluster.builder()
				.addContactPoint("127.0.0.1")
				.build();
		cluster.getConfiguration().getProtocolOptions().setCompression(Compression.LZ4);
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n", 
				metadata.getClusterName());
		for (Host host : metadata.getAllHosts() ) {
			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}
		
		session = cluster.connect();		

		// create keyspace
		session.execute("DROP KEYSPACE IF exists fxrate;");
		session.execute("CREATE KEYSPACE fxrate WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
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
		cluster.shutdown();
	}
	
	@Override
	public void startQuote(String currency1, String currency2) {
		currentCurrency1 = currency1;
		currentCurrency2 = currency2;
		String tableName = currency1 + currency2;
//		session.execute("DROP TABLE IF exists fxrate." + tableName + ";");

		session.execute("CREATE TABLE fxrate." + tableName + " ("
				+ "datetime timestamp,"
				+ "open float,"
				+ "high float,"
				+ "low float,"
				+ "close float,"
				+ "PRIMARY KEY (datetime));");		
		
		String insertSql = "INSERT INTO fxrate." + tableName + " "
				+ "(datetime, open, high, low, close) "
				+ "VALUES (?, ?, ?, ?, ?)";

		
        preparedStatement = session.prepare(insertSql);
        preparedStatement.setConsistencyLevel(ConsistencyLevel.ONE);  
        
        entryCount = 0;
        batchStatement = null;
	}



	@Override
	public void insertQuoteValue(Date date, float open, float high, float low, float close) {
		if (entryCount % BATCH_SIZE == 0) {
			if (batchStatement != null) {
				long startTime = System.currentTimeMillis();
				session.execute(batchStatement);
				long time = System.currentTimeMillis() - startTime;
				logger.info("Inserted {} entries for {}-{} in {} ms", BATCH_SIZE, currentCurrency1, currentCurrency2, time);
				cassandraTime += time;
			}
			batchStatement = new BatchStatement();
		}
		BoundStatement boundStatement = preparedStatement.bind(date, open, high, low, close);
		batchStatement.add(boundStatement);
		entryCount++;
	}



	@Override
	public void endQuote(String currency1, String currency2) {
		session.execute(batchStatement);
	}

	public static void main(String[] args) throws Exception {

		CassandraLoader loader = new CassandraLoader();
		loader.loadData();
	}
}
