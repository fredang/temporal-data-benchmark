package com.chimpler.example.temporal;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.github.junrar.Archive;
import com.github.junrar.impl.FileVolumeManager;
import com.github.junrar.rarfile.FileHeader;

public class CassandraLoader {
	public final static int BATCH_SIZE = 100;
	
	public static void main(String[] args) throws Exception {
		Cluster cluster = Cluster.builder()
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
		
		Session session = cluster.connect();
		

		session.execute("CREATE KEYSPACE fxrate WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
//		session.execute("CREATE TABLE fxrate.quote ("
//				+ "currency1 text,"
//				+ "currency2 text,"
//				+ "datetime timestamp,"
//				+ "open float,"
//				+ "high float,"
//				+ "low float,"
//				+ "close float,"
//				+ "PRIMARY KEY (currency1, currency2, datetime));");

//		"Date","Time","Open","High","Low","Close","TotalVolume"
//		11/27/2001,23:10:00,0.83330,0.83330,0.83320,0.83320,0

		session.execute("DROP KEYSPACE IF exists fxrate;");


        int fileCount = 0;
		File dataDirectory = new File("data");
		
		Pattern pattern = Pattern.compile("[A-Z]{6}[+](bid|ask)[.]rar");
		for(File file: dataDirectory.listFiles()) {
			Matcher matcher = pattern.matcher(file.getName());
			if (!matcher.find()) {
				System.out.println("Skipped " + file.getName());
				continue;
			}

			fileCount++;
			if(fileCount > 100) {
				break;
			}
			Archive archive = new Archive(new FileVolumeManager(file));
			List<FileHeader> fileHeaders = archive.getFileHeaders();
			DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

			int entryCount = 0;
			long startTime = System.currentTimeMillis();
			for(FileHeader fileHeader: fileHeaders) {
				String fileName = fileHeader.getFileNameString();
				String currency1 = fileName.substring(0, 3);
				String currency2 = fileName.substring(3, 6);
				String bidOrAsk = fileName.substring(7, 10);
				if (bidOrAsk.equals("bid")) {
					String tmp = currency1;
					currency1 = currency2;
					currency2 = tmp;
				}
				
				String tableName = currency1 + currency2;
				session.execute("DROP TABLE IF exists fxrate." + tableName + ";");

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
				
		        PreparedStatement preparedStatement = session.prepare(insertSql);
		        preparedStatement.setConsistencyLevel(ConsistencyLevel.ONE);  
				
				InputStream is = archive.getInputStream(fileHeader);
				BufferedReader br = new BufferedReader(new InputStreamReader(is));
				String line;
				// skip first line(header)
				br.readLine();
				BatchStatement batchStatement = new BatchStatement();
				while((line = br.readLine()) != null) {
					String[] tokens = line.split(",");
			        
					String dateTime = tokens[0] + " " + tokens[1];
					Date date = dateFormat.parse(dateTime);
					float open = Float.parseFloat(tokens[2]);
					float high = Float.parseFloat(tokens[3]);
					float low = Float.parseFloat(tokens[4]);
					float close = Float.parseFloat(tokens[5]);
					
					BoundStatement boundStatement = preparedStatement.bind(date, open, high, low, close);
					batchStatement.add(boundStatement);
					entryCount++;
					if (entryCount % BATCH_SIZE == 0) {
						session.execute(batchStatement);
						batchStatement = new BatchStatement();

					}
				}
				if (entryCount % BATCH_SIZE != 0) {
					session.execute(batchStatement);
				}
				br.close();
			}
			archive.close();
			System.out.println(file.getName() + ": " + entryCount + " in " + (System.currentTimeMillis() - startTime));

		}
		cluster.shutdown();
	}
}
