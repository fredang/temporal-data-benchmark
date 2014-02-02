package com.chimpler.example.temporal;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.junrar.Archive;
import com.github.junrar.exception.RarException;
import com.github.junrar.impl.FileVolumeManager;
import com.github.junrar.rarfile.FileHeader;

public class QuoteFileLoader {
	private final static DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
	private final static Logger logger = LoggerFactory.getLogger(QuoteFileLoader.class);

	public void load(String dataDirName, LoaderListener listener) throws IOException {
		File dataDirectory = new File(dataDirName);
		
		Pattern pattern = Pattern.compile("([A-Z]{3})([A-Z]{3})[+](bid|ask)[.]rar");
		for(File file: dataDirectory.listFiles()) {
			Matcher matcher = pattern.matcher(file.getName());
			if (!matcher.find()) {
				logger.info("Skipped archive {}: not matching pattern", file.getName());
				continue;
			}

			String currency1 = matcher.group(1);
			String currency2 = matcher.group(2);
			String bidOrAsk = matcher.group(3);;
			if (bidOrAsk.equals("bid")) {
				String tmp = currency1;
				currency1 = currency2;
				currency2 = tmp;
			}

			logger.info("Start writing quote for {} - {}", currency1, currency2);
			long startTime = System.currentTimeMillis();
			listener.startQuote(currency1, currency2);
			
			Archive archive;
			try {
				archive = new Archive(new FileVolumeManager(file));
			} catch (RarException e) {
				logger.info("Skip archive {}: {}", file.getName(), e.getMessage());
				continue;
			}
			List<FileHeader> fileHeaders = archive.getFileHeaders();
			
			for(FileHeader fileHeader: fileHeaders) {
				String fileName = fileHeader.getFileNameString();
				InputStream is;
				try {
					is = archive.getInputStream(fileHeader);
				} catch (RarException e) {
					logger.info("Skip entry in archive {}: entry {}: {}", fileName, fileHeader.getFileNameString(), e.getMessage());
					continue;
				}
				BufferedReader br = new BufferedReader(new InputStreamReader(is));
				
				// skip first line
				br.readLine();
				String line;
				int entryCount = 0;
				while((line = br.readLine()) != null) {
					String[] tokens = line.split(",");
			        
					String dateTime = tokens[0] + " " + tokens[1];
					Date date;
					try {
						date = dateFormat.parse(dateTime);
					} catch (ParseException e) {
						logger.info("Skip in {}: line {}: {}", fileName, line, e.getMessage());
						continue;
					}
					float open = Float.parseFloat(tokens[2]);
					float high = Float.parseFloat(tokens[3]);
					float low = Float.parseFloat(tokens[4]);
					float close = Float.parseFloat(tokens[5]);
					
					listener.insertQuoteValue(date, open, high, low, close);
					entryCount++;
					if (entryCount % 1000 == 0) {
						logger.info("Wrote {} quotes", entryCount);
					}
				}
				listener.endQuote(currency1, currency2);
				logger.info("Wrote {} quotes for {} - {} in {} ms", currency1, currency2, System.currentTimeMillis() - startTime);
			}
		}
	}
}
