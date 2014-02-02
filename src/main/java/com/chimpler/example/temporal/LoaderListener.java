package com.chimpler.example.temporal;

import java.util.Date;

public interface LoaderListener {
	void startQuote(String currency1, String currency2);
	void insertQuoteValue(Date date, float open, float high, float low, float close);
	void endQuote(String currency1, String currency2);
}
