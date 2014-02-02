package com.chimpler.example.temporal;

import com.basho.riak.client.convert.RiakKey;

public class QuoteValue {
	private Long timestamp;
	private float open;
	private float high;
	private float low;
	private float close;
	
	public QuoteValue() {
	}

	public QuoteValue(Long timestamp, float open, float high, float low, float close) {
		this.timestamp = timestamp;
		this.open = open;
		this.high = high;
		this.low = low;
		this.close = close;
	}


	public Long getTimestamp() {
		return timestamp;
	}


	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}
	
	public float getOpen() {
		return open;
	}

	public void setOpen(float open) {
		this.open = open;
	}

	public float getHigh() {
		return high;
	}

	public void setHigh(float high) {
		this.high = high;
	}

	public float getLow() {
		return low;
	}

	public void setLow(float low) {
		this.low = low;
	}

	public float getClose() {
		return close;
	}

	public void setClose(float close) {
		this.close = close;
	}

	@RiakKey
	public String getKey() {
		return Long.toString(timestamp);
	}
	
	public void setKey(String key) {
		this.timestamp = Long.parseLong(key);
	}
}
