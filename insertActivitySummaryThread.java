package com.mapmymotion.globalexchangeserver;

import com.tokenlibrary.Token;



// ActivitySummary Thread
public class insertActivitySummaryThread implements Runnable {
	private Token ltoken;
	private SQL mSQL;

	insertActivitySummaryThread(Token aToken,SQL aMsql ) {
		ltoken = aToken;
		mSQL=aMsql;
		//Thread t = new Thread(this);
		//t.start();
	}
	
	@Override
	public void run() {
		mSQL.InsertActivitySummary(ltoken);
	}
}
