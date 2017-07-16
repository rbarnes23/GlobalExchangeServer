package com.mapmymotion.globalexchangeserver;

import com.tokenlibrary.Token;

//ActivitySummary Thread
public class insertActivityThread implements Runnable {
	private Token ltoken;
	private SQL mSQL;

	insertActivityThread(Token aToken,SQL aMsql ) {
		ltoken = aToken;
		mSQL=aMsql;
		//Thread t = new Thread(this);
		//t.start();
	}
@Override
	public void run() {
		mSQL.InsertActivity(ltoken);
	}
}
