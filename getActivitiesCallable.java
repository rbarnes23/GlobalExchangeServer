package com.mapmymotion.globalexchangeserver;

import java.util.concurrent.Callable;

import com.tokenlibrary.Token;


class getActivitiesCallable implements Callable<String>{

	private Token mToken;
	private SQL mSQL;
	private String result;
	getActivitiesCallable(Token aToken,SQL aSQL) {
		if (aToken != null) {
		mToken = aToken;	
		mSQL=aSQL;
		}
	}

	@Override
	public String call() throws Exception {
		result= mSQL.getActivities(mToken); 
		return result;
	}
}