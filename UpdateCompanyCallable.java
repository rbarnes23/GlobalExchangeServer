package com.mapmymotion.globalexchangeserver;

import java.util.concurrent.Callable;

import org.json.JSONException;

import com.tokenlibrary.Token;


class UpdateCompanyCallable implements Callable<Token>{
	private Token mToken;
	private SQL mSQL;

	UpdateCompanyCallable(Token aToken,SQL aSQL) {
		if (aToken != null) {
		mToken = aToken;	
		mSQL=aSQL;
		}
	}

	@Override
	public Token call() throws Exception {
		try {
			// This is a new array for sending messages to particular
			// members
			mSQL.UpsertCompany(mToken);
//			String[] friendsArray = mSQL.UpsertCompany(mToken);
//			if (mToken.isMessage().contentEquals("Y")
//					&& !friendsArray[0].contentEquals("[]")) {
//				mToken.setTokenArray("to", friendsArray[0]);
//				mToken.setTokenArray("names", friendsArray[1]);
//			}
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return mToken;
	}
}