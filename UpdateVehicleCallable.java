package com.mapmymotion.globalexchangeserver;

import java.util.concurrent.Callable;

import org.json.JSONException;

import com.tokenlibrary.Token;


class UpdateVehicleCallable implements Callable<Token>{
	private Token mToken;
	private SQL mSQL;

	UpdateVehicleCallable(Token aToken,SQL aSQL) {
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
			mSQL.UpsertVehicle(mToken);
//			String[] friendsArray = mSQL.UpsertEdiMessage(mToken);
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