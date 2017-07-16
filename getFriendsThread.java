package com.mapmymotion.globalexchangeserver;

import com.tokenlibrary.Token;

//getFriends Thread
public class getFriendsThread implements Runnable {
	private Token ltoken;
	private SQL mSQL;
	private String mResponse;
	getFriendsThread(Token aToken,SQL aMsql ) {
		ltoken = aToken;
		mSQL=aMsql;
		Thread t = new Thread(this);
		t.start();
	}
	
	public String getResponse(){
		String mReply = null;
		while (mReply==null){
			mReply=mResponse;
			try {
				wait(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return mResponse;
	}


	public void run() {
		mResponse=mSQL.getFriends(ltoken);
	}
}
