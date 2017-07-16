package com.mapmymotion.globalexchangeserver;

/**
 * Nakov Chat Server
 * (c) Svetlin Nakov, 2002
 *
 * ServerDispatcher class is purposed to listen for messages received
 * from clients and to dispatch them to all the clients connected to the
 * chat server.
 */

import java.net.*;
import java.util.*;
import java.util.Map.Entry;
import javolution.util.FastMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.MongoException;
import com.tokenlibrary.Token;

//import com.mSQL.jdbc.integration.jboss.mSQLValidConnectionChecker;

public class ServerDispatcher extends Thread {
	private Vector mMessageQueue = new Vector();
	private Vector mClients = new Vector();
	FastMap<String, ClientInfo> mClientMembersmap = new FastMap<String, ClientInfo>();

	/**
	 * Adds given client to the server's client list.
	 */
	public synchronized void addClient(ClientInfo aClientInfo) {
		mClients.add(aClientInfo);
		// mSQL.dbConnect();
		// String message = mSQL.dbMongoConnect() + aClientInfo.toString();
		// Change to be a Json type message of session
		// add to session collection
		String message = "CONNECTED: ";
		message += aClientInfo.sSessionId.toString();
		// sendMessageToClient(message, aClientInfo);

	}

	/**
	 * Get Key from hashmap based on Value .
	 */

	public static <T, E> T getKeyByValue(FastMap<T, E> map, E value) {
		for (Entry<T, E> entry : map.entrySet()) {
			if (value.equals(entry.getValue())) {
				return entry.getKey();
			}
		}
		return null;
	}

	/**
	 * Deletes given client from the server's client list if the client is in
	 * the list.
	 */
	public synchronized void deleteClient(ClientInfo aClientInfo) {
		String key = getClientKey(aClientInfo);
		if (key !=null) {
			mClientMembersmap.remove(key);
		}

		int clientIndex = mClients.indexOf(aClientInfo);
		if (clientIndex != -1) {
			mClients.removeElementAt(clientIndex);
		}
		// REB added 20120114
		int size = mClientMembersmap.size();
		aClientInfo = null;
	}

	/**
	 * Adds given message to the dispatcher's message queue and notifies this
	 * thread to wake up the message queue reader (getNextMessageFromQueue
	 * method). dispatchMessage method is called by other threads
	 * (ClientListener) when a message is arrived.
	 * 
	 * @throws JSONException
	 */

	public synchronized void dispatchMessage(ClientInfo aClientInfo,
			String aMessage) {
		// Socket socket = aClientInfo.mSocket;
		// String senderIP = socket.getInetAddress().getHostAddress();
		// String senderPort = "" + socket.getPort();
		setClientInfo(aMessage, aClientInfo); // tie clientinfo to memberid when
												// login
		// aMessage = senderIP + ":" + senderPort + " : " + aMessage;
		// String test=getClients();
		mMessageQueue.add(aMessage);
		notify();
	}

	private synchronized String getClientKey(ClientInfo aClientInfo) {
		String key = null ;
		for (FastMap.Entry<String, ClientInfo> e = mClientMembersmap.head(), end = mClientMembersmap
				.tail(); (e = e.getNext()) != end;) {
			String comp = e.getValue().toString();
			if (comp.contentEquals(aClientInfo.toString())) {
				key = e.getKey();
				break;
			}
		}
		return key;
	}

	private synchronized String getClients() {
		Token ltoken = new Token();
		ltoken.createNewToken();

		String clientlist = "[";
		if (mClientMembersmap.size() > 0) {
			for (FastMap.Entry<String, ClientInfo> e = mClientMembersmap
					.head(), end = mClientMembersmap.tail(); (e = e.getNext()) != end;) {
				clientlist += e.getKey() + ",";
			}
			clientlist = clientlist.substring(0, clientlist.length() - 1);
		}
		clientlist += "]";

		ltoken.setToken("clients", clientlist);
		return ltoken.getString("clients");

	}

	// This should become obsolete as this is handled at CL->login
	private synchronized void setClientInfo(String aMessage,
			ClientInfo aClientInfo) {
		// String message;

		// if (aMessage.contains("LOGIN")) {
		try {
			JSONObject json_data = new JSONObject(aMessage);
			mClientMembersmap.put(json_data.getString("memberid"), aClientInfo);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// }
	}

	/**
	 * @return and deletes the next message from the message queue. If there is
	 *         no messages in the queue, falls in sleep until notified by
	 *         dispatchMessage method.
	 */
	private synchronized String getNextMessageFromQueue()
			throws InterruptedException {
		while (mMessageQueue.size() == 0)
			wait();
		String message = (String) mMessageQueue.get(0);
		mMessageQueue.removeElementAt(0);
		return message;
	}

	/**
	 * Sends given message to all clients in the client list. Actually the
	 * message is added to the client sender thread's message queue and this
	 * client sender thread is notified.
	 */
	/*
	 * private synchronized void sendMessageToClient(String aMessage,ClientInfo
	 * aClientInfo) { ClientInfo mClientInfo = null; if (aClientInfo!=null){ int
	 * mClientNo = mClients.indexOf(aClientInfo); mClientInfo = (ClientInfo)
	 * mClients.get(mClientNo); mClients.indexOf(mClientInfo); } else { String
	 * msClientInfo = mSQL.findClient(aMessage); for (int
	 * i=0;i<mClients2.size();i++){ // Find the object ClientInfo from the
	 * vector that matches the string clientinfo Map map=new HashMap(); map=
	 * (Map) mClients2.elementAt(i); Object mClient = map.get(msClientInfo);
	 * mClientInfo = (ClientInfo) mClient; if (mClientInfo != null){break;} } }
	 * 
	 * if (mClientInfo !=null){ mClientInfo.mClientSender.sendMessage(aMessage);
	 * }
	 * 
	 * }
	 */

	private synchronized void sendMessageToClient(String aMessage,
			ClientInfo aClientInfo) {
		if (aClientInfo != null) {
			int mClientNo = mClients.indexOf(aClientInfo);
			ClientInfo mClientInfo = (ClientInfo) mClients.get(mClientNo);
			mClientInfo.mClientSender.sendMessage(aMessage);
		}
	}

	/*
	 * If to array is empty don't sent to anyone, otherwise send to members in
	 * array If to array is "0" then send to everyone
	 */
	private synchronized void sendMessageToFriends(String aMessage) {
		ClientInfo mClientInfo = null;
		JSONObject json_data;
		JSONArray json_array;
		String memberid = null;

		try {
			json_data = new JSONObject(aMessage);
			if (json_data.getString("type").contentEquals("edi")) {
				json_array = json_data.getJSONArray("employees");
			} else {
				json_array = json_data.getJSONArray("to");
			}
			for (int i = 0; i < json_array.length(); i++) {
				memberid = json_array.getString(i);

				// If memberid is -1 then don't reply to sender
				if (memberid.contentEquals("-1")) {
					break;
				}

				// Send to all clients if to is equal to "0"
				if (memberid.contentEquals("0")) {
					for (int j = 0; j < mClients.size(); j++) {
						ClientInfo clientInfo = (ClientInfo) mClients.get(j);
						clientInfo.mClientSender.sendMessage(aMessage);
					}

					break;
				}
				mClientInfo = (ClientInfo) mClientMembersmap.get(memberid);
				if (mClientInfo != null) {
					mClientInfo.mClientSender.sendMessage(aMessage);
				}

			}

		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * Sends given message to all clients in the client list. Actually the
	 * message is added to the client sender thread's message queue and this
	 * client sender thread is notified.
	 */
	private synchronized void sendMessageToAllClients(String aMessage) {
		for (int i = 0; i < mClients.size(); i++) {
			ClientInfo clientInfo = (ClientInfo) mClients.get(i);
			clientInfo.mClientSender.sendMessage(aMessage);
		}
	}

	/**
	 * Infinitely reads messages from the queue and dispatch them to all clients
	 * connected to the server.
	 */
	public void run() {

		try {
			while (true) {
				String message = getNextMessageFromQueue();
				if (message.contains("quit23")) {
					System.exit(-1);
				}
				// sendMessageToClient(message,null);
				sendMessageToFriends(message);

				/*
				 * // Get the String representation of the found ClientInfo
				 * object String msClientInfo = mSQL.findClient(message);
				 * 
				 * for (int i=0;i<mClients2.size();i++){ // Find the object
				 * ClientInfo from the vector that matches the string clientinfo
				 * Map map=new HashMap(); map= (Map) mClients2.elementAt(i);
				 * Object mClient = map.get(msClientInfo); ClientInfo
				 * mClientInfo = (ClientInfo) mClient; if (mClientInfo != null){
				 * sendMessageToClient(message,mClientInfo); break;} }
				 */
				// sendMessageToAllClients(message);
			}
		} catch (InterruptedException ie) {
			// Thread interrupted. Stop its execution
		}
	}

}