package com.mapmymotion.globalexchangeserver;

/**
 * Nakov Chat Server - (c) Svetlin Nakov, 2002
 *
 * ClientListener class is purposed to listen for client messages and
 * to forward them to ServerDispatcher.
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.swing.JEditorPane;

import org.json.JSONArray;
import org.json.JSONException;

import com.tokenlibrary.Token;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;

public class ClientListener extends Thread {
	private ServerDispatcher mServerDispatcher;
	private ClientInfo mClientInfo;
	private BufferedReader mIn;
	private String[] mFriendsArray;
	// Used to create token object from message
	Token ltoken = new Token();

	// Used to write to database
	private SQL mSQL = new SQL();
	Socket mSocket;
	ExecutorService executorService = Executors.newFixedThreadPool(10);

	public ClientListener(ClientInfo aClientInfo,
			ServerDispatcher aServerDispatcher) throws IOException {
		mClientInfo = aClientInfo;
		mServerDispatcher = aServerDispatcher;
		mSocket = aClientInfo.mSocket;
		mIn = new BufferedReader(
				new InputStreamReader(mSocket.getInputStream()));

		mSQL.dbConnect();
	}

	/**
	 * Until interrupted, reads messages from the client socket, forwards them
	 * to the server dispatcher's queue and notifies the server dispatcher.
	 */
	public void run() {
		try {

			// while(mSocket.isConnected()){
			while (!isInterrupted()) {

				String message = mIn.readLine();
				message = checkToken(message);
				// Check here if a return message is necessary....if not
				// dont send one
				if ((message.length() > 0) || (message.contentEquals("quit23"))) {
					mServerDispatcher.dispatchMessage(mClientInfo, message);
				}
			}
		} catch (Exception ioex) {
			String error = ioex.getMessage();
			ioex.printStackTrace();
		}

		// Communication is broken. Interrupt both listener and sender threads
		mClientInfo.mClientSender.interrupt();
		mSQL.deleteClient(mClientInfo);
		mSQL.Cleanup();
		executorService.shutdown();

	}

	// Might be a good idea to create a thread to call this
	// ...order is not as necessary as getting it off the main thread or worse
	// creating a thread from each message
	// Change gettype to switch statements for minimal evaluations.

	private String checkToken(String aMessage) {
		// if unable to create a token then return the original message for
		// dispatch
		if (aMessage.isEmpty() || aMessage == null) {
			return "";
		}

		if (ltoken.createToken(aMessage).contentEquals("-1")) {
			return aMessage;
		}
		;
		String mResponse = null;
		// Update the databases as necessary
		/*
		 * if (ltoken.getType().contentEquals("ZNW")) { new
		 * insertActivitySummaryThread(ltoken,mSQL);
		 * //mSQL.InsertActivitySummary(ltoken); } if
		 * (ltoken.getType().contentEquals("TEL")) {
		 * //mSQL.InsertActivity(ltoken); new insertActivityThread(ltoken,mSQL);
		 * }
		 */
		if (ltoken.getReply().contentEquals("Y")) {
			// Make activity Summary record in MySql and MongoDb
			if (ltoken.getType().contentEquals("ZNW")) {
				// es.execute(new insertActivitySummaryThread(ltoken,mSQL));
				// new insertActivitySummaryThread(ltoken,mSQL);
				mResponse = mSQL.InsertActivitySummary(ltoken);
			}
			// Make activity record in MySql and MongoDb
			if (ltoken.getType().contentEquals("TEL")) {
				// ObjectId id = new ObjectId(); Use to create a mongodb type
				// object id
				mResponse = mSQL.InsertActivity(ltoken);
				// es.execute(new insertActivityThread(ltoken,mSQL));
			}

			if (ltoken.getType().contentEquals("MOTIONACTIVITY")) {
				// es.execute(new insertActivitySummaryThread(ltoken,mSQL));
				// new insertActivitySummaryThread(ltoken,mSQL);
				try {
					mResponse = mSQL.InsertMotionActivity(ltoken);
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			// Get friends json object for use in friends
			if (ltoken.getType().contentEquals("getfriends")) {
				// new getFriendsThread(ltoken,mSQL);
				mResponse = mSQL.getFriends(ltoken);
			}
			if (ltoken.getType().contentEquals("getactivities")) {
				mResponse = mSQL.getActivities(ltoken);

			}
			// Get points for the GeoPoints section of the Trip Mapping
			if (ltoken.getType().contentEquals("getpoints")) {
				// new getPointsThread(ltoken,mSQL);
				mResponse = mSQL.getPoints(ltoken);
			}
			// Get the Intervals for the Trip
			if (ltoken.getType().contentEquals("getintervals")) {
				// new getPointsThread(ltoken,mSQL);
				mResponse = mSQL.getIntervals(ltoken);
			}
			// Create a new Memberid into a session
			if (ltoken.getType().contentEquals("session")) {
				try {
					mSQL.InsertNewCourseMember(ltoken);
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			if (ltoken.getType().contentEquals("sessionupdate")) {
				try {
					// This is not right....need to inform friends..not
					// everybody and this does not have a "to" section
					ltoken.setTokenArray("members", mFriendsArray[0]);
					ltoken.setTokenArray("to", mFriendsArray[0]);
					mSQL.UpsertSession(ltoken);
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}

			if (ltoken.getType().contentEquals("VEHICLEDATA")) {
				Future<Token> future = executorService
						.submit(new UpdateVehicleCallable(ltoken, mSQL));
				try {
					ltoken = future.get();
					ltoken.setToken("message", ""); // Remove message so we dont

				} catch (InterruptedException | ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			if (ltoken.getType().contentEquals("COMPANYDATA")) {
				Future<Token> future = executorService
						.submit(new UpdateCompanyCallable(ltoken, mSQL));
				try {
					ltoken = future.get();
					ltoken.setToken("message", ""); // Remove message so we dont

				} catch (InterruptedException | ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			if (ltoken.getType().contentEquals("EMPLOYEEDATA")) {
				Future<Token> future = executorService
						.submit(new UpdateEmployeeCallable(ltoken, mSQL));
				try {
					ltoken = future.get();
					ltoken.setToken("message", ""); // Remove message so we dont

				} catch (InterruptedException | ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			// Used for routing user messages to all friends
			if (ltoken.getType().contentEquals("edi")) {
				// mResponse = ltoken.getString("message");

				Future<Token> future = executorService
						.submit(new getEdiCallable(ltoken, mSQL));
				// Token ltoken;
				try {
					ltoken = future.get();
					ltoken.setToken("message", ""); // Remove message so we dont

				} catch (InterruptedException | ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				// try {
				// // This is a new array for sending messages to particular
				// // members
				// String[] friendsArray = mSQL.UpsertEdiMessage(ltoken);
				// if (ltoken.isMessage().contentEquals("Y")
				// && !friendsArray[0].contentEquals("[]")) {
				// ltoken.setTokenArray("to", friendsArray[0]);
				// ltoken.setTokenArray("names", friendsArray[1]);
				// }
				// } catch (JSONException e) {
				// // TODO Auto-generated catch block
				// e.printStackTrace();
				// }
				// ltoken.setToken("message", ""); // Remove message so we dont
				// send it back with response

			}

			// Used for routing user messages to all friends
			if (ltoken.getType().contentEquals("getmessage")) {
				mResponse = ltoken.getString("message");
				ltoken.setTokenArray("to", mFriendsArray[0]);
				ltoken.setTokenArray("names", mFriendsArray[1]);

				try {
					// This is a new array for sending messages to particular
					// members
					String[] friendsArray = mSQL.UpsertMessage(ltoken);
					if (ltoken.isMessage().contentEquals("Y")
							&& !friendsArray[0].contentEquals("[]")) {
						ltoken.setTokenArray("to", friendsArray[0]);
						ltoken.setTokenArray("names", friendsArray[1]);
					}
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				ltoken.setToken("message", ""); // Remove message so we dont
				// send it back with response

			}
			// Following SHO,ACC,INV updates friends status'
			if (ltoken.getType().contentEquals("SHO")) {
				// new getFriendsThread(ltoken,mSQL);
				mResponse = mSQL.updateFriendStatus(ltoken);
				ltoken.setToken("type", "getfriends"); // Change type so i can
														// populate friends list
			}
			if (ltoken.getType().contentEquals("ACC")) {
				// new getFriendsThread(ltoken,mSQL);
				mResponse = mSQL.updateFriendStatus(ltoken);
				ltoken.setToken("type", "getfriends");
			}
			if (ltoken.getType().contentEquals("INV")) {
				// new getFriendsThread(ltoken,mSQL);
				mResponse = mSQL.updateFriendStatus(ltoken);
				ltoken.setToken("type", "getfriends");
			}
			// Used for AutoComplete Search - based on characters entered
			// returns matches
			if (ltoken.getType().contentEquals("findmember")) {
				try {
					mResponse = mSQL.findMember(ltoken);
					// String test=mResponse;
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			if (ltoken.getType().contentEquals("stopinfo")) {
				try {
					String id = ltoken.getString("_id");
					mResponse = mSQL.updateStopInfo(ltoken);
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				ltoken.setToken("message", ""); // Remove message so we dont
			}

			if (ltoken.getType().contentEquals("login")) {
				// Add the client to the Client Session table
				mSQL.addClient(ltoken.getMemberid(), mClientInfo);

				// Make an array of messages
				JSONArray jArray = new JSONArray();

				// Send a list of sessions to the user only
				String sessions = mSQL.getSessions();
				jArray.put(sessions);
				// Now I need to get the users unread messages and send to him
				try {
					jArray = mSQL
							.getMessages(ltoken.getMemberid(), "U", jArray);
					if (mSocket.getPort() == 2004) {
						jArray = mSQL.getEdiMessages(ltoken.getMemberid(), "U",
								jArray);
						JSONArray jVehicles = mSQL.getVehicles("11111");
						JSONArray jCompanies = mSQL.getCompanyList("11111");
						JSONArray jEmployees = mSQL.getEmployeeList("11111");
						String vehicles = ltoken.createVehicleToken(
								ltoken.getMemberid(), jVehicles.toString(),
								true, false);
						String companies = ltoken.createCompanyToken(
								ltoken.getMemberid(), jCompanies.toString(),
								true, false);
						String employees = ltoken.createEmployeeToken(
								ltoken.getMemberid(), jEmployees.toString(),
								true, false);
						mServerDispatcher.dispatchMessage(mClientInfo,
								companies);
						mServerDispatcher
								.dispatchMessage(mClientInfo, vehicles);
						mServerDispatcher.dispatchMessage(mClientInfo,
								employees);
					}
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (mSocket.getPort() == 2003) {
					sessions = ltoken.createSessionsToken(ltoken.getMemberid(),
							jArray.toString(), false, true);

					mServerDispatcher.dispatchMessage(mClientInfo, sessions);
				
				// This message lets everyone except the sender(me) know that I
				// logged in
				mResponse = mSQL.getFriendsArray(ltoken);
				mFriendsArray = ltoken.createFriendsArray(mResponse, ltoken);
				ltoken.setTokenArray("to", mFriendsArray[0]);
				ltoken.setTokenArray("names", mFriendsArray[1]);
				}
				// ltoken.setToken("type", "getmessage");
				ltoken.setToken("message", "Logged In");
			}

			ltoken.setToken("response", mResponse);
			aMessage = ltoken.getTokenString();
		}

		// send the modified message for transmittal
		return aMessage;

	}

}