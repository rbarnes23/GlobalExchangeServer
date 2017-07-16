package com.mapmymotion.globalexchangeserver;

import java.net.UnknownHostException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import javolution.util.FastMap;

import org.bson.types.BasicBSONList;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.tokenlibrary.Token;

public class SQL {
	DB db;
	Connection mConnection;
	String message = "No Message";
	static DBCollection sActivities;
	static DBCollection sClientInfo;
	static DBCollection sSessions;
	static DBCollection sMembers;
	static DBCollection sEmployee;
	static DBCollection sMessages;
	static DBCollection sEdi;
	static DBCollection sRates;
	static DBCollection sCompanies;
	static DBCollection sVehicles;
	Mongo mongo;
	FastMap lKey = new FastMap();
	static ObjectId activityId;
	// String dbName = "artytheartist";
	String dbName = "tx12edi";

	public String dbConnect() {
		String url = "jdbc:mysql://96.31.86.130:3306/";

		// String url = "jdbc:mysql://96.31.86.128:3306/";
		String dbName = "motion";// Note that i dont have a tx12edi sql
									// database now

		String driver = "com.mysql.jdbc.Driver";
		String userName = "rbarnes23";
		String password = "sasha23";
		try {
			Class.forName(driver);
			mConnection = DriverManager.getConnection(url + dbName, userName,
					password);
			mConnection.setAutoCommit(true);
			message = "sql Connected";

		} catch (Exception e) {
			e.printStackTrace();
			message = "Not connected";
		}
		return message;
	}

	public String dbMongoConnect() {
		message = "Mongo Not Connected";
		try {
			// mongo = new Mongo("localhost");
			mongo = new Mongo("96.31.86.129");
			message = "Mongo Connected";
			db = mongo.getDB(dbName);
			if (db.collectionExists("clientinfo")) {
				// FOR TESTING db.getCollection("clientinfo").drop();
			}
			sActivities = db.getCollection("activities");
			sClientInfo = db.getCollection("clientinfo");
			sSessions = db.getCollection("sessions");
			sMembers = db.getCollection("members");
			sEmployee = db.getCollection("employee");
			sMessages = db.getCollection("messages");
			sEdi = db.getCollection("edi");
			sCompanies = db.getCollection("company");
			sVehicles = db.getCollection("vehicles");

			sRates = db.getCollection("rates");

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			mongo = null;
			message = "Unknown Host";
		} catch (MongoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			mongo = null;
			message = "Mongo Exception";
		}

		return message;
	}

	private static BasicDBObject makeActivitySummaryDocument(String _type,
			String _memberid, String _startTime, boolean _resume) {
		BasicDBObject query = new BasicDBObject();
		if (_resume) {
			query.put("memberid", _memberid);
			// query.append(new BasicDBObject("sort", {"_id":-1}));
			// Get the last id used in case of RESUME type operation
			DBCursor cur = sActivities.find(query)
					.sort(new BasicDBObject("_id", -1)).limit(1);
			// TRY THIS LATER DBCursor cur2 = ((DBCursor)
			// sActivities.findOne(query)).sort( new BasicDBObject( "_id" , -1 )
			// );

		} else {
			// If New Activity then get a new id
			activityId = new ObjectId();
		}
		BasicDBObject doc = new BasicDBObject();
		doc.put("type", _type);
		doc.put("memberid", _memberid);
		doc.put("starttime", _startTime);
		ArrayList<BasicDBObject> activityData = new ArrayList<BasicDBObject>();
		doc.put("activitydata", activityData);
		doc.put("_id", activityId);
		return doc;
	}

	private static BasicDBObject makeActivityDocument(String type,
			String memberid, String latitude, String longitude,
			String epochtime, String distance, String altitude) {
		BasicDBObject doc = new BasicDBObject();
		ArrayList<Float> x = new ArrayList<Float>();
		x.add(Float.valueOf(longitude));
		x.add(Float.valueOf(latitude));
		doc.put("loc", x);
		doc.put("epochtime", Long.valueOf(epochtime));
		doc.put("distance", Float.valueOf(distance));
		doc.put("altitude", Float.valueOf(altitude));
		return doc;
	}

	/*
	 * private static BasicDBObject makeActivityDocument(String type, String
	 * memberid, String latitude, String longitude, String epochtime, String
	 * distance, String altitude) { BasicDBObject doc = new BasicDBObject();
	 * doc.put("type", type); doc.put("memberid", memberid); doc.put("latitude",
	 * latitude); doc.put("longitude", longitude); doc.put("epochtime",
	 * Long.valueOf(epochtime)); doc.put("distance", distance);
	 * doc.put("altitude", altitude); // doc.put("activitysummaryid", "2323");
	 * // doc.put("activitysummaryid", Long.valueOf(myActivitySummaryId));
	 * return doc; }
	 */
	private static BasicDBObject makeClientInfoDocument(String _memberid,
			ClientInfo _clientinfo) {
		BasicDBObject doc = new BasicDBObject();
		doc.put("memberid", _memberid);
		doc.put("clientinfo", _clientinfo.toString());
		doc.put("port", Integer.toHexString(_clientinfo.mSocket.getPort()));

		return doc;
	}

	public String getSessions() {
		String mSessions = "[";

		if (mongo == null) {
			dbMongoConnect();
		}
		DBCursor cur = sSessions.find();
		while (cur.hasNext()) {
			mSessions += cur.next().toString() + ",";
		}
		mSessions += "]";
		mSessions = mSessions.replace(",]", "]");
		return mSessions;
	}

	// Purpose is when login tie memberid to clientinfo
	public String addClient(String _memberId, ClientInfo aClientInfo) {
		if (mongo == null) {
			dbMongoConnect();
		}
		BasicDBObject update = new BasicDBObject();
		update = makeClientInfoDocument(_memberId, aClientInfo);
		BasicDBObject query = new BasicDBObject();
		query.put("memberid", _memberId);
		// Dont allow memberid to have more than one connection.
		sClientInfo.update(query, update, true, false);
		message = "Added ClientInfo : " + sClientInfo.getCount();

		return message;
	}

	public void deleteClient(ClientInfo aClientInfo) {
		if (mongo == null) {
			dbMongoConnect();
		}
		BasicDBObject query = new BasicDBObject();
		query.put("clientinfo", aClientInfo.toString());
		Object error = sClientInfo.remove(query);

		String test = error.toString();

		// sClientInfo.findAndRemove(query);
	}

	public void deleteClients() {
		sClientInfo.drop();
	}

	public String findClient(String aMessage) {
		String mClientInfo = null;
		BasicDBObject query = new BasicDBObject();
		JSONObject json_data;
		String mData = null;
		if (mongo == null) {
			dbMongoConnect();
		}

		try {
			json_data = new JSONObject(aMessage);
			query.put("memberid", json_data.getString("memberid"));
			// query.append(new BasicDBObject("sort", {_id:-1}));

			DBObject cur = sClientInfo.findOne(query);
			// DBCursor cur= sClientInfo.find(new
			// BasicDBObject().append("memberid",
			// json_data.getString("memberid"))).sort(new BasicDBObject("_id",
			// -1)).limit(1);
			// DBCursor cur= sClientInfo.find(new
			// BasicDBObject().append("memberid",
			// json_data.getString("memberid"))).limit(1);
			// while(cur.hasNext()){
			// mData = cur.curr().toString();
			// }
			if (cur != null) {
				json_data = new JSONObject(cur.toString());
				mClientInfo = json_data.getString("clientinfo");
			}
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			// return mClientInfo;
		}
		return mClientInfo;
	}

	protected String InsertNewCourseMember(Token aToken) throws JSONException {

		// MONGO STUFF
		if (mongo == null) {
			dbMongoConnect();
		}
		// JSONObject jnewMember;
		BasicDBObject querymember = new BasicDBObject();
		String id = aToken.getNewMemberId();
		querymember.put("id", Integer.parseInt(id));
		DBCursor cur = sMembers.find(querymember).limit(1);

		if (cur != null) {
			if (cur.hasNext()) {
				String r = cur.next().toString();
				// jnewMember = new JSONObject(cur.next().toString());
				String firstname = cur.curr().get("firstname").toString();
				// jnewMember = new JSONObject(cur.next().toString());
				String lastname = cur.curr().get("lastname").toString();
				String _id = cur.curr().get("_id").toString();

				BasicDBObject querysession = new BasicDBObject();
				String courseid = aToken.getCourseId();
				ObjectId companyObj = new ObjectId(courseid);

				querysession.put("_id", companyObj);
				BasicDBObject update = new BasicDBObject();
				update.put(
						"$addToSet",
						new BasicDBObject("members", makeSessionUpdateDocument(
								id, firstname + " " + lastname)));
				sSessions.update(querysession, update, false, false);
			}
		}
		// END MONGO STUFF

		return message;
	}

	protected String UpsertSession(Token aToken) throws JSONException {

		// MONGO STUFF
		if (mongo == null) {
			dbMongoConnect();
		}
		// Query Object
		BasicDBObject query = new BasicDBObject();
		// String courseid = aToken.getCourseId();
		// ObjectId companyObj = new ObjectId(courseid);
		ObjectId companyObj = new ObjectId();
		query.put("_id", companyObj);
		// Update Object
		BasicDBObject update = new BasicDBObject();
		update = makeSessionDocument(aToken.getMemberid(),
				aToken.getString("sessionno"), aToken.getString("sessionname"));

		// Upsert- if query is found update else insert - for now the query will
		// never be found as
		// sSessions.update(query,update,false,false);
		sSessions.insert(update);
		// END MONGO STUFF

		return message;
	}

	protected String UpsertCompany(Token aToken) throws JSONException {
		// MONGO STUFF
		if (mongo == null) {
			dbMongoConnect();
		}
		// Query Object
		BasicDBObject query = new BasicDBObject();
		String messageid = aToken.getID();

		query.put("_id", messageid);

		// Update Object
		JSONObject jMessage = new JSONObject(aToken.getMessage());
		Map map = JsonHelper.toMap(jMessage);
		// Update Object
		BasicDBObject update = new BasicDBObject(map);

		sCompanies.update(query, update, true, false);

		return null;

	}

	protected String UpsertVehicle(Token aToken) throws JSONException {
		// MONGO STUFF
		if (mongo == null) {
			dbMongoConnect();
		}
		// Query Object
		BasicDBObject query = new BasicDBObject();
		String messageid = aToken.getID();

		query.put("_id", messageid);

		// Update Object
		JSONObject jMessage = new JSONObject(aToken.getMessage());
		Map map = JsonHelper.toMap(jMessage);
		// Update Object
		BasicDBObject update = new BasicDBObject(map);

		sVehicles.update(query, update, true, false);

		return null;
	}

	protected String UpsertEmployee(Token aToken) throws JSONException {
		// MONGO STUFF
		if (mongo == null) {
			dbMongoConnect();
		}
		// Query Object
		BasicDBObject query = new BasicDBObject();
		String messageid = aToken.getID();

		query.put("_id", messageid);

		// Update Object
		JSONObject jMessage = new JSONObject(aToken.getMessage());
		Map map = JsonHelper.toMap(jMessage);
		// Update Object
		BasicDBObject update = new BasicDBObject(map);

		sEmployee.update(query, update, true, false);

		return null;
	}

	protected String updateStopInfo(Token aToken) throws JSONException {

		// MONGO STUFF
		if (mongo == null) {
			dbMongoConnect();
		}
		// Query Object
		BasicDBObject query = new BasicDBObject();
		String messageid = aToken.getID();

		query.put("_id", messageid);

		// Update Object
		Map map = JsonHelper.toMap(aToken.getToken());

		JSONArray jStops = new JSONArray();
		DBCursor cur = sEdi.find(query).limit(1);
		while (cur.hasNext()) {
			String curline = cur.next().toString();
			JSONObject jCur = new JSONObject(curline);
			jStops = jCur.optJSONArray("stopinfo");
			if (jStops == null) {
				jStops = new JSONArray();
			}
			JSONObject jStopInfo = new JSONObject();
			jStopInfo.put("position", map.get("position"));
			jStopInfo.put("lon", map.get("lon"));
			jStopInfo.put("lat", map.get("lat"));
			jStopInfo.put("signature", map.get("signature"));
			jStopInfo.put("identificationNo", map.get("identificationNo"));
			int pos = Integer.parseInt((String) map.get("position"));
			jStops.put(jStopInfo);
			jCur.put("stopinfo", jStops);
			// Update Object
			Map curmap = JsonHelper.toMap(jCur);
			BasicDBObject update = new BasicDBObject(curmap);

			sEdi.update(query, update, true, false);
		}
		return null;
	}

	protected String[] UpsertEdiMessage(Token aToken) throws JSONException {

		// MONGO STUFF
		if (mongo == null) {
			dbMongoConnect();
		}

		String[] mFriendsArray = new String[2];
		String senderId = aToken.getString("ISA06");
		String receiverId = aToken.getString("ISA08");

		// Get an distinct employee set and add it to the Token as an array
		HashSet<String> employeeSet = getEmployeeSet(senderId, receiverId);
		aToken.setTokenArray("employees", employeeSet.toString());

		// mstat idea will be deprecated soon for edi as it is redundant
		String mstat = aToken.getString("mStat");
		if (mstat == null) {
			aToken.setTokenArray("mStat", "[]");
		}
		JSONArray mStat = new JSONArray(aToken.getString("mStat"));
		BasicBSONList mStatus = new BasicBSONList();
		mFriendsArray[0] = "[";
		mFriendsArray[1] = "[";
		for (int i = 0; i < mStat.length(); i++) {
			try {
				JSONObject jgroupobject = mStat.getJSONObject(i);
				HashMap<String, String> fieldsMap = new HashMap<String, String>();
				Iterator<String> iter = jgroupobject.keys();
				while (iter.hasNext()) {
					String key = (String) iter.next();

					String value = jgroupobject.getString(key);
					if (key.contentEquals("id")) {
						mFriendsArray[0] += value + ",";
					} else if (key.contentEquals("name")) {
						mFriendsArray[1] += value + ",";
					}
					fieldsMap.put(key, value);
				}
				mStatus.put(i, fieldsMap);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		// Query Object
		BasicDBObject query = new BasicDBObject();
		String messageid = aToken.getID();

		query.put("_id", messageid);

		// Update Object
		Map map = JsonHelper.toMap(aToken.getToken());
		BasicDBObject update = new BasicDBObject(map);

		// Upsert- if query is found update else insert - for now the query will
		// never be found as
		sEdi.update(query, update, true, false);
		// END MONGO STUFF
		mFriendsArray[0] += "]";
		mFriendsArray[0] = mFriendsArray[0].replace(",]", "]"); // Removes comma
																// before
																// closing array
		mFriendsArray[1] += "]";
		mFriendsArray[1] = mFriendsArray[1].replace(",]", "]"); // Removes comma
																// before
																// closing array
		return mFriendsArray;
	}

	private HashSet<String> getEmployeeSet(String senderId, String receiverId) {
		// Used to remove duplicates from the employee list
		HashSet<String> employeeSet = new HashSet<>();
		// Sender ID
		BasicDBObject findSender = new BasicDBObject();
		// String senderid = aToken.getString("ISA06");
		findSender.put("isaId ", senderId);
		DBCursor curSender = sCompanies.find(findSender).limit(1);
		try {
			while (curSender.hasNext()) {
				String curline = curSender.next().toString();
				JSONObject jsoncur = new JSONObject(curline);
				JSONArray jEmployees = jsoncur
						.getJSONArray("authorizedEmployeenos");
				for (int i = 0; i < jEmployees.length(); i++) {
					employeeSet.add(jEmployees.getString(i));
				}
			}

		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			curSender.close();
		}

		// Receiver ID
		BasicDBObject findReceiver = new BasicDBObject();
		// String receiverid = aToken.getString("ISA08");
		findReceiver.put("isaId ", receiverId);
		DBCursor curReceiver = sCompanies.find(findReceiver).limit(1);
		try {
			while (curReceiver.hasNext()) {
				String curline = curReceiver.next().toString();
				JSONObject jsoncur = new JSONObject(curline);
				JSONArray jEmployees = jsoncur
						.getJSONArray("authorizedEmployeenos");
				for (int i = 0; i < jEmployees.length(); i++) {
					employeeSet.add(jEmployees.getString(i));
				}
			}
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			curReceiver.close();
		}
		return employeeSet;
	}

	protected String[] UpsertMessage(Token aToken) throws JSONException {

		// MONGO STUFF
		if (mongo == null) {
			dbMongoConnect();
		}
		/*
		 * // Query Current Group BasicDBObject queryGroup = new
		 * BasicDBObject(); queryGroup.put("sessionNo", "2312"); //
		 * sSessions.find(queryGroup); // Now get the members Array from the
		 * session DBCursor cur = sSessions.find(queryGroup).limit(1);
		 * BasicDBList mStatus = new BasicDBList(); if (cur != null) { if
		 * (cur.hasNext()) { String r = cur.next().toString(); mStatus =
		 * (BasicDBList) cur.curr().get("members"); } }
		 */String[] mFriendsArray = new String[2];

		String mstat = aToken.getString("mStat");
		if (mstat == null) {
			aToken.setTokenArray("mStat", "[]");
		}
		JSONArray mStat = new JSONArray(aToken.getString("mStat"));
		BasicBSONList mStatus = new BasicBSONList();
		mFriendsArray[0] = "[";
		mFriendsArray[1] = "[";
		for (int i = 0; i < mStat.length(); i++) {
			try {
				JSONObject jgroupobject = mStat.getJSONObject(i);
				HashMap<String, String> fieldsMap = new HashMap<String, String>();
				Iterator<String> iter = jgroupobject.keys();
				while (iter.hasNext()) {
					String key = (String) iter.next();

					String value = jgroupobject.getString(key);
					if (key.contentEquals("id")) {
						mFriendsArray[0] += value + ",";
					} else if (key.contentEquals("name")) {
						mFriendsArray[1] += value + ",";
					}
					fieldsMap.put(key, value);
				}
				mStatus.put(i, fieldsMap);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		// Query Object
		BasicDBObject query = new BasicDBObject();
		String messageid = aToken.getID();

		query.put("_id", messageid);

		// Update Object
		BasicDBObject update = new BasicDBObject();
		update = makeMessageUpdateDocument(messageid,
				aToken.getString("message"), aToken.isMessage(),
				aToken.getMemberid(), aToken.getReply(), aToken.getSelf(),
				aToken.getType(), mStatus);

		// Upsert- if query is found update else insert - for now the query will
		// never be found as
		sMessages.update(query, update, true, false);
		// END MONGO STUFF
		mFriendsArray[0] += "]";
		mFriendsArray[0] = mFriendsArray[0].replace(",]", "]");
		mFriendsArray[1] += "]";
		mFriendsArray[1] = mFriendsArray[1].replace(",]", "]");
		return mFriendsArray;
	}

	/**
	 * getMessages - Get list of all message for a member
	 * 
	 * @param memberid
	 * @param aType
	 *            - Note the aType is (N)ot read ,(R)ead,(S)ent,(U)nsent
	 * @param jMessages
	 * @return JSONArray - jMessages
	 * @throws JSONException
	 */
	protected JSONArray getMessages(String memberid, CharSequence aType,
			JSONArray jMessages) throws JSONException {
		// String messages= "[";
		// JSONArray jMessages=new JSONArray();
		if (mongo == null) {
			dbMongoConnect();
		}
		BasicDBObject querymessages = new BasicDBObject();
		querymessages.put("isMsg", "Y");
		querymessages.put("mStat.id", memberid);
		querymessages.put("mStat.status", aType);

		DBCursor cur = sMessages.find(querymessages).sort(
				new BasicDBObject("_id", -1));
		while (cur.hasNext()) {
			// messages += cur.next().toString() + ",";
			jMessages.put(cur.next());
		}
		// messages += "]";
		// messages = messages.replace(",]", "]");

		return jMessages;

	}

	/**
	 * getRates - Get list of all Rates for a sender and receiver
	 * 
	 * @param senderId
	 * @param receiverId
	 * @return
	 * @throws JSONException
	 */

	protected JSONArray getRates(String senderId, String receiverId)
			throws JSONException {
		JSONArray jRates = new JSONArray();
		if (mongo == null) {
			dbMongoConnect();
		}
		BasicDBObject queryrates = new BasicDBObject();
		queryrates.put("senderId", senderId);
		queryrates.put("receiverId", receiverId);

		DBCursor cur = sRates.find(queryrates).sort(
				new BasicDBObject("fromCity", 1));
		while (cur.hasNext()) {
			jRates.put(cur.next());
		}
		return jRates;
	}

	/**
	 * getVehicles - Get list of all Vehicles for a company
	 * 
	 * @param companyId
	 * @param jVehicles
	 * @return
	 * @throws JSONException
	 */

	public JSONArray getVehicles(String companyId) throws JSONException {

		JSONArray jVehicles = new JSONArray();
		if (mongo == null) {
			dbMongoConnect();
		}
		BasicDBObject queryvehicles = new BasicDBObject();
		// queryvehicles.put("company_id", new ObjectId(companyId));
		// Not using filter for now
		DBCursor cur = sVehicles.find()
				.sort(new BasicDBObject("company_no", 1));
		while (cur.hasNext()) {
			jVehicles.put(cur.next());
		}
		return jVehicles;
	}

	/**
	 * getCompanyList - Get list of all Companies
	 * 
	 * @param companyId
	 * @param jCompanies
	 * @return JSONArray
	 * @throws JSONException
	 */

	public JSONArray getCompanyList(String companyId) throws JSONException {

		JSONArray jCompanies = new JSONArray();
		if (mongo == null) {
			dbMongoConnect();
		}
		BasicDBObject queryCustomers = new BasicDBObject();
		// queryCustomers.put("tradingpartnerids", new ObjectId(companyId));

		// This is not using above filter queryCustomers at all for testing
		DBCursor cur = sCompanies.find().sort(
				new BasicDBObject("companyname", 1));
		while (cur.hasNext()) {
			jCompanies.put(cur.next());
		}
		return jCompanies;
	}

	/**
	 * getEmployeeList - Get list of all Employees
	 * 
	 * @param companyId
	 * @return JSONArray
	 * @throws JSONException
	 */

	public JSONArray getEmployeeList(String companyId) throws JSONException {

		JSONArray jEmployees = new JSONArray();
		if (mongo == null) {
			dbMongoConnect();
		}
		BasicDBObject query = new BasicDBObject();
		// query.put("company_id", new ObjectId(companyId));

		// This is not using above filter queryCustomers at all for testing
		DBCursor cur = sEmployee.find().sort(
				new BasicDBObject("lastname", 1).append("firstname", 1));
		while (cur.hasNext()) {
			jEmployees.put(cur.next());
		}
		return jEmployees;
	}

	/**
	 * getEdiMessages - Get list of all Edi message for a member
	 * 
	 * @param memberid
	 * @param aType
	 *            - Note the aType is (N)ot read ,(R)ead,(S)ent,(U)nsent
	 * @param jMessages
	 * @return
	 * @throws JSONException
	 */
	protected JSONArray getEdiMessages(String memberid, CharSequence aType,
			JSONArray jMessages) throws JSONException {
		if (mongo == null) {
			dbMongoConnect();
		}
		BasicDBObject querymessages = new BasicDBObject();
		querymessages.put("employees", Integer.parseInt(memberid));

		DBCursor cur = sEdi.find(querymessages).sort(
				new BasicDBObject("_id", -1));
		while (cur.hasNext()) {
			// JSONObject curEdiMessage = new JSONObject();
			DBObject curEdiMessage = cur.next();
			// Now find rates for each sender and attach to current message
			String senderId = curEdiMessage.get("ISA06").toString();
			String receiverId = curEdiMessage.get("ISA08").toString();
			JSONArray jRates = new JSONArray();
			jRates = getRates(senderId, receiverId);
			if (jRates.length() > 0) {
				curEdiMessage.put("RATES", jRates);
			}
			jMessages.put(curEdiMessage.toMap());
			// jMessages.put(cur.curr());
		}
		return jMessages;
	}

	protected String findMember(Token aToken) throws JSONException {
		String mMembers = "[";
		if (mongo == null) {
			dbMongoConnect();
		}
		BasicDBObject querymember = new BasicDBObject();
		String knownas = aToken.getString("knownas");
		querymember.put("knownas", java.util.regex.Pattern.compile(knownas));
		DBCursor cur = sMembers.find(querymember).sort(
				new BasicDBObject("lastname", 1));

		while (cur.hasNext()) {
			mMembers += cur.next().toString() + ",";
		}
		mMembers += "]";
		mMembers = mMembers.replace(",]", "]");
		return mMembers;

	}

	private static BasicDBObject makeSessionUpdateDocument(String id,
			String name) {
		BasicDBObject doc = new BasicDBObject();
		doc.put("name", name);
		doc.put("id", id);
		return doc;
	}

	private static BasicDBObject makeSessionDocument(String memberid,
			String sessionno, String sessionname) {
		BasicDBObject doc = new BasicDBObject();
		doc.put("sessionName", sessionname);
		doc.put("sessionNo", sessionno);
		doc.put("moderatorid", memberid);
		// BasicBSONList creates an empty array called members
		doc.put("members", new BasicBSONList());

		return doc;
	}

	private BasicDBObject makeMessageUpdateDocument(String _id, String message,
			String isMsg, String memberid, String reply, String self,
			String type, BasicBSONList mStat) {
		BasicDBObject doc = new BasicDBObject();
		doc.put("message", message);
		doc.put("_id", _id);
		doc.put("isMsg", isMsg);
		doc.put("MSG", isMsg.contentEquals("Y") ? true : false);
		doc.put("memberid", memberid);
		doc.put("reply", reply);
		doc.put("self", self);
		doc.put("type", type);
		/*
		 * for (int i = 0; i < _mStat.size(); i++) { HashMap<String, String>
		 * addStatus = (HashMap<String, String>) _mStat .get(i);
		 * addStatus.put("status", "U"); _mStat.put(i, addStatus); }
		 *//*
			 * BasicBSONList BL = new BasicBSONList(); HashMap<String, String>
			 * test = new HashMap(); test.put("id", "222"); test.put("name",
			 * "Jack Morris"); test.put("status", "U"); BL.add(test);
			 */
		doc.put("mStat", mStat);

		return doc;
	}

	protected String InsertMotionActivity(Token aToken) throws JSONException,
			SQLException {
		// 1) insert activity_assignment
		// 2) insert activity_events
		// 3) insert all motions
		if (mConnection == null || mConnection.isClosed()) {
			message = dbConnect();
		}
		Statement st = mConnection.createStatement();
		
		JSONObject jMotionSummary = new JSONObject(aToken.getMessage());
		JSONArray jEvents = jMotionSummary.getJSONArray("events");
		JSONArray jMotions = jMotionSummary.getJSONArray("motions");

		String SQLString = null;
		// Parse the string
		SQLString = "Insert into motion.activity_assignment (activityid,memberid) "
				+ "values('"
				+ jMotionSummary.getString("activityId")
				+ "','"
				+ jMotionSummary.getString("memberId") + "')";

		st.executeUpdate(SQLString);
		
		
		for (int i = 0; i < jEvents.length(); i++) {
			JSONObject jEvent = jEvents.getJSONObject(i);
			SQLString = null;
			// Parse the string
			SQLString = "Insert into motion.activity_events (activityid,eventtype,eventsubtype,eventtime) "
					+ "values('"
					+ jEvent.getString("activityid")
					+ "','"
					+ jEvent.getInt("eventtype")
					+ "','"
					+ jEvent.getInt("eventsubtype")
					+ "','"
					+ jEvent.getLong("eventtime") + "')";

			st.executeUpdate(SQLString);
			// NOT USED AS AUTOCOMMIT=TRUE mConnection.commit();
			message = "SQL Update Successful";

		}

		for (int i = 0; i < jMotions.length(); i++) {
			JSONObject jMotion = jMotions.getJSONObject(i);
			SQLString = null;
			// Parse the string
			SQLString = "Insert into motion.activity (latitude,longitude,epochtime,distance,altitude,activityid) "
					+ "values('"
					+ jMotion.getDouble("latitude")
					+ "','"
					+ jMotion.getDouble("longitude")
					+ "','"
					+ jMotion.getLong("currentTime")
					+ "','"
					+ jMotion.getDouble("currentDistance")
					+ "','"
					+ jMotion.getDouble("currentAltitude")
					+ "','"
					+ jMotion.getString("activityid") + "')";
			st.executeUpdate(SQLString);
			// NOT USED AS AUTOCOMMIT=TRUE mConnection.commit();
			message = "SQL Update Successful";
		}

		return message;
	}

	protected String InsertActivity(Token aToken) {

		String SQLString = null;
		// Parse the string
		SQLString = "Insert into mapmymotion.activity (type,memberid,latitude,longitude,epochtime,distance,altitude,activitysummaryid) "
				+ "values('"
				+ aToken.getActivityType()
				+ "','"
				+ aToken.getMemberid()
				+ "','"
				+ aToken.getLatitude()
				+ "','"
				+ aToken.getLongitude()
				+ "','"
				+ aToken.getTime()
				+ "','"
				+ aToken.getDistance()
				+ "','"
				+ aToken.getAltitude()
				+ "','"
				+ lKey.get(aToken.getMemberid()) + "')";
		message = SQLString;
		// MONGO STUFF
		if (mongo == null) {
			dbMongoConnect();
		}

		BasicDBObject query = new BasicDBObject();
		query.put("_id", activityId);
		BasicDBObject update = new BasicDBObject();
		update.put(
				"$push",
				new BasicDBObject("activitydata", makeActivityDocument(
						aToken.getActivityType(), aToken.getMemberid(),
						aToken.getLatitude(), aToken.getLongitude(),
						aToken.getTime(), aToken.getDistance(),
						aToken.getAltitude())));
		sActivities.update(query, update, false, false);
		// END MONGO STUFF

		// Make a connection if neccesary
		try {
			if (mConnection == null || mConnection.isClosed()) {
				message = dbConnect();
			}
			Statement st = mConnection.createStatement();
			st.executeUpdate(SQLString);
			// NOT USED AS AUTOCOMMIT=TRUE mConnection.commit();
			message = "SQL Update Successful";

		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			message = "ERROR1";
		}
		return message;
	}

	/*
	 * protected String InsertActivityMongo(String s) { String[] temp = null;
	 * String SQLString = null; JSONObject json_data = null; // Parse the string
	 * if (s.contains("TEL")) {
	 * 
	 * delimiter String delimiter = "\\|"; temp = s.split(delimiter); print
	 * substrings if (temp.length > 0) {
	 * 
	 * SQLString =
	 * "Insert into mapmymotion.activity (tcpipaddress,type,memberid,latitude,longitude,epochtime,distance,altitude) values('"
	 * + temp[0] + "','" + temp[8] + "','" + temp[2] + "','" + temp[3] + "','" +
	 * temp[4] + "','" + temp[5] + "','" + temp[6] + "','" + temp[7] + "')";
	 * 
	 * message = SQLString;
	 * 
	 * if(mongo==null){ dbMongoConnect(); } try { json_data = new JSONObject(s);
	 * sActivities.insert(makeActivityDocument(
	 * json_data.getString("activitytype"), json_data.getString("memberid"),
	 * json_data.getString("latitude"), json_data.getString("longitude"),
	 * json_data.getString("epochtime"), json_data.getString("distance"),
	 * json_data.getString("altitude"))); message = "Total Records : " +
	 * sActivities.getCount();
	 * 
	 * } catch (JSONException e) { // TODO Auto-generated catch block
	 * e.printStackTrace(); }
	 * 
	 * // Make a connection if neccesary
	 * 
	 * try { if (mConnection == null || mConnection.isClosed()) {message =
	 * dbConnect();} Statement st = mConnection.createStatement();
	 * st.executeUpdate(SQLString); //NOT USED AS AUTOCOMMIT=TRUE
	 * mConnection.commit(); message = "SQL Update Successful";
	 * 
	 * 
	 * } catch (SQLException e1) { // TODO Auto-generated catch block
	 * e1.printStackTrace(); message="ERROR1"; } }
	 * 
	 * }
	 * 
	 * return message;
	 * 
	 * } } return message; }
	 */
	protected String InsertActivitySummary(Token aToken) {

		String SQLStringA, SQLStringB = null;
		ResultSet rs;
		if (aToken.getContinue().contentEquals("Y")) {
			// Used to Continue last activity
			SQLStringA = "Select max(id) from mapmymotion.activitysummary where memberid ="
					+ aToken.getMemberid();
		} else {
			// Set the endtime of the last activitysummary
			SQLStringB = "Update mapmymotion.activitysummary set endtime=(select max(epochtime) from mapmymotion.activity where memberid="
					+ aToken.getMemberid()
					+ ") where memberid ="
					+ aToken.getMemberid() + " order by starttime desc limit 1";
			// Insert the new ActivitySummary
			SQLStringA = "Insert into mapmymotion.activitysummary(memberid,starttime,activitytype) values('"
					+ aToken.getMemberid()
					+ "','"
					+ aToken.getStartTime()
					+ "','" + aToken.getActivityType() + "')";
			message = SQLStringB;
			// Make a connection if neccesary
		}
		// Mongo
		if (mongo == null) {
			dbMongoConnect();
		}

		sActivities
				.insert(makeActivitySummaryDocument(aToken.getActivityType(),
						aToken.getMemberid(), aToken.getStartTime(),
						Boolean.getBoolean(aToken.getContinue())));
		// End Mongo
		try {
			while (mConnection == null || mConnection.isClosed()) {
				message = dbConnect();
			}
			Statement st = mConnection.createStatement();
			if (aToken.getContinue().contentEquals("Y")) {
				st.executeQuery(SQLStringA);
				rs = st.getResultSet();
				if (!rs.first()) {
					st.executeUpdate(SQLStringB);

					st.executeUpdate(SQLStringA,
							Statement.RETURN_GENERATED_KEYS);

					rs = st.getGeneratedKeys();

				}
			} else {
				st.executeUpdate(SQLStringB);

				st.executeUpdate(SQLStringA, Statement.RETURN_GENERATED_KEYS);

				rs = st.getGeneratedKeys();
			}
			// Previously used work next
			if (rs.first()) {
				lKey.put(aToken.getMemberid(), rs.getInt(1));
			} else {

				// throw an exception from here
			}

			// NOT USED AS AUTOCOMMIT=TRUE mConnection.commit();
			message = "SQL Update Successful";

		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			// message="ERROR";
			message = "ERROR:" + e1.getMessage();
		}
		return message;
	}

	protected String getActivities(Token aToken) {
		String SQLString = null;
		// Parse the string
		// SQLString = "call getactivities3('"+ aToken.getMemberid() +"','" +
		// aToken.getString("friends") + "')";
		SQLString = "call getactivities4('" + aToken.getMemberid() + "','"
				+ "0" + "','" + aToken.getString("UOM") + "')";

		message = SQLString;
		// Make a connection if neccesary
		try {
			if (mConnection == null || mConnection.isClosed()) {
				message = dbConnect();
			}
			Statement st = mConnection.createStatement();
			ResultSet rs = st.executeQuery(SQLString);

			// Fetch each row from the result set
			message = "";
			while (rs.next()) {
				// Get the data from the row using the column name
				// message += rs.getString("memberid")+ " " +
				// rs.getString("starttime")+ " " + rs.getString("initials") +
				// " " + rs.getString("localdate")+",";
				message += rs.getString("json");
				// message += rs.getString("memberid");
			}

			// NOT USED AS AUTOCOMMIT=TRUE mConnection.commit();
			// message = "SQL Update Successful";

		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			message = "ERROR1";
		}

		return message;
	}

	protected String getIntervals(Token aToken) {
		String SQLString = null;
		// Parse the string
		// SQLString = "call getactivities2('"+
		// activityToken.getString("memberid") +"','" +
		// activityToken.getString("friends") + "')";
		String mUOM = aToken.getString("UOM").contentEquals("km") ? "1000"
				: "1609.344";
		SQLString = "call getintervals('"
				+ aToken.getString("activitysummaryid") + "','" + mUOM + "')";

		message = SQLString;
		// Make a connection if necessary
		try {
			if (mConnection == null || mConnection.isClosed()) {
				message = dbConnect();
			}
			Statement st = mConnection.createStatement();
			ResultSet rs = st.executeQuery(SQLString);

			// Fetch each row from the result set
			message = "";
			while (rs.next()) {
				// Get the data from the row using the column name
				message += rs.getString("json");
			}

			// NOT USED AS AUTOCOMMIT=TRUE mConnection.commit();
			// message = "SQL Update Successful";

		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			message = "ERROR1";
		}

		return message;
	}

	protected String getPoints(Token aToken) {
		String SQLString = null;
		// Parse the string
		SQLString = "call getpoints('" + aToken.getString("activitysummaryid")
				+ "')";

		message = SQLString;
		// Make a connection if neccesary
		try {
			if (mConnection == null || mConnection.isClosed()) {
				message = dbConnect();
			}
			Statement st = mConnection.createStatement();
			ResultSet rs = st.executeQuery(SQLString);

			// Fetch each row from the result set
			message = "";
			while (rs.next()) {
				// Get the data from the row using the column name
				// message += rs.getString("memberid")+ " " +
				// rs.getString("starttime")+ " " + rs.getString("initials") +
				// " " + rs.getString("localdate")+",";
				message += rs.getString("json");
			}

		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			message = "ERROR1";
		}

		return message;
	}

	protected String getFriends(Token aToken) {
		String SQLString = null;
		SQLString = "call getfriends('" + aToken.getString("memberid") + "','"
				+ aToken.getString("acceptedoption") + "')";
		// SQLString="select distinct friendsid as memberid,firstname,lastname,showlocation,showemail,showphoneno,accepted from groupmembers,members  where id=friendsid and  accepted=1 and memberid='"+
		// email
		// +"' union select distinct memberid,firstname,lastname, showlocation,showemail,showphoneno,accepted from groupmembers,members  where  id=memberid and accepted=1 and friendsid='"+
		// email +"'";
		message = SQLString;
		// Make a connection if neccesary
		try {
			if (mConnection == null || mConnection.isClosed()) {
				message = dbConnect();
			}
			Statement st = mConnection.createStatement();
			ResultSet rs = st.executeQuery(SQLString);

			// Fetch each row from the result set
			message = "";
			while (rs.next()) {
				message += rs.getString("json");
			}

		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			message = "ERROR1";
		}

		return message;
	}

	protected String getFriendsArray(Token aToken) {
		String SQLString = null;
		SQLString = "call getfriendsarray('" + aToken.getString("memberid")
				+ "')";
		message = SQLString;
		// Make a connection if necessary
		try {
			if (mConnection == null || mConnection.isClosed()) {
				message = dbConnect();
			}
			Statement st = mConnection.createStatement();
			ResultSet rs = st.executeQuery(SQLString);

			// Fetch each row from the result set
			message = "";
			while (rs.next()) {
				message += rs.getString("json");
			}
			if (message == null) {
				message = "[-1]";
			}

		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			message = "ERROR3";
		}

		return message;
	}

	public String InsertActivities(StringBuffer toSend) {
		String message = "No Message";
		// Send data
		String act = toSend.toString();
		String[] templines = null;

		try {

			// Parse the string
			/* delimiter */
			String delimiter = "\n";
			templines = act.split(delimiter);
			if (templines.length < 1) {
				message = "No Activities found";
			}
			;

			// Now that we have a list of activities lets parse each line and
			// write to file
			for (int i = 0; i < templines.length; i++) {
				// message = InsertActivity(templines[i]);
			}
			toSend.setLength(0);
		} catch (Exception m) {
			message = "Unable to Update";
		}
		return message;
	};

	protected String getFriends(String amemberid, String aoptions) {
		String SQLString = null;
		ResultSet rs = null;
		// Parse the string
		SQLString = "{call getFriends(" + amemberid + "," + aoptions + ")}";
		message = SQLString;
		// Make a connection if neccesary
		try {
			if (mConnection == null || mConnection.isClosed()) {
				message = dbConnect();
			}
			CallableStatement st = mConnection.prepareCall(SQLString);
			rs = st.executeQuery();
			if (rs.next()) {
				message = rs.getString(1);
			}

		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			message = "ERROR1";
		}

		return message;
	}

	protected String updateFriendStatus(Token aToken) {
		ResultSet rs = null;
		String SQLString = null;
		// Parse the string
		SQLString = "call updategroupmember(" + aToken.getString("memberid")
				+ "," + aToken.getString("friendsid") + ",\""
				+ aToken.getString("type") + "\")";
		message = SQLString;

		// Make a connection if necessary
		try {
			if (mConnection == null || mConnection.isClosed()) {
				message = dbConnect();
			}
			Statement st = mConnection.createStatement();
			rs = st.executeQuery(SQLString);
			// NOT USED AS AUTOCOMMIT=TRUE mConnection.commit();
			if (rs.next()) {
				message = rs.getString(1);
			}

			// message = "SQL Update Successful";

		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			message = "ERROR1";
		}
		return message;
	}

	public void Cleanup() {

		try {
			// if (!mConnection.isClosed()) {
			mConnection.close();
			mongo.close();
			// }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
