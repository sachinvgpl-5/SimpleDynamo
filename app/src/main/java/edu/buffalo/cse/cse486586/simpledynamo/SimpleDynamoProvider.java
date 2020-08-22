package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StreamCorruptedException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.nfc.Tag;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	static final int SERVER_PORT = 10000;
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static String node_id;
	static String myPort;
	static String portStr;
	static String pref_port1;
	static String pref_port2;
	static String pred_port1;
	static String pred_port2;
	final static String[] nodes = {"5554", "5556", "5558", "5560", "5562"};
	final HashMap<String, String> getNode= new HashMap<String, String>() {{
		try {
			for(String s: nodes) {
				put(genHash(s), String.valueOf(Integer.parseInt(s)*2));
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}};
	final ArrayList<String> hashedNodes = new ArrayList<String>() {{
		try {
			for(String s: nodes) {
				add(genHash(s));
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}};

	static int key_count = -1;
	static int replica_1_count = -1;
	static int replica_2_count = -1;
	static ArrayList<String> object_list = new ArrayList<String>();
	static ArrayList<String> replica_1_list = new ArrayList<String>();
	static ArrayList<String> replica_2_list = new ArrayList<String>();

	static ArrayList<String> del_list_1 = new ArrayList<String>();
	static ArrayList<String> del_list_2 = new ArrayList<String>();


	private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
	/**
	 * buildUri() demonstrates how to build a URI for a ContentProvider.
	 *
	 * @param scheme
	 * @param authority
	 * @return the URI
	 */
	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	private String getSucc(String type, String node) {
		int index;
		String succ1, succ2;
		try {
			node_id = genHash(node.trim());


			if(type.equals("node"))
			{

				index = hashedNodes.indexOf(node_id);
				succ1 = getNode.get(hashedNodes.get((index+1)%5));
				succ2 = getNode.get(hashedNodes.get((index+2)%5));
				return succ1+","+succ2;
			}
			else
			{
				int i = 0;
				while(i<5) {
					if (node_id.compareTo(hashedNodes.get(i)) < 0) {
						break;
					} else {
						i += 1;
					}
				}
				if(i==0 || i==5)
				{
					return getNode.get(hashedNodes.get(0));
				}
				else
				{
					return getNode.get(hashedNodes.get(i));
				}
			}

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}

	private String getPred(String node) {
		int index;
		String pred1, pred2;
		try {
			node_id = genHash(node);
			index = hashedNodes.indexOf(node_id);
			if(index == 0)
			{
				pred1 = getNode.get(hashedNodes.get(4));
				pred2 = getNode.get(hashedNodes.get(3));
			}
			else if(index == 1)
			{
				pred1 = getNode.get(hashedNodes.get(index-1));
				pred2 = getNode.get(hashedNodes.get(4));
			}
			else
			{
				pred1 = getNode.get(hashedNodes.get(index-1));
				pred2 = getNode.get(hashedNodes.get(index-2));
			}
			Log.v("pred1", pred1);
			Log.v("pred2", pred2);
			return pred1+","+pred2;

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}


	@Override
	public boolean onCreate() {
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		Collections.sort(hashedNodes);
		String[] pref_ports = getSucc("node", portStr).split(",");
		pref_port1 = pref_ports[0];
		pref_port2 = pref_ports[1];
		Log.v("pref ports", pref_port1+","+pref_port2);
		String[] predports = getPred(portStr).split(",");
		pred_port1 = predports[0];
		pred_port2 = predports[1];

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerJoinTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

			new Recovery().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, pref_port1, "pred", myPort);
			new Recovery().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, pref_port2, "pred", myPort);
			new Recovery().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, pred_port1, "succ", myPort);
			new Recovery().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, pred_port2, "succ", myPort);

			new Recovery().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, pref_port1, "del", myPort);
			new Recovery().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, pref_port2, "del", myPort);


		} catch (IOException e) {
			/*
			 * Log is a good way to debug your code. LogCat prints out all the messages that
			 * Log class writes.
			 *
			 * Please read http://developer.android.com/tools/debugging/debugging-projects.htmlmyPort
			 * and http://developer.android.com/tools/debugging/debugging-log.html
			 * for more information on debugging.
			 */
			Log.e(TAG, "Can't create a ServerSocket");
			Log.getStackTraceString(e);
		}

		return false;
	}


	private class ServerJoinTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... serverSockets) {
			ServerSocket serverSocket = serverSockets[0];
			Socket clientSocket = null;
			BufferedReader in = null;
			PrintWriter out = null;
			String inputString;

			while (true) {

				try
				{
					clientSocket = serverSocket.accept();
					in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
					out = new PrintWriter(clientSocket.getOutputStream(), true);
					if ((inputString = in.readLine()) != null)
					{
						String[] split_input = inputString.split(",");
						if(split_input[0].equals("insert"))
						{
							ContentValues cv = new ContentValues();
							cv.put(KEY_FIELD, split_input[1]);
							cv.put(VALUE_FIELD, split_input[2]);
							try {
								insert(mUri, cv);
							} catch (Exception ex) {
								Log.e(TAG, ex.toString());
							}
							out.println("success");
						}

						else if(split_input[0].equals("replication"))
						{
							String remote_port = split_input[1];
							String obj_id = split_input[2];
							String key = split_input[3];
							String value = split_input[4];
							ContentValues cv = new ContentValues();
							cv.put(KEY_FIELD, "replica,"+key);
							cv.put(VALUE_FIELD, value);
							try {
								insert(mUri, cv);
//								Log.v(myPort, "replica created");
							} catch (Exception ex) {
								Log.e(TAG, ex.toString());
							}
							if(remote_port.equals(pred_port1)) {
								replica_1_count += 1;
								replica_1_list.add(key+","+obj_id+","+replica_1_count);
//								for(String s: replica_1_list)
//								{
//									Log.v("replica list 1", s);
//								}
							}
							else if(remote_port.equals(pred_port2))
							{
								replica_2_count += 1;
								replica_2_list.add(key+","+obj_id+","+replica_2_count);
//								for(String s: replica_2_list)
//								{
//									Log.v("replica list 2", s);
//								}
							}
							out.println("success");
						}

						else if(split_input[0].equals("query"))
						{
							int keyIndex;
							int valueIndex;
							String return_string = "";
							String returnKey = "";
							String returnValue = "";
							if(split_input[1].equals("*"))
							{
								Cursor resultCursor = query(mUri, null, "@", null, null);

								keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
								valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
//                                        resultCursor.moveToFirst();
								for(resultCursor.moveToFirst(); !resultCursor.isAfterLast(); resultCursor.moveToNext())
								{
									returnKey = resultCursor.getString(keyIndex);
									returnValue = resultCursor.getString(valueIndex);
									return_string = return_string + returnKey + "," + returnValue + ";";
//									Log.v("val = ", return_string);
									resultCursor.close();

								}
								out.println(return_string);
							}
							else if(split_input[1].equals("pref"))
							{
								Log.v("selection being Qed:", split_input[2]);
								Cursor resultCursor = query(mUri, null, "pref,"+split_input[2].trim(), null, null);

								keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
								valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);

								resultCursor.moveToFirst();

								returnKey = resultCursor.getString(keyIndex);
								returnValue = resultCursor.getString(valueIndex);
								out.println((returnKey+","+returnValue));
								resultCursor.close();
							}
							else
							{
								Log.v("selection being Qed:", split_input[1]);
								Cursor resultCursor = query(mUri, null, split_input[1].trim(), null, null);

								keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
								valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);

								resultCursor.moveToFirst();

								returnKey = resultCursor.getString(keyIndex);
								returnValue = resultCursor.getString(valueIndex);
								out.println((returnKey+","+returnValue));
								resultCursor.close();
							}

						}
						else if(split_input[0].equals("delete"))
						{
							try {
								if(split_input[1].equals("*"))
								{
									if(delete(mUri, "@", null) == 1)
										Log.v(myPort, "deleted all my files");
									else
										Log.v(myPort, "Could not delete all my files");

									out.println("success");
								}
								else {

									if(split_input[2].equals("replica"))
									{
										Log.v("file to delete", split_input[1]);
										delete(mUri, "replica"+','+split_input[1], null);
										out.println("success");
									}
									else
									{
										Log.v("file to delete", split_input[1]);
										delete(mUri, split_input[1],null);
										out.println("success");
									}

								}

							}
							catch (Exception ex) {
								Log.e(TAG, ex.toString());
							}

						}
						else if(split_input[0].equals("prefdelete"))
						{
							Log.v("file to delete", split_input[1]);
							delete(mUri, "replica"+','+split_input[1], null);
							out.println("success");
							if(getSucc("key", split_input[1]).equals(pred_port1))
							{
								del_list_1.add(split_input[1]);
							}
							else
							{
								del_list_2.add(split_input[1]);
							}
						}


						else if(split_input[0].equals("prefinsert"))
						{
							String key = split_input[1];
							String value = split_input[2];
							ContentValues cv = new ContentValues();
							cv.put(KEY_FIELD, "replica,"+key);
							cv.put(VALUE_FIELD, value);
							try {
								insert(mUri, cv);
//								Log.v(myPort, "replica created for recovery");
								out.println("success");
							} catch (Exception ex) {
								Log.e(TAG, ex.toString());
							}
						}
						else if(split_input[0].equals("recover"))
						{
							int keyIndex, valueIndex;
							String returnKey, returnValue;
							if(split_input[1].equals("pred"))
							{
								Cursor resultCursor = query(mUri, null, "@", null, null);

								keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
								valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
//                                        resultCursor.moveToFirst();
								if(resultCursor.getCount()<1)
								{
									out.println("up to date");
								}
								else
								{
									for(resultCursor.moveToFirst(); !resultCursor.isAfterLast(); resultCursor.moveToNext())
									{
										returnKey = resultCursor.getString(keyIndex);
										returnValue = resultCursor.getString(valueIndex);
										resultCursor.close();
										Log.v("this is the pref key-->", returnKey);
//										if(getSucc("key", returnKey).equals(split_input[2]))
//										{
										out.println(returnKey+","+returnValue);
										Log.v("returning to:"+split_input[2]+":", returnKey+","+returnValue);
//										}
									}
									out.println("done");
								}

							}
							else if(split_input[1].equals("succ"))
							{
								Cursor resultCursor = query(mUri, null, "@", null, null);

								keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
								valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
//                                        resultCursor.moveToFirst();
								if(resultCursor.getCount()<1)
								{
									out.println("up to date");
								}
								else
								{
									for(resultCursor.moveToFirst(); !resultCursor.isAfterLast(); resultCursor.moveToNext())
									{
										returnKey = resultCursor.getString(keyIndex);
										returnValue = resultCursor.getString(valueIndex);
										resultCursor.close();
										Log.v("this is the pred key-->", returnKey);
										String[] req_pref = getSucc("node", split_input[2]).split(",");
//										if(!getSucc("key", returnKey).equals(req_pref[0])  || !getSucc("key", returnKey).equals(req_pref[1]) )
//										{
										out.println(returnKey+","+returnValue);
										Log.v("returning to:"+split_input[2]+":", returnKey+","+returnValue);
//										}
									}
									out.println("done");
								}
							}
							else
							{

								if(split_input[2].equals(pred_port1))
								{
									if(del_list_1.isEmpty())
									{
										out.println("up to date");
									}
									else
									{
										for(String s: del_list_1)
										{
											out.println(s);
										}
										del_list_1.clear();
										out.println("done");
									}


								}
								else
								{
									if(del_list_2.isEmpty())
									{
										out.println("up to date");
									}
									else
									{
										for(String s: del_list_2)
										{
											out.println(s);
										}
										del_list_2.clear();
										out.println("done");
									}
								}

							}
						}

					}

					if (clientSocket.isClosed()) {
						break;
					}

				}
				catch (UnknownHostException e)
				{
					Log.e(TAG, e.toString());
				}
				catch (IOException e)
				{
					Log.e(TAG, e.toString());
				}
//				catch (NoSuchAlgorithmException e)
//				{
//					Log.e(TAG, e.toString());
//				}

			}
			return null;
		}
	}

	private  class Recovery extends  AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... strings) {

			Log.v("RECOVERYYYY", "RECOVERYYYY");

			String remotePort = strings[0];

			String msg_to_send = "recover,"+strings[1]+","+strings[2];

			Socket socket = null;

			PrintWriter out = null;

			BufferedReader in = null;

			String response;

			try {


				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));

				/* Client Code that sends the received messages to the server as long
				 * as the socket connection is alive*/
				out = new PrintWriter(socket.getOutputStream(), true);
				in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				out.println(msg_to_send);
//				Log.v("sending rep req from", myPort);
				response = in.readLine();
				socket.setSoTimeout(100);
				while(response != null)
				{

					String[] split_input;
					if(response.equals("done") || response.equals("up to date"))
					{
						break;
					}
					Log.v(remotePort+":response--->: "+strings[1], "::::"+response);
					if(strings[1].equals("pred"))
					{
						synchronized (this)
						{
							split_input = response.split((","));
							Log.v("the key from pref--->", split_input[0]);
							Log.v(myPort+"::actual successor of: "+split_input[0], ":"+getSucc("key", split_input[0]));
							if(getSucc("key", split_input[0]).equals(myPort))
							{
								ContentValues cv = new ContentValues();
								cv.put(KEY_FIELD, "recover,"+split_input[0]);
								cv.put(VALUE_FIELD, split_input[1]);

								try {
									insert(mUri, cv);
									Log.v(split_input[0], "accepted!!!!!!!!!!!!!");
								} catch (Exception ex) {
									Log.e(TAG, "ERRRRORRRRRR in recovery");
								}
							}

						}
					}
					else if(strings[1].equals("succ"))
					{
						synchronized (this)
						{
							split_input = response.split((","));
							Log.v("the key from pred--->", split_input[0]);
							Log.v(myPort+":actual successor of: "+split_input[0], ":"+getSucc("key", split_input[0]));
							Log.v("succ :my pred1", pred_port1);
							Log.v("succ :my pred2", pred_port2);
							if(getSucc("key", split_input[0]).equals(pred_port1) || getSucc("key", split_input[0]).equals(pred_port2))
							{
								ContentValues cv = new ContentValues();
								cv.put(KEY_FIELD, "replica,"+split_input[0]);
								cv.put(VALUE_FIELD, split_input[1]);

								try {
									insert(mUri, cv);
									Log.v(split_input[0], "accepted!!!!!!!!!!!!!");
								} catch (Exception ex) {
									Log.e(TAG, "ERRRRORRRRRR in recovery");
								}
							}
						}
					}
					else
					{
						split_input = response.split((","));
						for(String s: split_input)
						{
							delete(mUri, s, null);
						}
					}

					response = in.readLine();
				}
				String returnKey, returnValue;
				Cursor resultCursor = query(mUri, null, "@", null, null);
				int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
				int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
				for(resultCursor.moveToFirst(); !resultCursor.isAfterLast(); resultCursor.moveToNext()) {
					returnKey = resultCursor.getString(keyIndex);
					returnValue = resultCursor.getString(valueIndex);
					resultCursor.close();
					Log.v(strings[1]+"::stored---->", returnKey+","+returnValue);
				}

			} catch (NullPointerException e) {
				Log.e(myPort, "nullpointer exception in recovery");
			} catch (SocketTimeoutException e)
			{
				Log.e(myPort, "Recovery socket timed out");
			}
			catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

			return null;
		}
	}

	private String passToSuccessor(String op, String successor, String val1, String val2) {


		PrintWriter out = null;
		BufferedReader in = null;
		Socket socket = null;

		try {
			socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
			out = new PrintWriter(socket.getOutputStream(), true);
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			out.println(op+"," + val1 + "," + val2);
			String input_string;
			if((input_string = in.readLine()) != null) {
				return input_string;
			}

		}
		catch (SocketException e) {
			Log.e(TAG, e.toString());
		} catch (UnknownHostException e) {
			Log.e(TAG, e.toString());
		} catch (IOException e) {
			Log.e(TAG, e.toString());
		} catch (NullPointerException e)
		{
			Log.e(TAG, "Connection error");
		}
		return null;

	}



	private String passToAll(String op, String selection) {
		PrintWriter out = null;
		BufferedReader in = null;
		Socket socket = null;
		String result = "";
		String input_string;

		try
		{
			for(String port: nodes)
			{
				if(!port.equals(portStr))
				{
					String remotePort = String.valueOf(Integer.parseInt(port)*2);
					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
					out = new PrintWriter(socket.getOutputStream(), true);
					in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					out.println(op+"," + selection);
					if((input_string = in.readLine()) != null) {
						result += input_string;
					}
				}
			}

		}
		catch (SocketException e) {
			Log.e(TAG, e.toString());
		} catch (UnknownHostException e) {
			Log.e(TAG, e.toString());
		} catch (IOException e) {
			Log.e(TAG, e.toString());
		} catch (NullPointerException e)
		{
			Log.e(TAG, "Connection error");
		}
		return result;
	}

	private String synchronizeQuery(String key, String object_id) {

		PrintWriter out = null;
		BufferedReader in = null;
		Socket socket1 = null;
		Socket socket2 = null;
		try {
			Log.v(portStr,"synchronize reps");
			socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(pref_port1));
			socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(pref_port2));
			out = new PrintWriter(socket1.getOutputStream(), true);
			in = new BufferedReader(new InputStreamReader(socket1.getInputStream()));
			out.println("syncquery"+","+ key);
			String input_string;
			if((input_string = in.readLine()) != null) {

			}

		}
		catch (SocketException e) {
			Log.e(TAG, e.toString());
		} catch (UnknownHostException e) {
			Log.e(TAG, e.toString());
		} catch (IOException e) {
			Log.e(TAG, e.toString());
		} catch (NullPointerException e)
		{
			Log.e(TAG, "Connection error");
		}
		return null;
	}

	private class Replicate extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			String remotePort = msgs[0];

			String[] split_object = msgs[1].split(",");
			String object_id = split_object[1];
			String key = msgs[2];
			String value = msgs[3];

			String msg_to_send = "replication,"+ myPort +"," + object_id+","+key+","+value;

			Socket socket = null;

			PrintWriter out = null;

			BufferedReader in = null;

			String status;

			try {


				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));

				/* Client Code that sends the received messages to the server as long
				 * as the socket connection is alive*/
				out = new PrintWriter(socket.getOutputStream(), true);
				in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				out.println(msg_to_send);
				status = in.readLine();
				socket.setSoTimeout(100);
				if(status != null)
				{
//					Log.v("replication status", status);
				}

			} catch (NullPointerException e) {
				Log.e(myPort, "Replication failed");
			} catch (SocketTimeoutException e)
			{
				Log.e(myPort, "Replication failed");
			}
			catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}
	}




	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		String successor;
		Log.v("delete", "inside delete function");
		try {

			if(selection.equals("@"))
			{
				for (String file: getContext().fileList()) {
					Log.v("delete", "listing files");

					if(getContext().deleteFile(file)) {
						Log.v("delete", "file deleted successfully");
					}
					else {
						Log.v("delete", "File deletion unsuccessful");
					}

				}

			}
			else if(selection.equals("*"))
			{
				Log.v("deletion:", "delete everything");
				for (String file: getContext().fileList()) {
					if(getContext().deleteFile(file)) {
						Log.v("delete", "file deleted successfully");
					}
					else {
						Log.v("delete", "File deletion unsuccessful");
					}
				}

				String result = passToAll("delete", "*");
				Log.v("deletion result", result);

			}
			else {

				String[] split_input;
				if((split_input = selection.split(",")).length > 1)
				{
					Log.v("replica deletion: ", split_input[1]);
					if(getContext().deleteFile(split_input[1])) {
						Log.v("delete", "replica deleted successfully");
					}
					else {
						Log.v("delete", "replica deletion unsuccessful");
					}
				}
				else
				{
					successor = getSucc("key", selection);
					if(successor.equals(myPort))
					{
						if(getContext().deleteFile(selection)) {
							Log.v("delete", "file deleted successfully");
						}
						else {
							Log.v("delete", "File deletion unsuccessful");
						}

						String res_pref1 = passToSuccessor("delete", pref_port1, selection, "replica");
						Log.v("pref1 delete status", res_pref1);

						String res_pref2 = passToSuccessor("delete", pref_port2, selection, "replica");
						Log.v("pref2 delete status", res_pref2);
					}
					else
					{
						String res = passToSuccessor("delete", successor, selection, myPort);
						if(res == null)
						{
							String[] pref_ports = getSucc("node", String.valueOf(Integer.parseInt(successor)/2)).split(",");
							String res1 = passToSuccessor("prefdelete", pref_ports[0], selection, myPort);
							String res2 = passToSuccessor("prefdelete", pref_ports[1], selection, myPort);
							Log.v("result of deletion:", res1+","+res2);
						}
					}
				}


			}


		}
		catch (NullPointerException e)
		{
			Log.v("exception caught", "Exception caught at delete");
			Log.e(TAG, e.toString());
		}

		return 1;

	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		Iterator iter = values.keySet().iterator();
		String key = "", value = "", hashedfilename="", successor="";
		OutputStream outputStream;
		while(iter.hasNext())
		{
			String col = (String) iter.next();

			if(col.equals(VALUE_FIELD))
			{
				value = values.getAsString(col)+"\n";
			}
			else
			{
				key = values.getAsString(col);
				try {
					String[] splitkey = key.split(",");
					if(splitkey.length > 1)
					{
						key = splitkey[1];
						outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
						outputStream.write(value.getBytes());
						outputStream.close();
						if(splitkey[0].equals("replicate"))
						{
							Log.v(key," ::::successfully replicated!!!!!!!!:"+key);
						}
						else
						{
							Log.v(key," ::::successfully recovered!!!!!!!!:"+key);
						}

					}
					else
					{
						successor = getSucc("key", key);
//						Log.v("successor for key", successor);
						if(successor.equals(myPort))
						{
							Log.v(successor, "current port is successor");
							key_count += 1;
							String object_val = key+","+key_count;
							object_list.add(object_val);
							outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
							outputStream.write(value.getBytes());
							outputStream.close();

//							Log.v("object", object_val);
							new Replicate().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, pref_port1, object_val, key, value);
							new Replicate().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, pref_port2, object_val, key, value);

						}
						else
						{
							String res = passToSuccessor("insert", successor, key, value);
//							Log.v("original successor", successor);
							if(res==null)
							{
								String[] pref_ports = getSucc("node", String.valueOf(Integer.parseInt(successor)/2)).split(",");
								Log.v("orginal failed:", successor);
								Log.v(key+"  being saved in:", pref_ports[0]+","+pref_ports[1]);
								String pref1_res = passToSuccessor("prefinsert",pref_ports[0], key, value);
								String pref2_res = passToSuccessor("prefinsert",pref_ports[1], key, value);
								Log.v(pref1_res, "success!!!!!!!!!!!!!!!!!!!!!!!!!");
								Log.v(pref2_res, "success!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

							}
						}
					}

				}
				catch (IOException e) {
					Log.e("GroupMessengerProvider", "File write failed");
				}
			}
		}
		return uri;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		String[] columnNames = {KEY_FIELD, VALUE_FIELD};
		MatrixCursor cursor;
		cursor = new MatrixCursor(columnNames);
		FileInputStream fis;
		String successor= getSucc("key", selection);
		Log.v("query for selection--->", selection);
		try {

			if(selection.equals("@")) {
				for (String file : getContext().fileList()) {
//					Log.v("file--->", file);
					fis = getContext().openFileInput(file);
					InputStreamReader inputStreamReader = new InputStreamReader(fis, StandardCharsets.UTF_8);
					BufferedReader reader = new BufferedReader(inputStreamReader);
					String line = reader.readLine();
					String[] columnValues = {file, line};

					cursor.addRow(columnValues);
					reader.close();
					inputStreamReader.close();
				}
				Log.v("count of all local keys", String.valueOf(cursor.getCount()));
			}

			else if(selection.equals("*")) {
				for (String file: getContext().fileList()) {
//					Log.v("file--->", file);
					fis = getContext().openFileInput(file);
					InputStreamReader inputStreamReader = new InputStreamReader(fis, StandardCharsets.UTF_8);
					BufferedReader reader = new BufferedReader(inputStreamReader);
					String line = reader.readLine();
					String[] columnValues = {file, line};
					String result = passToAll("query", "*");
					cursor.addRow(columnValues);
					for(String row: result.trim().split(";"))
					{
//						Log.v("row:", row);
						String[] split = row.split(",");
						if(split.length == 2)
							cursor.addRow(split);
						else
						{
							Log.v("bad split--1", split[0]);
						}
					}

					Log.v("final count", String.valueOf(cursor.getCount()));

					reader.close();
					inputStreamReader.close();
				}


			}
			else {

				String[] split_selection = selection.split(",");
				if(split_selection.length>1)
				{
					Log.v(myPort,":"+split_selection[0]+":reaches here!!");
					fis = getContext().openFileInput(split_selection[1]);
					InputStreamReader inputStreamReader = new InputStreamReader(fis, StandardCharsets.UTF_8);
					BufferedReader reader = new BufferedReader(inputStreamReader);
					String line = reader.readLine();
					String[] columnValues = {split_selection[1], line};
					cursor.addRow(columnValues);
					reader.close();
					inputStreamReader.close();
					Log.v("sending>>>>", split_selection[1]+","+line);

				}
				else if(successor.equals(myPort))
				{

					fis = getContext().openFileInput(selection);
					InputStreamReader inputStreamReader = new InputStreamReader(fis, StandardCharsets.UTF_8);
					BufferedReader reader = new BufferedReader(inputStreamReader);
					String line = reader.readLine();
					String[] columnValues = {selection, line};
					cursor.addRow(columnValues);
					reader.close();
					inputStreamReader.close();

				}
				else
				{
					String result = passToSuccessor("query", successor, selection, myPort);
					if(result == null)
					{
						Log.v(successor, ":failed!!passing to pref1");
						String[] pref_ports = getSucc("node", String.valueOf(Integer.parseInt(successor)/2)).split(",");
						Log.v("the prefs:", pref_ports[0]+","+pref_ports[1]);
						result = passToSuccessor("query", pref_ports[0], "pref,"+selection, myPort);
					}
					Log.v("this is the result!!", result);
					for(String row: result.trim().split(";"))
					{
//						Log.v("row:", row);
						String[] split = row.split(",");
						if(split.length == 2)
							cursor.addRow(split);
						else
						{
							Log.v("bad split--2", split[0]);
						}
					}
				}

			}

		}
		catch (IOException e)
		{
			Log.e("error content provider", e.toString());
			String[] columnValues = {selection, "N/A"};
			cursor.addRow(columnValues);
		}
		cursor.close();
		return cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}
}