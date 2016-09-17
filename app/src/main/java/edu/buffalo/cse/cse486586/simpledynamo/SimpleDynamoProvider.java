package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.Formatter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


public class SimpleDynamoProvider extends ContentProvider {



	static final String TAG = SimpleDynamoActivity.class.getSimpleName();
	static final String[] REMOTE_PORT = {"11108", "11112", "11116", "11120", "11124"};
	static final int SERVER_PORT = 10000;
	static String predecessor="0";
	static String pre_predecessor="0";
	static String[] successors=new String[2];
	static String Myport="";
	static String Failnode="0";
	static int updatestate=0;
	final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
	Comparator<NodeObject> NodeCompare = new NodeComparator();
	BlockingQueue<NodeObject> pq= new PriorityBlockingQueue<NodeObject>(100, NodeCompare);

	NodeObject[] NodeInfo=new NodeObject[5];

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	public class NodeObject {
		String portnum;
		String hashvalue;
		String state;
		String pre_pre;
		String pre;
		String suc1;
		String suc2;

		public NodeObject() {
		}

		public NodeObject(String port, String gh) {
			this.portnum=port;
			this.hashvalue=gh;
			this.state="up";
			this.pre=null;
			this.pre_pre=null;
			this.suc1=null;
			this.suc2=null;
		}
	}

	public class NodeComparator implements Comparator<NodeObject>
	{
		@Override
		public int compare(NodeObject n1, NodeObject n2)
		{
			if (n1.hashvalue.compareTo(n2.hashvalue)<0)
			{ return -1; }
			if (n1.hashvalue.compareTo(n2.hashvalue)>0)
			{ return 1;  }
			return 0;
		}
	}


	@Override
	public boolean onCreate() {
		Log.e("Oncreate", "got in");
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		Myport=myPort;

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT, 100);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
		}


		for (int i=0; i<5; i++)
		{
			Log.e("Myport NodeObject", REMOTE_PORT[i]);
			try {
				String emuhash=String.valueOf(Integer.parseInt(REMOTE_PORT[i])/2);
				NodeObject n= new NodeObject(REMOTE_PORT[i],genHash(emuhash));
				pq.add(n);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}

		for (int j=0; j<5; j++)
		{
			NodeInfo[j]=pq.poll();
			Log.e(NodeInfo[j].portnum, "add NodeInfo");
		}

		int pqsize=5;
		Log.e("pq size", String.valueOf(pqsize));

		for (int i=0; i<pqsize; i++)
		{
			Log.e("inside pre suc", "update");
			if (i==pqsize-2)
			{
				NodeInfo[i].pre_pre=NodeInfo[i-2].portnum;
				NodeInfo[i].pre=NodeInfo[i-1].portnum;
				NodeInfo[i].suc1=NodeInfo[i+1].portnum;
				NodeInfo[i].suc2=NodeInfo[0].portnum;
			}
			else if (i==pqsize-1)
			{
				NodeInfo[i].pre_pre=NodeInfo[i-2].portnum;
				NodeInfo[i].pre=NodeInfo[i-1].portnum;
				NodeInfo[i].suc1=NodeInfo[0].portnum;
				NodeInfo[i].suc2=NodeInfo[1].portnum;
			}
			else if (i==0)
			{
				NodeInfo[i].pre_pre=NodeInfo[pqsize-2].portnum;
				NodeInfo[i].pre=NodeInfo[pqsize-1].portnum;
				NodeInfo[i].suc1=NodeInfo[i+1].portnum;
				NodeInfo[i].suc2=NodeInfo[i+2].portnum;
			} else if (i==1)
			{
				NodeInfo[i].pre_pre=NodeInfo[pqsize-1].portnum;
				NodeInfo[i].pre=NodeInfo[i-1].portnum;
				NodeInfo[i].suc1=NodeInfo[i+1].portnum;
				NodeInfo[i].suc2=NodeInfo[i+2].portnum;
			}
			else
			{
				NodeInfo[i].pre_pre=NodeInfo[i-2].portnum;
				NodeInfo[i].pre=NodeInfo[i-1].portnum;
				NodeInfo[i].suc1=NodeInfo[i+1].portnum;
				NodeInfo[i].suc2=NodeInfo[i+2].portnum;
			}

			if (NodeInfo[i].portnum.equals(Myport))
			{
				Log.e("pre", "suc");
				pre_predecessor=NodeInfo[i].pre_pre;
				predecessor=NodeInfo[i].pre;
				successors[0]=NodeInfo[i].suc1;
				successors[1]=NodeInfo[i].suc2;
				Log.e(successors[0], successors[1]);
			}
		}


		new ClientTaskNJ().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);

		return false;
	}

	public class ClientTaskNJ  extends AsyncTask<String, Void, Void> {
		@Override
		protected Void doInBackground(String... msgs) {
			try {
				updatestate=1;
				CheckMsg alive= new CheckMsg();
				String ping=alive.alivePing();

				CheckMsg checkMsg= new CheckMsg();
				String reply=checkMsg.updateMsg();
				updatestate=0;
				Log.e("Ping + reply", reply + " " + ping);
			} catch (Exception e) {
				Log.e("ClientNJ Exception", e.toString());
			}
			return null;
		}
	}

	public class CheckMsg
	{
		public String alivePing ()
		{
			try{
				for (int i=0; i<5; i++)
				{
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(NodeInfo[i].portnum));
					PrintWriter send = new PrintWriter(socket.getOutputStream(), true);
					String msgToSend = "A ";
					msgToSend = msgToSend.concat(Myport);
					send.println(msgToSend);
					socket.close();
					Log.e("Pinged", NodeInfo[i].portnum );
				}
			} catch (Exception e)
			{
				Log.e("AlivePing Exception", e.toString());
			}
			return "done";
		}

		public String updateMsg ()
		{
			Log.v("In checkMsg", "update");
			String state="false";
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(predecessor));
				PrintWriter send = new PrintWriter(socket.getOutputStream(), true);
				String msgToSend = "P ";
				msgToSend = msgToSend.concat(Myport);
				send.println(msgToSend);
				InputStreamReader isr = new InputStreamReader(socket.getInputStream());
				BufferedReader br = new BufferedReader(isr);
				String msg= br.readLine();

				if (msg.equalsIgnoreCase("new"))
				{
					state="true";
					Log.v("In checkMsg P", "update new");

				}
				else {
					String[] temp = msg.split("-");
					for (int i = 0; i < temp.length; i++) {
						String key = temp[i].split(" ")[0];
						String value = temp[i].split(" ")[1];
						FileOutputStream fos = null;
						Context context = getContext();
						fos = context.openFileOutput(key, context.MODE_PRIVATE);
						fos.write(value.getBytes());
						fos.close();
						Log.v("In checkMsg update P", key+" "+value);
						state="true";
					}
				}
				socket.close();

				Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(successors[0]));
				PrintWriter send1 = new PrintWriter(socket1.getOutputStream(), true);
				String msgToSend1 = "S ";
				msgToSend1 = msgToSend1.concat(Myport);
				send1.println(msgToSend1);
				InputStreamReader isr1 = new InputStreamReader(socket1.getInputStream());
				BufferedReader br1 = new BufferedReader(isr1);
				String msg1= br1.readLine();

				if (msg1.equalsIgnoreCase("new"))
				{
					state="true";
					Log.v("In checkMsg S", "update new");
				}
				else {
					String[] temp = msg1.split("-");
					for (int i = 0; i < temp.length; i++) {
						String key = temp[i].split(" ")[0];
						String value = temp[i].split(" ")[1];
						FileOutputStream fos = null;
						Context context = getContext();
						fos = context.openFileOutput(key, context.MODE_PRIVATE);
						fos.write(value.getBytes());
						fos.close();
						Log.v("write S", key);
						state="true";
						Log.v("In checkMsg update S", key+" "+value);
					}
				}
				socket1.close();

			} catch (Exception e)
			{
				Log.e("updateMsg Exception" , e.toString());
			}
			return state;
		}

		public String sendMsg(char c)
		{
			String state="false";
			Log.v("In checkMsg", "sendMsg");

			if (c=='P') {
				Log.e("In P", Myport);
				FileInputStream fis = null;
				String data = null;
				Context context = getContext();
				String filepath = context.getFilesDir().getAbsolutePath();
				String sendfile="";
				try {
					File f = new File(filepath);
					File file[] = f.listFiles();
					if (file.length <= 0) {
						Log.v("In checkMsg P", "sendMsg new");
						return "new";
					}
					String hashpre_pre = genHash(String.valueOf(Integer.parseInt(pre_predecessor)/2));
					String hashpre = genHash(String.valueOf(Integer.parseInt(predecessor)/2));
					String hashcur = genHash(String.valueOf(Integer.parseInt(Myport)/2));
					for (int i = 0; i < file.length; i++) {
						String fname = file[i].getName();
						String hashkey = genHash(fname);
						if ((hashcur.compareTo(hashpre) < 0 && (hashkey.compareTo(hashcur) <= 0 || hashkey.compareTo(hashpre) > 0)) ||
								(hashcur.compareTo(hashpre) > 0 && hashkey.compareTo(hashcur) <= 0 && hashkey.compareTo(hashpre) > 0) ||
								(hashpre.compareTo(hashpre_pre) < 0 && (hashkey.compareTo(hashpre) <= 0 || hashkey.compareTo(hashpre_pre) > 0)) ||
								(hashpre.compareTo(hashpre_pre) > 0 && hashkey.compareTo(hashpre) <= 0 && hashkey.compareTo(hashpre_pre) > 0)) {
							fis = context.openFileInput(fname);
							BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
							data = reader.readLine();
							sendfile=sendfile.concat(fname);
							sendfile=sendfile.concat(" ");
							sendfile=sendfile.concat(data);
							sendfile=sendfile.concat("-");
							fis.close();
							reader.close();
							Log.v("In checkMsg sendMsg P", fname +" "+data);
						}
					}

				} catch (Exception e) {
					Log.e("sendMsg Exception", e.toString());
				}
				return sendfile;
			}

			else if (c=='S')
			{
				Log.e("In S", Myport);
				Log.v("In checkMsg", "sendMsg S");
				FileInputStream fis = null;
				String data = null;
				Context context = getContext();
				String filepath = context.getFilesDir().getAbsolutePath();
				String sendfile="";
				try {
					File f = new File(filepath);
					File file[] = f.listFiles();
					if (file.length <= 0) {
						Log.v("In checkMsg", "sendMsg new");
						return "new";
					}
					String hashpre_pre = genHash(String.valueOf(Integer.parseInt(pre_predecessor)/2));
					String hashpre = genHash(String.valueOf(Integer.parseInt(predecessor)/2));
					for (int i = 0; i < file.length; i++) {
						String fname = file[i].getName();
						String hashkey = genHash(fname);
						if ((hashpre.compareTo(hashpre_pre) < 0 && (hashkey.compareTo(hashpre) <= 0 || hashkey.compareTo(hashpre_pre) > 0)) ||
								(hashpre.compareTo(hashpre_pre) > 0 && hashkey.compareTo(hashpre) <= 0 && hashkey.compareTo(hashpre_pre) > 0)) {
							fis = context.openFileInput(fname);
							BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
							data = reader.readLine();
							sendfile=sendfile.concat(fname);
							sendfile=sendfile.concat(" ");
							sendfile=sendfile.concat(data);
							sendfile=sendfile.concat("-");
							fis.close();
							reader.close();
							Log.v("In checkMsg sendMsg P", fname+" "+data);
						}
					}

				} catch (Exception e) {
					Log.e("sendMsg Exception", e.toString());
				}
				return sendfile;
			}
			return "new";
		}
	}



	public class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			Log.e("socket", String.valueOf(sockets[0]));
			try {
				while (true) {
					Socket s = serverSocket.accept();

					while (updatestate==1)
					{
						try{
							Thread.sleep(200);
						} catch (Exception e)
						{
							Log.e ("sleep Exception"," ");
						}
					}

					Log.e("Server", "accept");
					InputStreamReader isr = new InputStreamReader(s.getInputStream());
					BufferedReader br = new BufferedReader(isr);
					PrintWriter pw = new PrintWriter(s.getOutputStream(), true);
					String input = br.readLine();
					if (input.charAt(0)=='P' || input.charAt(0)=='S')
					{
						CheckMsg checkMsg= new CheckMsg();
						String reply=checkMsg.sendMsg(input.charAt(0));
						Log.v("Server checkMsg", reply.toString());
						pw.println(reply);
					} else if (input.charAt(0)=='A')
					{
						String[] temp = input.split(" ");
						if (Failnode.equals(temp[1]))
						{
							Log.v("Node update Fail", Failnode);
							Failnode="0";
							Log.v("Node update After", Failnode);
						}
					} else if(input.charAt(0)=='I') {
						String[] temp = input.split(" ");
						String key=temp[1];
						String value=temp[2];
						Thread insertthread= new Thread(new InsertThread(key, value, s));
						insertthread.start();

					} else if(input.charAt(0)=='R' ) {
						String[] temp = input.split(" ");
						String key=temp[1];
						String value=temp[2];
						String FILENAME = key;
						String data = value;
						FileOutputStream fos = null;
						Context context = getContext();
						fos = context.openFileOutput(FILENAME, context.MODE_PRIVATE);
						fos.write(data.getBytes());
						fos.close();
						pw.println("done");
						Log.v("insert R", value.toString());
					} else if(input.charAt(0)=='Q' && input.charAt(1)=='S') {
						String[] temp = input.split(" ");
						String key=temp[1];
						Log.e("got match", " ");
						FileInputStream fis = null;
						Context context = getContext();
						String data=null;
						int st=0;
						do {
							try {
								fis = context.openFileInput(key);
								BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
								data = reader.readLine();
								fis.close();
								reader.close();
								st=0;
							} catch (Exception e)
							{
								st=1;
								Log.e("QS Exception", e.toString() );
							}
						}while (st==1);

						Log.e("QS CC", data);
						String msg=key;
						msg=msg.concat(" ");
						msg=msg.concat(data);
						pw.println(msg);
						Log.e("QS msg", msg);
					} else if(input.charAt(0)=='Q' && input.charAt(1)=='A') {
						String[] temp = input.split(" ");
						String selection=temp[1];
						Cursor resultCursor =query(mUri, null, selection, null, null);
						Log.e("QA CC", String.valueOf(resultCursor.getCount()));
						Log.e("Q " + Myport, selection);
						int c=0;
						String msg="-";
						resultCursor.moveToFirst();
						while (!resultCursor.isAfterLast()) {
							if (c>0)
							{
								msg= msg.concat("-");
							}
							c++;
							String returnKey = resultCursor.getString(0);
							String returnValue = resultCursor.getString(1);
							msg=msg.concat(returnKey);
							msg=msg.concat(" ");
							msg=msg.concat(returnValue);
							resultCursor.moveToNext();
						}
						pw.println(msg);
						Log.e("QA msg", msg);
						resultCursor.close();
					} else if(input.charAt(0)=='D') {
						String selection="@";
						delete(mUri, selection, null);
					} else if (input.charAt(0)=='F')
					{
						String[] temp = input.split(" ");
						Failnode=temp[1];
					}
					pw.flush();
				}

			}catch (Exception e) {
				Log.e("Server Exception", e.toString());
			}
			return null;
		}
	}


	public void InformFailure (String failnode)
	{
		Failnode= failnode;
		try {
			for (int j=0; j<5; j++)
			{
				if (NodeInfo[j].portnum.equals(failnode))
				{
					continue;
				}
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(NodeInfo[j].portnum));
				PrintWriter send = new PrintWriter(socket.getOutputStream(), true);
				String msgToSend = "F ";
				msgToSend = msgToSend.concat(failnode);
				send.println(msgToSend);
				Log.v("send F", failnode);
				socket.close();
			}
		} catch (Exception e)
		{
			Log.e("InformFailure Exception", e.toString());
		}
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		String FILENAME=selection;
		Context context=getContext();

		try {
			if (selection=="@") {
				String filepath = context.getFilesDir().getAbsolutePath();
				File f = new File(filepath);
				File file[] = f.listFiles();
				for (int k = 0; k < file.length; k++) {
					String fname = file[k].getName();
					file[k].delete();
					Log.e("File deleted", fname);
				}
			} else {
				for (int j = 0; j < 5; j++) {
					if (NodeInfo[j].portnum.equals(Myport))
					{
						String filepath = context.getFilesDir().getAbsolutePath();
						File f = new File(filepath);
						File file[] = f.listFiles();
						for (int k = 0; k < file.length; k++) {
							String fname = file[k].getName();
							file[k].delete();
							Log.e("File deleted", fname);
						}
					}
					else {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(NodeInfo[j].portnum));
						PrintWriter send = new PrintWriter(socket.getOutputStream(), true);
						String msgToSend = "D ";
						msgToSend = msgToSend.concat("@");
						send.println(msgToSend);
					}
				}
			}

		}catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String FILENAME = (String) values.get("key");
		String data = (String) values.get("value");


		while (updatestate==1)
		{
			try{
				Thread.sleep(200);
			} catch (Exception e)
			{
				Log.e ("sleep Exception"," ");
			}
		}

		Context context = getContext();
		FileOutputStream fos = null;
		try {
			for (int i=0; i<5; i++) {
				String hashkey = genHash(FILENAME);
				String hashpre = genHash(String.valueOf(Integer.parseInt(NodeInfo[i].pre) / 2));
				String hashcur = genHash(String.valueOf(Integer.parseInt(NodeInfo[i].portnum) / 2));
				if ((hashcur.compareTo(hashpre) < 0 && (hashkey.compareTo(hashcur) <= 0 || hashkey.compareTo(hashpre) > 0)) ||
						(hashcur.compareTo(hashpre) > 0 && hashkey.compareTo(hashcur) <= 0 && hashkey.compareTo(hashpre) > 0)) {
					Log.e(NodeInfo[i].portnum, Myport);
					if (NodeInfo[i].portnum.equals(Myport)) {
						fos = context.openFileOutput(FILENAME, context.MODE_PRIVATE);
						fos.write(data.getBytes());
						fos.close();
						Log.v("insert 1", values.toString());

						for (int j=0; j<2; j++)
						{
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(successors[j]));
							PrintWriter send = new PrintWriter(socket.getOutputStream(), true);
							String msgToSend = "R ";
							msgToSend = msgToSend.concat(FILENAME);
							msgToSend = msgToSend.concat(" ");
							msgToSend = msgToSend.concat(data);
							send.println(msgToSend);
							InputStreamReader isr = new InputStreamReader(socket.getInputStream());
							BufferedReader br = new BufferedReader(isr);
							String status=br.readLine();

							if (status!=null)
							{
								Log.v("insert R", values.toString());
								socket.close();
								continue;
							}
							else
							{
								if (Failnode.equals(successors[j])) {}
								else {InformFailure(successors[j]);}
							}

							Log.v("insert R", values.toString());
							socket.close();
						}
					} else {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(NodeInfo[i].portnum));
						PrintWriter send = new PrintWriter(socket.getOutputStream(), true);
						String msgToSend = "I ";
						msgToSend = msgToSend.concat(FILENAME);
						msgToSend = msgToSend.concat(" ");
						msgToSend = msgToSend.concat(data);
						send.println(msgToSend);
						InputStreamReader isr = new InputStreamReader(socket.getInputStream());
						BufferedReader br = new BufferedReader(isr);
						String status=br.readLine();

						if (status!=null)
						{
							Log.v("insert 2", values.toString());
							socket.close();
							continue;
						}
						else
						{
							if (Failnode.equals(NodeInfo[i].portnum)) { socket.close();}
							else {InformFailure(NodeInfo[i].portnum); socket.close();}
							for (int j=0; j<2; j++)
							{
								String successor="0";
								if (j==0) { successor=NodeInfo[i].suc1; }
								else { successor=NodeInfo[i].suc2;}

								Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										Integer.parseInt(successor));
								PrintWriter send1 = new PrintWriter(socket1.getOutputStream(), true);
								String msgToSend1 = "R ";
								msgToSend1 = msgToSend1.concat(FILENAME);
								msgToSend1 = msgToSend1.concat(" ");
								msgToSend1 = msgToSend1.concat(data);
								send1.println(msgToSend1);
								InputStreamReader isr1 = new InputStreamReader(socket1.getInputStream());
								BufferedReader br1 = new BufferedReader(isr1);
								String status1=br1.readLine();

								Log.v("insert R in I", values.toString());

								continue;
							}

						}
					}
				}
			}
		} catch (Exception e) {
			Log.e("Insert Exception", e.toString());
		}



		return null;
	}


	public class InsertThread implements Runnable
	{
		String FILENAME;
		String data;
		Socket s;
		public InsertThread (String key, String value, Socket s1)
		{
			this.FILENAME=key;
			this.data=value;
			this.s =s1;
		}

		public void run()
		{
			try {
				PrintWriter pw = new PrintWriter(s.getOutputStream(), true);
				ContentValues mNewValues = new ContentValues();
				mNewValues.put("key", FILENAME);
				mNewValues.put("value", data);
				insert(mUri, mNewValues);
				Log.e("Inside Insert Thread", FILENAME);
				pw.println("done");
			} catch (Exception e)
			{
				Log.e("Insert Thread Exception", e.toString());
			}
		}
	}



	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		FileInputStream fis = null;
		String FILENAME = selection;
		String data = null;
		Context context = getContext();
		String filepath = context.getFilesDir().getAbsolutePath();
		MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
		try {

			while (updatestate==1)
			{
				try{
					Thread.sleep(200);
				} catch (Exception e)
				{
					Log.e ("sleep Exception"," ");
				}
			}

			if (selection.equals("@")) {
				File f = new File(filepath);
				File file[] = f.listFiles();
				for (int i = 0; i < file.length; i++) {
					String fname = file[i].getName();
					fis = context.openFileInput(fname);
					BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
					data = reader.readLine();
					fis.close();
					reader.close();
					MatrixCursor.RowBuilder builder = cursor.newRow();
					builder.add("key", file[i].getName());
					builder.add("value", data);
				}
				Log.v("query @", selection);
			} else if (selection.equals("*")) {
				for (int i = 0; i < 5; i++) {
					if (NodeInfo[i].portnum.equals(Myport)) {
						Log.e("* my port", NodeInfo[i].portnum);
						File f = new File(filepath);
						File file[] = f.listFiles();
						for (int k = 0; k < file.length; k++) {
							String fname = file[k].getName();
							fis = context.openFileInput(fname);
							BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
							data = reader.readLine();
							fis.close();
							reader.close();
							String key = file[k].getName();
							String value = data;
							MatrixCursor.RowBuilder builder = cursor.newRow();
							builder.add("key", key);
							builder.add("value", value);
							Log.e("* my port", key +" " + value);
						}
					} else {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(NodeInfo[i].portnum));
						PrintWriter send = new PrintWriter(socket.getOutputStream(), true);
						InputStreamReader isr = new InputStreamReader(socket.getInputStream());
						BufferedReader br = new BufferedReader(isr);
						String msgToSend = "QA ";
						msgToSend = msgToSend.concat("@");
						send.println(msgToSend);
						String fromserver = br.readLine();

						if (fromserver==null)
						{
							if (Failnode.equals(NodeInfo[i].portnum)) {socket.close(); continue;}
							else {InformFailure(NodeInfo[i].portnum); socket.close(); continue;}
						}

						String[] temp1 = fromserver.split("-");
						Log.v("query *", selection);
						Log.e("* from server " + NodeInfo[i].portnum, fromserver);
						for (int k = 1; k < temp1.length; k++) {
							String[] temp2 = temp1[k].split(" ");
							String key = temp2[0];
							String value = temp2[1];
							MatrixCursor.RowBuilder builder = cursor.newRow();
							builder.add("key", key);
							builder.add("value", value);
						}
						socket.close();
					}
				}
			} else {
				for (int i = 0; i < 5; i++) {
					String hashkey = genHash(FILENAME);
					String hashpre = genHash(String.valueOf(Integer.parseInt(NodeInfo[i].pre) / 2));
					String hashcur = genHash(String.valueOf(Integer.parseInt(NodeInfo[i].portnum) / 2));

					if (((hashcur.compareTo(hashpre) < 0) && (hashkey.compareTo(hashcur) <= 0 || hashkey.compareTo(hashpre) > 0)) ||
							(hashcur.compareTo(hashpre) > 0 && hashkey.compareTo(hashcur) <= 0 && hashkey.compareTo(hashpre) > 0)) {
						Log.e(NodeInfo[i].portnum, Myport);
						if (NodeInfo[i].suc2.equals(Myport)) {
							Log.e("got match", " ");
							int st = 0;
							do {
								try {
									fis = context.openFileInput(FILENAME);
									BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
									data = reader.readLine();
									fis.close();
									reader.close();
									st = 0;
								} catch (Exception e) {
									st = 1;
								}
							} while (st == 1);
							Log.v("data", data);
							MatrixCursor.RowBuilder builder = cursor.newRow();
							builder.add("key", selection);
							builder.add("value", data);
							Log.v("query 3", selection);
							Log.e(selection, data);
							break;
						} else {
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(NodeInfo[i].suc2));
							PrintWriter send = new PrintWriter(socket.getOutputStream(), true);
							InputStreamReader isr = new InputStreamReader(socket.getInputStream());
							BufferedReader br = new BufferedReader(isr);
							String msgToSend = "QS ";
							msgToSend = msgToSend.concat(selection);
							send.println(msgToSend);
							String fromserver = br.readLine();

							if (fromserver==null)
							{
								if (Failnode.equals(NodeInfo[i].suc2)) {}
								else {InformFailure(NodeInfo[i].suc2);}
								Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										Integer.parseInt(NodeInfo[i].suc1));
								PrintWriter send1 = new PrintWriter(socket1.getOutputStream(), true);
								InputStreamReader isr1 = new InputStreamReader(socket1.getInputStream());
								BufferedReader br1 = new BufferedReader(isr1);
								String msgToSend1 = "QS ";
								msgToSend1 = msgToSend1.concat(selection);
								send1.println(msgToSend1);
								fromserver = br1.readLine();
								socket1.close();
							}
							Log.e("from server in Q", fromserver);
							String key = fromserver.split(" ")[0];
							String value = fromserver.split(" ")[1];
							MatrixCursor.RowBuilder builder = cursor.newRow();
							builder.add("key", key);
							builder.add("value", value);
							Log.v("query 5", selection);
							Log.v(selection, fromserver);
							socket.close();
							break;
						}
					}
				}
			}

		}catch (Exception e) {
			Log.e("Query Exception", e.toString());
		}
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
