package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    public SimpleDynamoProvider() {
        super();
    }

    public static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    public static String my_id;
    public static String myPort;
    public static ContentResolver mContentResolver ;
    public static Context mContxt;
    public static final String[] REMOTE_PORTS= {"11108","11112","11116","11120","11124"};
    public static final String[] NODE_IDs= {"5554","5556","5558","5560","5562"};
    public static final int SERVER_PORT = 10000;
    public static Uri mUri;
    public static int insertAckCount = 0;
    public static String key_current;

    public static ArrayList<HashMap> queryRes = null;
    public static int queryAckCount = 0;




    @Override
    public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
        if(selection.equals("\"@\"")){
            localDelete(getContext(), "\"@\"");
        }
        else if(selection.equals("\"*\"")){
            localDelete(getContext(), "\"@\"");
            for (int i = 0; i < REMOTE_PORTS.length; i++) {
                if(!NODE_IDs[i].equals(myPort)){

                    Request requestMsg  = new Request();
                    requestMsg.setKey("\"@\"");
                    requestMsg.setFromID(myPort);
                    requestMsg.setRequestType("DELETE");
                    requestMsg.setToID(NODE_IDs[i]);

                    (new Thread(new Client(requestMsg))).start();
                }
            }
        }
        else{
            String Coord = getCoordId(selection);
            if(Coord.equals(my_id)){
                localDelete(getContext(), selection);
            }
            else {
                Request requestMsg  = new Request();
                requestMsg.setRequestType("DELETE");
                requestMsg.setKey(selection);
                requestMsg.setToID(Coord);
                requestMsg.setFromID(myPort);


                (new Thread(new Client(requestMsg))).start();
            }

            String replica1 = getSuccId(Coord,"s");
            String replica2 = getSuccId(replica1, "s");

            Request requestMsg1  = new Request();
            requestMsg1.setRequestType("DELETE");
            requestMsg1.setFromID(myPort);
            requestMsg1.setToID(replica1);
            requestMsg1.setKey(selection);
            (new Thread(new Client(requestMsg1))).start();



            Request requestMsg2  = new Request();
            requestMsg2.setRequestType("DELETE");
            requestMsg2.setToID(replica2);
            requestMsg2.setFromID(myPort);
            requestMsg2.setKey(selection);

            (new Thread(new Client(requestMsg2))).start();





        }

        return 0;
    }


    public static synchronized void localDelete(Context context, String selection){
        File dir = context.getFilesDir();
        String key = selection;
        for(File x: dir.listFiles()){
            if(key.equals(x.getName()) || key.equals("\"@\"")){
                x.delete();
            }
        }
    }


    private String getCoordId(String key) {
        String CoordNodeId = null;
        ArrayList<String> a = new ArrayList<String>();
        for (int i = 0; i < NODE_IDs.length; i++) {
            a.add(NODE_IDs[i]);
        }
        a.add(key);
        Collections.sort(a, new Comparator<String>() {
            @Override
            public int compare(String b1, String b2) {
                try {
                    return genHash(b1).compareTo(genHash(b2));
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                return 0;
            }
        });
        if(a.indexOf(key)==5){
            CoordNodeId = a.get(0);
        }else{
            int j = a.indexOf(key) + 1;
            CoordNodeId= a.get(j);
        }
        return CoordNodeId;
    }


    private String getSuccId(String node, String flag) {
        String succ = null;
        ArrayList<String> List = new ArrayList<String>();
        List.add("5562");
        List.add("5556");
        List.add("5554");
        List.add("5558");
        List.add("5560");
        if(flag.equals("s")){
            if(List.indexOf(node)==4){
                succ =  List.get(0);
            }else{
                int k = List.indexOf(node) + 1;
                succ = List.get(k);
            }
        }
        else{
            if(List.indexOf(node)==0){
                succ = List.get(4);

            }else{
                int p = List.indexOf(node) - 1;
                succ = List.get(p);

            }

        }


        return succ;
    }









    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized Uri insert(Uri uri, ContentValues values) {
        Log.d("insert","i1");
        insertAckCount = 0;

        String key = null;
        String value = null;

        for (Map.Entry x : values.valueSet()) {
            if (x.getKey().equals("key")) {
                key = (String) x.getValue();
                Log.d("insert","i2");
            } else {
                value = (String) x.getValue();
                Log.d("insert","i3");
            }
        }

        String Coord = getCoordId(key);
        Log.d("insert","i4");
        String replica1 = getSuccId(Coord, "s");
        Log.d("insert","i5");
        String replica2 = getSuccId(replica1, "s");
        Log.d("insert","i6");


        if(my_id.equals(Coord)){
            Log.d("insert","i7");
            localInsert(getContext(), key, value);
            Log.d("insert","i8");
            insertAckCount=2;
            Log.d("insert","i10");

}
        else{
            insertAckCount=3;
            Log.d("insert","i11");

            Request msg = new Request();
            Log.d("insert","i12");
            msg.setRequestType("INSERT");
            Log.d("insert","i13");
            msg.setKey(key);
            Log.d("insert","i14");
            msg.setVal(value);
            Log.d("insert","i15");
            msg.setFromID(my_id);
            Log.d("insert","i16");
            msg.setToID(Coord);
            Log.d("insert","i17");

            (new Thread(new Client(msg))).start();
            Log.d("insert","i18");
            }

        ReplicaMsg rm = new ReplicaMsg();
        rm.setRequestType("INSERT");
        rm.setKey(key);
        rm.setVal(value);
        rm.setFromID(my_id);
        rm.setToID(replica1);
       (new Thread(new Client(rm))).start();
        Log.d("insert","i19");

        ReplicaMsg rm1 = new ReplicaMsg();
        rm1.setRequestType("INSERT");
        rm1.setFromID(my_id);
        rm1.setKey(key);
        rm1.setVal(value);
        rm1.setToID(replica2);
        (new Thread(new Client(rm1))).start();
        Log.d("insert","i20");

        while(insertAckCount>0){
            Log.d("insert","i21");;
        }
        Log.d("insert","i22");
        insertAckCount=0;


         return null;
    }




    public static void localInsert(Context context, String key, String value) {
        FileOutputStream outputStream;
        try {
            outputStream = context.openFileOutput(key, Context.MODE_PRIVATE);
            outputStream.write(value.getBytes());
            outputStream.close();
        } catch (Exception e) {
            Log.e(TAG, "Insert failed",e);
        }
        Log.v("insert::", key + "::"+value + "at:: " + SimpleDynamoProvider.my_id);
    }








    @Override
    public synchronized boolean onCreate() {
        Log.i(SimpleDynamoProvider.TAG, "SimpleDynamoProvider onCreate ");

        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        my_id= portStr;
        mContentResolver = getContext().getContentResolver();
        mContxt = getContext();
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

        File dir = mContxt.getFilesDir();
        deleteStorage(dir);


        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, " No ServerSocket");
        }



        String Succnode_id = getSuccId(my_id, "s");
        String Prenode_id = getSuccId(my_id, "p");

        Recover rv = new Recover();
        rv.setFromID(my_id);
        rv.setToID(Succnode_id);
        (new Thread(new Client(rv))).start();


        Recover rvv = new Recover();
        rvv.setFromID(my_id);
        rvv.setToID(Prenode_id);
        (new Thread(new Client(rvv))).start();


        return false;
    }





    private void deleteStorage(File dir) {

        if(dir.isDirectory()){
            for(File x : dir.listFiles()){
                x.delete();
            }
        }


    }


    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public synchronized Cursor query(Uri uri, String[] projection, String selection,
                                     String[] selectionArgs, String sortOrder) {
      Log.d("query","q1");
        key_current = selection;
        Log.d("query","q2");
        queryRes = null;
        Log.d("query","q3");
        if(queryRes==null){
            Log.d("query","q4");
            queryRes = new ArrayList<HashMap>();
            Log.d("query","q5");
        }
        queryAckCount = 0;

        if(selection.equals("\"@\"")){
            Log.d("query","q6");
            queryRes.addAll(localQuery(getContext(), "\"@\""));
            Log.d("query","q7");
        }

        else if(selection.equals("\"*\"")){
            Log.d("query","q8");
            queryRes.addAll(localQuery(getContext(), "\"@\""));
            Log.d("query","q9");
            queryAckCount = 4;
            Log.d("query","q10");
            for (int i = 0; i < NODE_IDs.length; i++) {
                Log.d("query","q11");
                if(!NODE_IDs[i].equals(myPort)){
                    Log.d("query","q12");
                    Request Msg  = new Request();
                    Log.d("query","q13");
                    Msg.setRequestType("QUERY");
                    Log.d("query","q14");
                    Msg.setKey("\"@\"");
                    Log.d("query","q15");
                    Msg.setToID(NODE_IDs[i]);
                    Log.d("query","q16");
                    Msg.setFromID(my_id);
                    Log.d("query","q17");

                 (new Thread(new Client(Msg))).start();
                    Log.d("query","q18");
                }
            }
        }

        else {
            String Coord = getCoordId(selection);
            Log.d("query","q19");

            if(Coord.equals(my_id)){
                Log.d("query","q20");
                queryRes.addAll(localQuery(getContext(), selection));
                Log.d("query","q21");
            }else {
                queryAckCount = 1;
                Log.d("query","q22");
                Request g = new Request();
                Log.d("query","q23");          // REPEATED ALL AND FORWARDING TO A SINGLE ONE....WRITE COMMON METHOD WITH FLAG FOR D,Q,I
                g.setRequestType("QUERY");
                Log.d("query","q24");
                g.setFromID(my_id);
                Log.d("query","q25");
                g.setToID(Coord);
                Log.d("query","q26");
                g.setKey(selection);
                Log.d("query","q27");
                (new Thread(new Client(g))).start();
                Log.d("query","q28");
            }

        }
        if(!selection.equals("\"@\"")){
            Log.d("query","q29");
             while(queryAckCount>0){
                 Log.d("query","q30");
                ;
            }
            Log.d("query","q31");
        }


        String[] columnNames = {"key", "value"};
        MatrixCursor matrixCursor = new MatrixCursor(columnNames);


        for (int j = 0; j < queryRes.size(); j++) {
            HashMap<String, String> hm = queryRes.get(j);
            Set set = hm.entrySet();
            Iterator it = set.iterator();
            while (it.hasNext()) {
                Map.Entry me = (Map.Entry) it.next();
                Log.d("query","q1");
                String[] ColumnValues = {(String) me.getKey(), (String) me.getValue()};
                Log.d("query","q1");//REFERENCE: Android Documentation
                matrixCursor.addRow(ColumnValues);
                Log.d("query","q1");
            }
        }
        Log.d("query","q32");
        queryRes=null;
        queryAckCount=0;
        key_current=null;

        Log.d("query","q33");
        return matrixCursor;


 }



public static ArrayList<HashMap> localQuery(Context context,String key) {
    Log.d("localquery","lq1");
        File dir = context.getFilesDir();
    Log.d("localquery","lq2");
        HashMap<String,String> hm ;
    Log.d("localquery","lq3");

        ArrayList<HashMap> reslst = new ArrayList<>();
    Log.d("localquery","lq4");

        for(File x : dir.listFiles()){
            if(key.equals(x.getName()) || key.equals("\"@\"")){
                Log.d("localquery","lq5");
              hm  = new HashMap<>();
                Log.d("localquery","lq6");

                StringBuffer b = new StringBuffer();
                Log.d("localquery","lq7");
                try {
                    Log.d("localquery","lq8");
                    FileInputStream fs = context.openFileInput(x.getName());
                    Log.d("localquery","lq9");
                    int data;
                    while((data=fs.read()) != -1){
                        Log.d("localquery","lq10");
                        b.append((char)data);
                        Log.d("localquery","lq11");
                    }
                    fs.read();
                    Log.d("localquery","lq12");
                    fs.close();
                    Log.d("localquery","lq13");
                } catch (FileNotFoundException e) {
                    Log.e(TAG, "Query failed");
                } catch (IOException e) {
                    Log.e(TAG, "Query failed");
                }
                String value = b.toString();
                Log.d("localquery","lq14");
                b.setLength(0);
                Log.d("localquery","lq15");
                hm.put(x.getName(),value);
                Log.d("localquery","lq16");
                reslst.add(hm);
                Log.d("localquery","lq17");

            }
        }

        return reslst;

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