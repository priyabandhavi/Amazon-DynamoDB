package edu.buffalo.cse.cse486586.simpledynamo;

import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by priya on 5/5/15.
 */
public class ServerTask extends AsyncTask<ServerSocket, String, Void> {


    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";


    @Override
    protected Void doInBackground(ServerSocket... serverSockets) {

        Log.d("server","s1");
        ServerSocket serverSocket = serverSockets[0];
        Socket socket = null;
        while (true) {
            Log.d("server","s2");
            try {
                Log.d("server","s3");
                socket = serverSocket.accept();
                Log.d("server","s4");

                InputStream is = socket.getInputStream();
                Log.d("server","s5");
                BufferedInputStream bis = new BufferedInputStream(is);
                Log.d("server","s6");
                ObjectInputStream ois = new ObjectInputStream(bis);
                Log.d("server","s7");
                Object receivedmsg = ois.readObject();
                Log.d("server","s8");


                OutputStream os = socket.getOutputStream();
                Log.d("server","s8");
                BufferedOutputStream bos = new BufferedOutputStream(os);
                Log.d("server","s9");
                ObjectOutputStream obops = new ObjectOutputStream(bos);
                Log.d("server","s10");
                obops.writeObject("ack");
                Log.d("server","s11");
                obops.flush();
                Log.d("server","s12");


                if (receivedmsg instanceof Request) {
                    Log.d("server","s13");
                    Request r = (Request) receivedmsg;
                    if (r.getRequestType().equals("DELETE")) {
                        SimpleDynamoProvider.localDelete(SimpleDynamoProvider.mContxt, r.getKey());
                    }
                    else if(r.getRequestType().equals("INSERT")){
                        Log.d("server","s14");
                        String key = r.getKey();
                        Log.d("server","s15");
                        String val = r.getVal();
                        Log.d("server","s16");
                        String Coord = getCoordId(key);
                        Log.d("server","s17");
                        if(Coord.equals(SimpleDynamoProvider.my_id)){
                            Log.d("server","s18");
                         SimpleDynamoProvider.localInsert(SimpleDynamoProvider.mContxt, key, val);
                            Log.d("server","s19");

                            AckMsgReplica amr = new AckMsgReplica();
                            Log.d("server","s20");
                            amr.setFromID(SimpleDynamoProvider.my_id);
                            Log.d("server","s21");
                            amr.setToID(r.getFromID());
                            Log.d("server","s22");
                            (new Thread(new Client(amr))).start();
                            Log.d("server","s23");
                        }

                    }
                    else if(r.getRequestType().equals("QUERY")){
                        Log.d("server","sq1");
                        String key = r.getKey();
                        Log.d("server","sq2");

                        ArrayList<HashMap>resultList = SimpleDynamoProvider.localQuery(SimpleDynamoProvider.mContxt, key);
                        Log.d("server","sq3");

                        ResQue queryResultMsg = new ResQue(SimpleDynamoProvider.my_id, r.getFromID());
                        Log.d("server","sq4");
                        queryResultMsg.setQueryKey(key);
                        Log.d("server","sq5");
                        queryResultMsg.setQueResList(resultList);
                        Log.d("server","sq6");
                        (new Thread(new Client(queryResultMsg))).start();
                        Log.d("server","sq7");
                    }
                }
                else if(receivedmsg instanceof ReplicaMsg){
                    Log.d("server","s24");
                    ReplicaMsg m = (ReplicaMsg)receivedmsg;
                    Log.d("server","s25");
                    SimpleDynamoProvider.localInsert(SimpleDynamoProvider.mContxt, m.getKey(), m.getVal());
                    Log.d("server","s26");

                    AckMsgReplica ack = new AckMsgReplica();
                    Log.d("server","s27");
                    ack.setToID(m.getFromID());
                    Log.d("server","s28");
                    ack.setFromID(SimpleDynamoProvider.my_id);
                    Log.d("server","s29");
                    (new Thread(new Client(ack))).start();
                    Log.d("server","s30");

                 }
                else if(receivedmsg instanceof AckMsgReplica){
                    Log.d("server","s31");
                    SimpleDynamoProvider.insertAckCount--;
                    Log.d("server","s32");
                }
                else if(receivedmsg instanceof ResQue){
                    ResQue q = (ResQue)receivedmsg;

                    if(SimpleDynamoProvider.key_current!=null && !SimpleDynamoProvider.key_current.equals("\"@\"") && !SimpleDynamoProvider.key_current.equals("\"*\"")){

                        if(!(q.getQueResList().size()>0)
                                && q.getQueryKey().equals(SimpleDynamoProvider.key_current) && (q.getQueResList()!=null) ){


                            Request req = new Request();
                            req.setRequestType("QUERY");
                            req.setKey(SimpleDynamoProvider.key_current);
                            String c = getCoordId(SimpleDynamoProvider.key_current);
                            req.setToID(getSuccID(c,"s"));
                            req.setFromID(SimpleDynamoProvider.my_id);
                            (new Thread(new Client(req))).start();


                        }else if(q!=null && (q.getQueResList()!=null) && (q.getQueResList().size()>0)
                                && (SimpleDynamoProvider.key_current!=null)
                                && (SimpleDynamoProvider.key_current.equals(q.getQueryKey()))) {


                            if (SimpleDynamoProvider.queryRes == null) {
                                SimpleDynamoProvider.queryRes = new ArrayList<HashMap>();
                            }
                            SimpleDynamoProvider.queryRes.addAll(q.getQueResList());

                            //}
                            SimpleDynamoProvider.queryAckCount--;
                        }


                    }else{
                        if(SimpleDynamoProvider.queryRes == null){
                            SimpleDynamoProvider.queryRes = new ArrayList<HashMap>();
                        }
                        SimpleDynamoProvider.queryRes.addAll(q.getQueResList());


                        SimpleDynamoProvider.queryAckCount--;
                    }



                }


                else if(receivedmsg instanceof Recover){
                    Recover recovery = (Recover)receivedmsg;

                    ArrayList<HashMap>result = SimpleDynamoProvider.localQuery(SimpleDynamoProvider.mContxt, "\"@\"");



                    RecoverAck a = new RecoverAck(SimpleDynamoProvider.my_id, recovery.fromNode);
                    a.setQueResList(result);
                    (new Thread(new Client(a))).start();


                }
                else if(receivedmsg instanceof RecoverAck){
                    RecoverAck ra = (RecoverAck)receivedmsg;

                    ArrayList<HashMap> recoveryResList = ra.getQueResList();


                    for (int j = 0; j < recoveryResList.size(); j++) {
                        HashMap<String, String> hm = recoveryResList.get(j);
                        Set set = hm.entrySet();
                        Iterator it = set.iterator();
                        while (it.hasNext()) {
                            Map.Entry me = (Map.Entry) it.next();
                            String coordinatorID = getCoordId((String)me.getKey());
                            String pred1 = getSuccID(SimpleDynamoProvider.my_id, "p");
                            String pred2 = getSuccID(pred1, "p");
                            if(coordinatorID.equals(SimpleDynamoProvider.my_id)
                                    || coordinatorID.equals(pred1)
                                    || coordinatorID.equals(pred2)){
                                SimpleDynamoProvider.localInsert(SimpleDynamoProvider.mContxt, (String)me.getKey(), (String)me.getValue());
                            }

                                                          //REFERENCE: Android Documentation

                        }
                    }






                }




            } catch (IOException e) {
                Log.e(SimpleDynamoProvider.TAG, "Server Exception", e);
            } catch (ClassNotFoundException e) {
                Log.e(SimpleDynamoProvider.TAG, "Server Exception", e);
            }



        }

    }


    private String getCoordId(String key) {
        String CoordNodeId = null;
        ArrayList<String> a = new ArrayList<String>();
        for (int i = 0; i < SimpleDynamoProvider.NODE_IDs.length; i++) {
            a.add(SimpleDynamoProvider.NODE_IDs[i]);
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

    private String getSuccID(String node, String flag) {
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
