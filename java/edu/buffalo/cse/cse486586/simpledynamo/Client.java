package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;

/**
 * Created by priya on 5/5/15.
 */
public class Client implements Runnable {


    Object receivedMsg = null;

    public Client(Object msgToSend) {
        this.receivedMsg = msgToSend;
    }


    @Override
    public void run() {

        Integer remotePort = null;
        if (receivedMsg instanceof Request) {
            Request r = (Request) receivedMsg;
            remotePort = Integer.parseInt(r.getToID()) * 2;
            sendMessage(receivedMsg, remotePort, r.getRequestType());

        }

        else if(receivedMsg instanceof ReplicaMsg){
            ReplicaMsg rpm = (ReplicaMsg) receivedMsg;
            remotePort = Integer.parseInt(rpm.getToID())*2;
            sendMessage(receivedMsg, remotePort, rpm.getRequestType());

        }
        else{
            if(receivedMsg instanceof AckMsgReplica){
                AckMsgReplica rr = (AckMsgReplica) receivedMsg;
                remotePort = Integer.parseInt(rr.getToID()) * 2;
            }
            else if(receivedMsg instanceof Recover){
                Recover c = (Recover)receivedMsg;
                remotePort = Integer.parseInt(c.getToID()) * 2;
            }
            else if(receivedMsg instanceof ResQue){
                ResQue q = (ResQue) receivedMsg;
                remotePort = Integer.parseInt(q.getToId()) * 2;
            }
            else if(receivedMsg instanceof RecoverAck){
                RecoverAck ar = (RecoverAck) receivedMsg;
                remotePort = Integer.parseInt(ar.getToId()) * 2;
            }

            sendMessage(receivedMsg, remotePort, null);



        }






    }


    private void sendMessage(Object msgToSend, Integer remotePort, String Type) {
        Log.d(SimpleDynamoProvider.TAG,"c1");

        OutputStream os = null;
        Log.d(SimpleDynamoProvider.TAG,"c1");
        BufferedOutputStream bos = null;
        Log.d(SimpleDynamoProvider.TAG,"c2");
        ObjectOutputStream oos = null;
        Log.d(SimpleDynamoProvider.TAG,"c3");
        Socket socket = null;
        Log.d(SimpleDynamoProvider.TAG,"c4");
        try {
            Log.d(SimpleDynamoProvider.TAG,"c5");
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), remotePort);
            Log.d(SimpleDynamoProvider.TAG,"c6");
            os = socket.getOutputStream();
            Log.d(SimpleDynamoProvider.TAG,"c7");
            bos = new BufferedOutputStream(os);
            Log.d(SimpleDynamoProvider.TAG,"c8");
            oos = new ObjectOutputStream(bos);
            Log.d(SimpleDynamoProvider.TAG,"c9");
            //oos.reset();
            oos.writeObject(msgToSend);
            Log.d(SimpleDynamoProvider.TAG,"c10");
            oos.flush();
            Log.d(SimpleDynamoProvider.TAG,"c11");


            if(Type!=null && !(Type.equals("DELETE"))){

                Log.d(SimpleDynamoProvider.TAG,"c12");
                socket.setSoTimeout(2300);
                Log.d(SimpleDynamoProvider.TAG,"c13");
                InputStream is = socket.getInputStream();
                Log.d(SimpleDynamoProvider.TAG,"c14");
                BufferedInputStream bis = new BufferedInputStream(is);
                Log.d(SimpleDynamoProvider.TAG,"c15");
                ObjectInputStream ois = new ObjectInputStream(bis);
                Log.d(SimpleDynamoProvider.TAG,"c16");
                Object ackObj = ois.readObject();
                Log.d(SimpleDynamoProvider.TAG,"c17");
                if(ackObj == null){
                    Log.d(SimpleDynamoProvider.TAG,"c18");
                    if(Type.equals("INSERT")){
                        Log.d(SimpleDynamoProvider.TAG,"c19");
                        SimpleDynamoProvider.insertAckCount--;
                        Log.d(SimpleDynamoProvider.TAG,"c20");
                    }

                    else if(Type.equals("QUERY")){
                        Log.d(SimpleDynamoProvider.TAG,"cq1");
                        Request req = (Request)msgToSend;
                        Log.d(SimpleDynamoProvider.TAG,"cq2");
                        if(!req.getKey().equals("@")){
                            Log.d(SimpleDynamoProvider.TAG,"cq3");
                            String coordinator = getCoordId(req.getKey());
                            String sccucId = getSuccId(coordinator);
                            if(req.getToID().equals(coordinator)){
                                Integer sucessorPort = Integer.parseInt(sccucId)*2;
                                Log.d(SimpleDynamoProvider.TAG,"cq4");
                                req.setToID(sccucId);
                                req.setRequestType("QUERY");
                                Log.d(SimpleDynamoProvider.TAG,"cq5");
                                sendMessage(req, sucessorPort, req.getRequestType());
                                Log.d(SimpleDynamoProvider.TAG,"cq6");
                            }

                        }else{
                            Log.d(SimpleDynamoProvider.TAG,"cq7");
                            SimpleDynamoProvider.queryAckCount--;
                            Log.d(SimpleDynamoProvider.TAG,"cq8");
                        }
                    }
                }
            }

            oos.close();
            Log.d(SimpleDynamoProvider.TAG,"c21");
            os.flush();
            Log.d(SimpleDynamoProvider.TAG,"c22");
            os.close();
            Log.d(SimpleDynamoProvider.TAG,"c23");
            socket.close();
            Log.d(SimpleDynamoProvider.TAG,"c24");
        } catch (ClassNotFoundException e) {
            Log.e(SimpleDynamoProvider.TAG, "ClassNotFoundException", e);
        } catch (UnknownHostException e) {
            Log.e(SimpleDynamoProvider.TAG, "ClientTask UnknownHostException", e);
        } catch (IOException e) {
            Log.e(SimpleDynamoProvider.TAG, "IOException", e);

            try {
                oos.flush();
                oos.close();
                os.flush();
                os.close();
                socket.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }


            if(Type.equals("INSERT")){
                Log.d(SimpleDynamoProvider.TAG,"c25");
                SimpleDynamoProvider.insertAckCount--;
                Log.d(SimpleDynamoProvider.TAG,"c26");
            }

            else if(Type.equals("QUERY")){
                Log.d(SimpleDynamoProvider.TAG,"c27");
                Request req = (Request)msgToSend;
                Log.d(SimpleDynamoProvider.TAG,"c28");
                if(!req.getKey().equals("@")){
                    Log.d(SimpleDynamoProvider.TAG,"c29");

                    String coordinator = getCoordId(req.getKey());
                    String sccucId = getSuccId(coordinator);
                    if(req.getToID().equals(coordinator)){
                        Integer sucessorPort = Integer.parseInt(sccucId)*2;
                        Log.d(SimpleDynamoProvider.TAG,"c30");
                        req.setToID(sccucId);
                        req.setRequestType("QUERY");
                        sendMessage(req, sucessorPort, req.getRequestType());
                        Log.d(SimpleDynamoProvider.TAG,"c31");
                    }

                }else{
                    SimpleDynamoProvider.queryAckCount--;
                    Log.d(SimpleDynamoProvider.TAG,"c32");
                }
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



    private String getSuccId(String nodeId) {
        String ans = null;
        ArrayList<String> nodeList = new ArrayList<String>();
        nodeList.add("5562");
        nodeList.add("5556");
        nodeList.add("5554");
        nodeList.add("5558");
        nodeList.add("5560");
        if(nodeList.indexOf(nodeId)==4){
            ans = nodeList.get(0);

        }else{
            int a = nodeList.indexOf(nodeId)+1;
            ans = nodeList.get(a);

        }
        return ans;
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







