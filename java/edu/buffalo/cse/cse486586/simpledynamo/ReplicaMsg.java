package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;

/**
 * Created by priya on 5/6/15.
 */
public class ReplicaMsg
        implements Serializable {
    private static final long serialVersionUID = 1L;
    public String Type; //insert, query, delete, join
    public String fromNode; //5554,5556,...
    public String toNode;   //5554,5556,...
    public String key;    //"str","@","*", null
    public String value;    //"str", null


    public void setRequestType(String requestType) {
       Type = requestType;
    }

    public String getRequestType(){
        return Type;
    }

    public String getFromID() {
        return fromNode;
    }

    public void setFromID(String fromID) {
        fromNode = fromID;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getVal() {
        return value;
    }

    public void setVal(String val) {
      value = val;
    }

    public String getToID() {
        return toNode;
    }

    public void setToID(String toID) {
        toNode = toID;
    }
}
