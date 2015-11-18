package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by priya on 5/5/15.
 */


import java.io.Serializable;


public class Request implements Serializable {

    public String Type;
    public String fromNode;
    public String toNode;
    public String key;
    public String value;






    public String getRequestType() {
        return Type;
    }
    public void setRequestType(String requestType) {
        Type = requestType;
    }
    public String getFromID() {
        return fromNode;
    }
    public void setFromID(String fromID) {
        this.fromNode = fromID;
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
