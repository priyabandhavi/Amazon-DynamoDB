package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;

/**
 * Created by priya on 5/6/15.
 */
public class Recover implements Serializable {
    private static final long serialVersionUID = 1L;
    public String fromNode;
    public String toNode;



    public void setFromID(String fromID) {
        fromNode = fromID;
    }


    public String getToID() {
        return toNode;
    }
    public void setToID(String toID) {
        toNode = toID;
    }
}
