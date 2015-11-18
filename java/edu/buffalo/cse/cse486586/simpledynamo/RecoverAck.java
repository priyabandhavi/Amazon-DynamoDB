package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by priya on 5/7/15.
 */
public class RecoverAck implements Serializable {


    private static final long serialVersionUID = 1L;
    public ArrayList<HashMap> List = null;
    public String toNode =null;
    public String fromNode =null;

    public RecoverAck(String from, String to) {
        fromNode = from;
        toNode = to;
    }
    public ArrayList<HashMap> getQueResList() {
        return List;
    }

    public void setQueResList(ArrayList<HashMap> que) {
        List = que;
    }

    public String getToId() {
        return toNode;
    }



}
