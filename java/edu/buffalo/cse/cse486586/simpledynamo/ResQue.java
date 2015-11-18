package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by priya on 5/7/15.
 */
public class ResQue implements Serializable {

    private static final long serialVersionUID = 1L;
    public ArrayList<HashMap> queResList = null;
    public String toNode =null;
    public String fromNode =null;
    public String Key=null;

    public String getQueryKey() {
        return Key;
    }
    public void setQueryKey(String K) {
        Key = K;
    }


    public ResQue(String from, String to) {
        fromNode = from;
        toNode = to;
    }
    public ArrayList<HashMap> getQueResList() {
        return queResList;
    }

    public void setQueResList(ArrayList<HashMap> List) {
        this.queResList = List;
    }

    public String getToId() {
        return toNode;
    }


}
