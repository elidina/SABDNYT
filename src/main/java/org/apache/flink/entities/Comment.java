package org.apache.flink.entities;

import java.util.Arrays;

public class Comment {

    public long approveDate;
    public String articleID;
    public int articleWordCount;
    public int commentID;
    public String commentType;
    public long createDate;
    public int depth;
    public Boolean editorsSelection;
    public int inReplyTo;
    public String parentUserDisplayName;
    public int recommendations;
    public String sectionName;
    public String userDisplayName;
    public int userID;
    public String userLocation;
    public String error;
    public long arrivalTime;

    public Comment(){

    }

    public static Comment fromString(String s){
        Comment cl = new Comment();
        cl.arrivalTime = System.currentTimeMillis();
        //System.out.println("****" + cl.arrivalTime);

        if(s.contains("https:") || s.contains("http:")){
            cl.error = "http";
            cl.userID=0;
            return cl;
        }

        s = s.replace(",,",",null,");
        String[] attributes = s.split(",");
        int index = attributes.length;

        if(index < 14){
            cl.error = "invalid record";

            throw new RuntimeException( "Invalid record: " + s );
        }

        if(attributes[0].equals("null")){
            attributes = Arrays.copyOfRange(attributes,1,index);
        }


        cl.approveDate = Long.parseLong(attributes[0]);
        cl.articleID = attributes[1];

        try{
            cl.articleWordCount = Integer.parseInt(attributes[2]);
        }catch(Exception e){
            cl.error = "word count";

            cl.userID = 0;
            return cl;
        }

        try{
            cl.commentID = Integer.parseInt(attributes[3]);
        }catch(Exception e){
            cl.error = "commentid";

            cl.userID = 0;
            return cl;
        }

        cl.commentType = attributes[4];

        try{
            cl.createDate = Long.parseLong(attributes[5]);
        }catch(Exception e){
            cl.error = "createdate";

            cl.userID = 0;
            return cl;
        }

        try{
            cl.depth = Integer.parseInt(attributes[6]);
        }catch(Exception e){
            cl.error = "depth";

            cl.userID = 0;
            return cl;
        }

        if(attributes[7].equals("False")){
            cl.editorsSelection = false;
        }else{
            cl.editorsSelection = true;
        }

        try{
            cl.inReplyTo = Integer.parseInt(attributes[8]);
        }catch(Exception e){
            cl.error = "inreplyto";

            cl.userID = 0;
            return cl;
        }

        int j = 0;
        if(attributes[9].contains("\"")){
            if(attributes[10].contains("\"")){
                cl.parentUserDisplayName = attributes[9] + attributes[10];
                j=1;
            }else{
                cl.parentUserDisplayName = attributes[9] + attributes[10] + attributes[11];
                j=2;
            }
        }else{
            cl.parentUserDisplayName = attributes[9];
        }

        try{
            cl.recommendations = Integer.parseInt(attributes[10+j]);
        }catch(Exception e){
            cl.error = "recs";

            cl.userID = 0;
            return cl;
        }

        cl.sectionName = attributes[11+j];

        int i = 0;

        if(attributes[12+j] != null){
            if(attributes[12+j].contains("\"")){
                if(attributes[13+j].contains("\"")){
                    cl.userDisplayName = attributes[12+j] + attributes[13+j];
                    i=1;
                }
                else{

                    cl.userDisplayName = attributes[12+j] + attributes[13+j] + attributes[14+j];
                    i=2;
                }

            }else{
                cl.userDisplayName = attributes[12+j];
            }

        }else{
            cl.userDisplayName = null;
        }

        try{
            cl.userID = Integer.parseInt(attributes[13+i+j]);
        }catch(Exception e){
            cl.error = "userid: "+attributes[13+i+j];

            cl.userID = 0;
            return cl;
        }


        if(attributes[14+i+j].contains("\"")){
            cl.userLocation = attributes[14+i+j] + " - " + attributes[15+i+j];
        }else{
            cl.userLocation = attributes[14+i+j];
        }

        return cl;
    }

    @Override
    public String toString() {
        return "Comment{" +
                "approveDate=" + approveDate +
                ", articleID='" + articleID + '\'' +
                ", articleWordCount=" + articleWordCount +
                ", commentID=" + commentID +
                ", commentType='" + commentType + '\'' +
                ", createDate=" + createDate +
                ", depth=" + depth +
                ", editorsSelection=" + editorsSelection +
                ", inReplyTo=" + inReplyTo +
                ", parentUserDisplayName='" + parentUserDisplayName + '\'' +
                ", recommendations=" + recommendations +
                ", sectionName='" + sectionName + '\'' +
                ", userDisplayName='" + userDisplayName + '\'' +
                ", userID=" + userID +
                ", userLocation='" + userLocation + '\'' +
                ", ERROR='" + error + '\'' +
                '}';
    }

    public static boolean isNumeric(String word) {
        try {
            int i = Integer.parseInt(word);
        } catch (NumberFormatException | NullPointerException nfe) {
            return false;
        }
        return true;
    }
}
