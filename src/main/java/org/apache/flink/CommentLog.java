package org.apache.flink;

import java.util.Arrays;

public class CommentLog {

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

    public CommentLog(){

    }

    public static CommentLog fromString(String s){
        s = s.replace(",,",",null,");
        String[] attributes = s.split(",");
        int index = attributes.length;

        if(index < 14){
            throw new RuntimeException( "Invalid record: " + s );
        }

        if(attributes[0].equals("null")){
            attributes = Arrays.copyOfRange(attributes,1,index);
        }

        CommentLog cl = new CommentLog();
        cl.approveDate = Long.parseLong(attributes[0]);
        cl.articleID = attributes[1];
        cl.articleWordCount = Integer.parseInt(attributes[2]);
        cl.commentID = Integer.parseInt(attributes[3]);
        cl.commentType = attributes[4];
        cl.createDate = Long.parseLong(attributes[5]);
        cl.depth = Integer.parseInt(attributes[6]);

        if(attributes[7].equals("False")){
            cl.editorsSelection = false;
        }else{
            cl.editorsSelection = true;
        }

        cl.inReplyTo = Integer.parseInt(attributes[8]);
        cl.parentUserDisplayName = attributes[9];
        cl.recommendations = Integer.parseInt(attributes[10]);
        cl.sectionName = attributes[11];

        int i = 0;
        if(attributes[12].contains("\"")){
            cl.userDisplayName = attributes[12] + attributes[13];
            i=1;
        }else{
            cl.userDisplayName = attributes[12];
        }

        //errore amp
        if(isNumeric(attributes[13+i])){
            cl.userID = Integer.parseInt(attributes[13+i]);
        }else{
            cl.userID = 0;
        }


        if(attributes[14+i].contains("\"")){
            cl.userLocation = attributes[14+i] + " - " + attributes[15+i];
            i=1;
        }else{
            cl.userLocation = attributes[14+i];
        }

        return cl;
    }

    @Override
    public String toString() {
        return "CommentLog{" +
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
