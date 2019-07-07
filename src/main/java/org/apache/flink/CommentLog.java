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
        CommentLog cl = new CommentLog();

        if(s.contains("https:") || s.contains("http:")){
            cl.userID=0;
            return cl;
        }

        s = s.replace(",,",",null,");
        String[] attributes = s.split(",");
        int index = attributes.length;

        if(index < 14){
            throw new RuntimeException( "Invalid record: " + s );
        }

        if(attributes[0].equals("null")){
            attributes = Arrays.copyOfRange(attributes,1,index);
        }


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



        cl.recommendations = Integer.parseInt(attributes[10+j]);
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


        //errore amp
        if(isNumeric(attributes[13+i+j])){
            cl.userID = Integer.parseInt(attributes[13+i+j]);
        }else{
            cl.userID = 0;
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
