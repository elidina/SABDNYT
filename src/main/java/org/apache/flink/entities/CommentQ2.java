package org.apache.flink.entities;

public class CommentQ2 {

    private long ts;
    private String type;
    private String articleId;
    private int count;
    private int index;
    private int day;
    private long arrivalTime;

    public CommentQ2(){}

    public CommentQ2(long ts, String type, String articleId, int count, int index){

        this.ts = ts;
        this.type = type;
        this.articleId = articleId;
        this.count = count;
        this.index = index;
    }

    public CommentQ2(long ts, String type, int count, int index, int day, long at){

        this.ts = ts;
        this.type = type;
        this.count = count;
        this.index = index;
        this.day = day;
        this.arrivalTime = at;
    }

    public CommentQ2(long ts, String type, int count, int index, int day){

        this.ts = ts;
        this.type = type;
        this.count = count;
        this.index = index;
        this.day = day;
    }

    public long getArrivalTime() {
        return arrivalTime;
    }

    public void setArrivalTime(long arrivalTime) {
        this.arrivalTime = arrivalTime;
    }

    public long getTs() {

        return ts;
    }

    public void setTs(long ts) {

        this.ts = ts;
    }

    public String getType() {

        return type;
    }

    public void setType(String type) {

        this.type = type;
    }

    public String getArticleId() {

        return articleId;
    }

    public void setArticleId(String articleId) {

        this.articleId = articleId;
    }

    public int getCount() {

        return count;
    }

    public void setCount(int count) {

        this.count = count;
    }

    public int getIndex() {

        return index;
    }

    public void setIndex(int index) {

        this.index = index;
    }

    public int getDay() {

        return day;
    }

    public void setDay(int day) {

        this.day = day;
    }
}

