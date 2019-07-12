package org.apache.flink.entities;

import java.io.Serializable;

public class ArticleObject implements Serializable {

    public String article;
    public int comment;
    public long ts;

    public ArticleObject(){

    }

    public ArticleObject(String article, int comment, long ts) {
        this.article = article;
        this.comment = comment;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "ArticleObject{" +
                "article='" + article + '\'' +
                ", comment=" + comment +
                ", ts='" + ts + '\'' +
                '}';
    }

    public String getArticle() {
        return article;
    }

    public void setArticle(String article) {
        this.article = article;
    }

    public int getComment() {
        return comment;
    }

    public void setComment(int comment) {
        this.comment = comment;
    }
}
