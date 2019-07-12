package org.apache.flink.utils;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class TimestampHandler {

    /**
     * Metodo per estrarre da una data l'informazione relativa al "giorno" di creazione del commento
     * @param createDate
     * @return
     */
    public static int estraiGiorno(long createDate) {

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(createDate);

        Date date = calendar.getTime();
        int day = date.getDay();

        return day;
    }

    public static int estraiOra(long createDate) {

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(createDate);

        Date date = calendar.getTime();
        int hour = date.getHours();

        return hour;
    }

    /**
     * Calcolo l'indice della finestra a cui appartiene la stringa, usando il timestamp passato come parametro
     * @param createDate
     * @return
     */
    public static int calcolaIndex(long createDate) {
        int index = 0;

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(createDate);

        Date date = calendar.getTime();
        int hour = date.getHours();

        if(hour==0 || hour==1)
            index = 1;
        if(hour==2 || hour==3)
            index = 2;
        if(hour==4 || hour==5)
            index = 3;
        if(hour==6 || hour==7)
            index = 4;
        if(hour==8 || hour==9)
            index = 5;
        if(hour==10 || hour==11)
            index = 6;
        if(hour==12 || hour==13)
            index = 7;
        if(hour==14 || hour==15)
            index = 8;
        if(hour==16 || hour==17)
            index = 9;
        if(hour==18 || hour==19)
            index = 10;
        if(hour==20 || hour==21)
            index = 11;
        if(hour==22 || hour==23)
            index = 12;

        return index;
    }
}
