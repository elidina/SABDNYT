import producer.App;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.text.ParseException;
import java.time.Instant;
import java.util.*;

import static java.lang.System.exit;
import static java.lang.Thread.sleep;


public class Main {

    /**
     * L'invio della line avviene dopo aver attesto il tempo tra il timestamp precedente e quello attuale
     * @param args
     */
    public static void main(String[] args) throws IOException, ParseException, InterruptedException {

        App app = new App();

        String topic = "flink";

        //todo prendere dataset dall'esterno del programma
        String csvPath = "dataset/Comments_jan-apr2018.csv";

        BufferedReader br = null;
        String line;
        int i = 0;
        Long actualTimestamp = 0L;

        /*
        Il convertitore ci da rispettivamente per 1° e ultimo timestamp di creazione:
        dd MM yyyy HH:mm:ss
        - 01/01/2018 01:47:41
        - 03/01/2018 06:15:49
        La differenza è di 2 giorni, 5 ore, 28 min, 8 sec /

        Fissando un tempo di esecuzione di 10 min si ottiene che 1 sec va simulato in 3 ms.
         */

        //todo correggere
        //double executionTime = calcolaDurataEsecuzione(csvPath);
        //System.out.println("execution time: " + executionTime);
        double executionTime = 0.003;

        try {

            br = new BufferedReader(new FileReader(csvPath));

            while ((line = br.readLine()) != null) {

                //salto la 1^ riga di String
                if(i>0) {

                    if(i==1500)
                        exit(0);


                    Long newTimestamp = getTimestamp(line);
                    //System.out.println("actualTimestamp: " + actualTimestamp);
                    //System.out.println("newTimestamp: " + newTimestamp);

                    //la 1^ line letta avrà attesa nulla, le altre aspetteranno rispetto all'ultimo timestamp
                    if(i==1) {
                        actualTimestamp = newTimestamp;
                    }
                    int waitTime = calcolaDiffTimestamp(actualTimestamp, newTimestamp);
                    //System.out.println("vado in sleep per: " + waitTime);
                    //System.out.println(waitTime * executionTime);
                    sleep((long) (waitTime*executionTime));

                    app.runProducer(line, i, topic);
                    //System.out.println("invio line n° " + i);

                    actualTimestamp = newTimestamp;
                }
                i++;
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private static double calcolaDurataEsecuzione(String csvPath) throws IOException, ParseException {

        BufferedReader br;
        String line;
        Long firstTimestamp = 0L;
        Long lastTimestamp = 0L;
        int i=0;
        br = new BufferedReader(new FileReader(csvPath));

        while ((line = br.readLine()) != null) {
            if(i == 1) {
                line = lineFilter(line, i);
                firstTimestamp = getTimestamp(line);
            }
            if(i == 8868){
                line = lineFilter(line, i);
                lastTimestamp = getTimestamp(line);
            }
            i++;
        }
        //System.out.println(firstTimestamp + " " + lastTimestamp);

        int timeInSec = calcolaDiffTimestamp(firstTimestamp, lastTimestamp);
        //System.out.println(timeInSec);

        //todo per eseguire in 10 min mettere 10*60=600 al numeratore
        double s = 600/timeInSec;
        //System.out.println(s);

        return s;
    }


    /**
     * Questo metodo calcola la differenza di tempo in secondi tra due timestamp
     * @param firstTimestamp
     * @param lastTimestamp
     * @return
     */
    private static int calcolaDiffTimestamp(Long firstTimestamp, Long lastTimestamp) {

        Date firstDate = Date.from( Instant.ofEpochSecond(firstTimestamp));
        Date lastDate = Date.from( Instant.ofEpochSecond(lastTimestamp));
        /*
        System.out.println(firstDate);
        System.out.println(lastDate);
        */
        Long diffTimestamp = lastTimestamp - firstTimestamp;

        Date diffDate = Date.from( Instant.ofEpochSecond(diffTimestamp));
        //System.out.println(diffDate);
//calcolo giorni, ore, minuti e secondi di differenza ed esprimo tutto in sec
        int timeInSec = (lastDate.getDay()-firstDate.getDay())*24*60*60 + diffDate.getHours()*60*60 +
                diffDate.getMinutes()*60 + diffDate.getSeconds();
        return timeInSec;
    }


    /**
     * Elimino tutti i campi finale della line. Il campo intermedio vuoto per "comment" e utile per "userReply".
     * todo lo sostituisco con "null"/0 nel comment
     * I campi di "userDisplayName" e "userLocation" vengono aggregati se separati in + campi.
     * @param line
     * @return
     */
    public static String lineFilter(String line, int index) {
        //System.out.println("index: " + index);
        String csvSplitBy = ",";
        int i, j;

        StringJoiner joiner = new StringJoiner(csvSplitBy);

        String[] word = line.split(csvSplitBy); //divido la line in parole

        if(index>0) {
            //if(index==278)
            //    exit(0);
            if (word[4].equals("comment")) {
                word[9] = "null";
            }

            //l'obiettivo è trovare il valore del campo UserId. Il campo precedente e successivo vanno aggregati
            //da word[12] inizia l'username.
            j = 12;
            boolean trovato = false;
            while(j<word.length && !trovato) {
                trovato = isNumeric(word[j]);
                if(!trovato)
                    j++;
            }
            //System.out.println("indice: " + i + " " + "valore: " + word[i]);
            for(i=13; i<j; ++i) {
                word[12] = word[12] + word[i];
            }

            joiner = new StringJoiner(csvSplitBy);
            word[14] = word[j+1];
            for(i=j+2; i<word.length; ++i)
                word[14] = word[14] + word[i];
            //System.out.println("*******");
        }

        joiner = new StringJoiner(csvSplitBy);
        //System.out.println("line length: " + word.length);

        //ricreo la line con la sua struttura originale
        for (i = 0; i < 15; ++i) {
            //System.out.println(word[i]);
            joiner.add(word[i]);
        }

        String joinedString = joiner.toString();
        return joinedString;
    }


    public static boolean isNumeric(String word) {
        try {
            int i = Integer.parseInt(word);
        } catch (NumberFormatException | NullPointerException nfe) {
            return false;
        }
        return true;
    }


    public static Long getTimestamp(String line) throws ParseException {
        //System.out.println(line);

        String csvSplitBy = ",";

        String[] word = line.split(csvSplitBy); //divido la line in parole

        String timestamp = word[5];
        //System.out.println(timestamp);
        Long t = Long.parseLong(timestamp);
        //System.out.println(t);

        return t;
    }
}
