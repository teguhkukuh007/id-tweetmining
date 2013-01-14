/*
 *  Copyright (C) 2010 yudi wibisono (yudi1975@gmail.com)
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package edu.upi.cs.tweetmining;

import java.io.*;
import java.util.ArrayList;
import java.util.logging.Logger;

/**
 *
 * @author Yudi Wibisono (yudi@upi.edu)
 * Generic Preprocessing and util
 * 
 * Class ini dan PropSynStopWord akan digantikan oleh PreprocessingDB yang menggunakan database
 * 
 */

public class GenericPrepro {
    
    public void removeTimeStamp(String inputFile) {
      //sebelum proses bigram
      Logger log = Logger.getLogger("edu.cs.upi.tweetmining");
      String inputFileWoExt = inputFile.substring(0, inputFile.lastIndexOf('.')); //without ext
      String namaFileOutput = inputFileWoExt+"_notimestamp.txt";
      try {
            PrintWriter pw = new PrintWriter(namaFileOutput);
            FileInputStream fstream = new FileInputStream(inputFile);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            int cc=0;
            while ((strLine = br.readLine()) != null)   {
                cc++;
                System.out.println(cc);
                pw.println(strLine.substring(21));
            }
            pw.close();
            br.close();
            in.close();
            fstream.close();
            } catch (Exception e) {
                log.severe(e.toString());
       }
    }


    public void convert(String inputFile) {
      //convert ke  'standard format' yaiut  timestamp[dua spasi]isi  --> 2010-08-29T12:53:32Z  uyee. RT @promoTSELjkt: Paling Indonesia! Telkomsel cinta Indonesia, ayo donlot aplikasi LoveIndonesia di BB, gratis! http://mtw.tl/lpcy3a
      //input:  contoh "tanggal : 2011-04-05T23:54:09Z,Isi : via @Aditz92: 6.50 kegiatan peningkatan Jln Soekarno Hatta arah Cibiru setelah perempatan Kopo. Arus kendaraan tersendat #lalinbdg"
      //output: timestamp[dua spasi]isi   (_std)
      Logger log = Logger.getLogger("edu.cs.upi.tweetmining");
      String inputFileWoExt = inputFile.substring(0, inputFile.lastIndexOf('.')); //without ext
      String namaFileOutput = inputFileWoExt+"_std.txt";
      try {
            PrintWriter pw = new PrintWriter(namaFileOutput);
            FileInputStream fstream = new FileInputStream(inputFile);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine,strTimeStamp,strContent;
            int posIsi;
            int cc=0;
            while ((strLine = br.readLine()) != null)   {
                cc++;
                System.out.println(cc);
                strLine = strLine.trim();
                strTimeStamp = strLine.substring(10,30);  //timestamp
                posIsi = strLine.indexOf("Isi");
                if (strLine.charAt(strLine.length()-1)=='"')
                {
                    strContent = strLine.substring(posIsi+6,strLine.length()-1);
                }  else {
                    strContent = strLine.substring(posIsi+6);
                }
                pw.println(strTimeStamp+"  "+strContent);  //dua spasi
            }
            pw.close();
            br.close();
            in.close();
            fstream.close();
            } catch (Exception e) {
                log.severe(e.toString());
       }
    }



    public void removeTweetWithoutKeyword(String inputFile, String keywordFile) {
    //input file: text (case sudah dijadikan huruf kecil)
    //output file: baris yang mengandung minimal satu keyword yg berada di keywordFile (_withkeyword)
    //             baris yang dibuang karena tidak mengandung keyword (_withoutkeyword)

    Logger log = Logger.getLogger("edu.cs.upi.tweetmining");
    String inputFileWoExt = inputFile.substring(0, inputFile.lastIndexOf('.')); //without ext
    String namaFileOutput = inputFileWoExt+"_withkeyword.txt";
    String namaFileOutputDump = inputFileWoExt+"_withoutkeyword.txt"; //buangan
    ArrayList<String> alKeyWord = new ArrayList<String>();      //kata stopwordss

    //load keyword
    try {
            FileInputStream fstream = new FileInputStream(keywordFile);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            int cc=0;
            while ((strLine = br.readLine()) != null)   {
                cc++;
                alKeyWord.add(strLine);
            }
        } catch (Exception e) {
            log.severe(e.toString());
            //e.printStackTrace();
    } //try

     try {
            PrintWriter pw = new PrintWriter(namaFileOutput);
            PrintWriter pwDump = new PrintWriter(namaFileOutputDump);
            FileInputStream fstream = new FileInputStream(inputFile);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            int cc=0;
            boolean found;
            //while (sc.hasNextLine()) {
            while ((strLine = br.readLine()) != null)   {
                cc++;
                System.out.println(cc);
                found=false;
                for (String keyWord : alKeyWord) {
                    if (strLine.contains(keyWord)) {  //ketemu keyword
                        found = true;
                        //save
                        pw.println(strLine);
                        break;  //cukup satu yang disave
                    }
                }
                if (!found) {
                    pwDump.println(strLine);   //tidak mengandung keyword
                }
            }
            pw.close();
            pwDump.close();
            br.close();
            in.close();
            fstream.close();
            } catch (Exception e) {
                log.severe(e.toString());
       }
    }


    
    public void removeDuplicateTweet(String inputFile) {
    //input file: text, asumsi sudah *disort*, baris yg sama terletak berdekatan
    //output file: baris yang sama hanya dimunculkan sekali (mirip select distinct)
        Logger log = Logger.getLogger("edu.cs.upi.tweetmining");
        String inputFileWoExt = inputFile.substring(0, inputFile.lastIndexOf('.')); //without ext
        String namaFileOutput = inputFileWoExt+"_nodup.txt";
        try {
            PrintWriter pw = new PrintWriter(namaFileOutput);
            FileInputStream fstream = new FileInputStream(inputFile);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine,strLine2="";
            int cc=0;
            while ((strLine = br.readLine()) != null)   {
                cc++;
                System.out.println(cc);
                if (!strLine.equals(strLine2)) {
                    pw.println(strLine); //salin
                    strLine2 = strLine;
                }
            }
            pw.close();
            br.close();
            } catch (Exception e) {
                log.severe(e.toString());
            }
    }
    
    public void process2 (String inputFile) {
	     /* tidak seketat yang process, supaya masih bisa dibaca
	      *    -casefolding
	      *    -buang tweet <10 char
	      */
        Logger log = Logger.getLogger("edu.cs.upi.tweetmining");
        String inputFileWoExt = inputFile.substring(0, inputFile.lastIndexOf('.')); //without ext
        String namaFileOutput = inputFileWoExt+"_preprolunak.txt";
        String namaFileOutputWoTs = inputFileWoExt+"_preprolunak_nots.txt";  //tanpa timestamp
        try {
            PrintWriter pw = new PrintWriter(namaFileOutput);
            PrintWriter pw2 = new PrintWriter(namaFileOutputWoTs);
            FileInputStream fstream = new FileInputStream(inputFile);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            String msg="";
            String ts="";
            int cc=0;
            while ((strLine = br.readLine()) != null)   {
                cc++;
                ts = strLine.substring(0,20);
                //ambil msg, split dengan time
                msg = strLine.substring(22);
                System.out.println(cc);
                if (msg.contains("4sq.com")) {continue;}  //skip tweets yg mengandung foursquare
                msg = msg.toLowerCase();                  //casefolding
                if (msg.length()>10)                      //skip message yg <10 char
                {
                    pw.println(ts+"  "+msg);  //tulis ke file dengan timestamp
                    pw2.println(msg);  //tulis tanpa timestamp
               }
            } //while
            pw.close();
            pw2.close();
        } catch (Exception e) {
            //e.printStackTrace();
            log.severe(e.toString());
        }
    }

    public void process(String inputFile) {
    /*
     sebaiknya jalankan removeDup terlebih dulu
     * prepro:
     *  - skip tweets yg mengandung foursquare
     *  - casefolding
     *  - buang @xxxx
     *  - buang url
     *  - buang selain a z 0-9
     *  - buang tweet yg <20 char
     input: file mentah twitter, contoh:
         2010-08-29T12:53:32Z  uyee. RT @promoTSELjkt: Paling Indonesia! Telkomsel cinta Indonesia, ayo donlot aplikasi LoveIndonesia di BB, gratis! http://mtw.tl/lpcy3a
     output: file hasil prepro dalam format yg sama
    */
        Logger log = Logger.getLogger("edu.cs.upi.tweetmining");
        String inputFileWoExt = inputFile.substring(0, inputFile.lastIndexOf('.')); //without ext
        String namaFileOutput = inputFileWoExt+"_prepro.txt";
        String namaFileOutputWoTs = inputFileWoExt+"_prepro_nots.txt";  //tanpa timestamp
        try {
            PrintWriter pw = new PrintWriter(namaFileOutput);
            PrintWriter pw2 = new PrintWriter(namaFileOutputWoTs);
            FileInputStream fstream = new FileInputStream(inputFile);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            String msg="";
            String ts="";
            int cc=0;
            //while (sc.hasNextLine()) {
            while ((strLine = br.readLine()) != null)   {
                cc++;
                ts = strLine.substring(0,20);
                //ambil msg, split dengan time
                msg = strLine.substring(22);
                System.out.println(cc);
                if (msg.contains("4sq.com")) {continue;}  //skip tweets yg mengandung foursquare
                msg = msg.toLowerCase();                  //casefolding
                msg = msg.replaceAll("#[\\w|:_]*", " ");  //buang #xxxx hashtag
                msg = msg.replaceAll("@[\\w|:_]*", " ");  //buang @xxxx
                msg = msg.replaceAll("http://[-A-Za-z0-9+&@#/%?=~_()|!:,.;]*[-A-Za-z0-9+&@#/%=~_()|]"," "); // buang url
                msg = msg.replaceAll("[^a-z0-9 ]"," "); // buang selain a z, 0-9

                if (msg.length()>10)                    //skip message yg <10 char
                {
                    pw.println(ts+"  "+msg);  //tulis ke file dengan timestamp
                    pw2.println(msg);  //tulis tanpa timestamp
               }
            } //while
            pw.close();
            pw2.close();
        } catch (Exception e) {
            //e.printStackTrace();
            log.severe(e.toString());
        }
    }

    public static void main(String[] Args) {
      GenericPrepro gp = new GenericPrepro();
      gp.removeDuplicateTweet("G:\\eksperimen\\corpus_tweet_opini\\corpus_mentah_gabungan_0_75000.txt");
      //gp.process("G:\\eksperimen\\corpus_tweet_opini\\corpus_1307_1600.txt");
      
      //GenericPrepro gp2 = new GenericPrepro();
      //gp2.process("G:\\eksperimen\\corpus_tweet_opini\\corpus_1_opini.txt");
      
      //gp.removeDuplicateTweet("G:\\eksperimen\\corpus_tweet_opini\\corpus_0_20000_sort.txt");
      //gp.removeDuplicateTweet("G:\\eksperimen\\corpus_tweet_opini\\corpus_0_20000_sort_nodup.txt");
      //testing    	
      //GenericPrepro gp = new GenericPrepro();
      //gp.process("e:\\tweetmining\\corpus_0_1000.txt");


      //untuk tweet lalinbdg
      //GenericPrepro gp = new GenericPrepro();
      //gp.convert("F:\\visualiasitweet\\tweets_lalinbdg.txt");
      //gp.removeDuplicateTweet("F:\\visualiasitweet\\tweets_lalinbdg_std.txt");
      //gp.process("F:\\visualiasitweet\\tweets_lalinbdg_std_nodup.txt");
      //gp.removeTweetWithoutKeyword("F:\\visualiasitweet\\tweets_lalinbdg_std_nodup_prepro.txt", "F:\\visualiasitweet\\keyword_macet.txt");
      //gp.removeTweetWithoutKeyword("F:\\visualiasitweet\\tweets_lalinbdg_std_nodup_prepro_withkeyword_nodup.txt", "F:\\visualiasitweet\\keyword_suryasumantri_satukata.txt");
      //sort manual dengan notepad++ lalu hilangkan duplikasi lagi
      //gp.removeDuplicateTweet("F:\\visualiasitweet\\tweets_lalinbdg_std_nodup_prepro_withkeyword.txt");
      //gp.removeTimeStamp("F:\\visualiasitweet\\tweets_lalinbdg_std_nodup_prepro_withkeyword_nodup_preproSyn.txt");
      
    }

}
