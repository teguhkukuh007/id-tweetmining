/*
 *  Copyright (C) 2012 yudi wibisono (yudi1975@gmail.com/cs.upi.edu)
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
import java.util.TreeMap;
import java.util.Scanner;
import java.util.Map;
import java.util.TreeSet;
import java.util.logging.Logger;

/**
 *
 * @author Yudi Wibisono (yudi@upi.edu)
 */
public class BigramFreq {

    public void process(String inputFile) {
        //input file tidak mengandung timestamp (bisa dibuang di method removeTimeStamp di genericPrepro)

        Logger log = Logger.getLogger("edu.cs.upi.tweetmining");

        String inputFileWoExt = inputFile.substring(0, inputFile.lastIndexOf('.')); //without ext
        TreeMap<String,Integer> countWord  = new TreeMap<String,Integer>();  //terurut berd keyword
        TreeSet<String> countWord2  = new TreeSet<String>();  //terurut berd freq

        String namaFileOutput1 = inputFileWoExt+"_bigramfreq_bykeyword.txt";
        String namaFileOutput2 = inputFileWoExt+"_bigramfreq_byfreq.txt";
        Integer freq;
        String  kata1;
        String  kata2="";
        //File f = new File(inputFile);
        try {
             FileInputStream fstream = new FileInputStream(inputFile);
             DataInputStream in = new DataInputStream(fstream);
             BufferedReader br = new BufferedReader(new InputStreamReader(in));
             String strLine;
             int cc=0;
             while ((strLine = br.readLine()) != null)   {
                cc++;
                System.out.println(cc);
                Scanner sc = new Scanner(strLine);
                //hitung frekuensi
                //saya makan nasi  --> saya makan --> makan nasi
                if (sc.hasNext()) {
                   kata2 = sc.next();
                }
                while (sc.hasNext()) {
                    kata1 = kata2;
                    if (sc.hasNext()) {
                        kata2 = sc.next();
                    } else {continue;}
                    freq = countWord.get(kata1+" "+kata2);  //ambil kata
                    //jika kata itu tidak ada, isi dengan 1, jika ada increment
                    countWord.put(kata1+" "+kata2, (freq == null) ? 1 : freq + 1);
                }
                sc.close();
            }
            //simpan hasilnya ke file teks
            PrintWriter pw = new PrintWriter(namaFileOutput1);
            PrintWriter pw2 = new PrintWriter(namaFileOutput2);


            String kata;
            //loop untuk semua isi countWord, terurut berdasarkan keyword
            for (Map.Entry<String,Integer> entry : countWord.entrySet()) {
            	kata = entry.getKey();
                freq = entry.getValue();
                pw.println(kata+"="+freq);  //tulis ke file

                String strFreq = String.format("%05d",freq);
                countWord2.add(strFreq+"="+kata);

            }

            for (String freqKeyword:countWord2) {
                pw2.println(freqKeyword);  //tulis ke file
             }


            pw.close();
            pw2.close();
        } catch (Exception e) {
            //e.printStackTrace();
            log.severe(e.toString());
        }
    }

    public static void main(String[] Args) {
        //BigramFreq bf = new BigramFreq();
        //bf.process("e:\\tweetmining\\corpus_0_10000_prepro_nots_preproSyn.txt");

        BigramFreq bf = new BigramFreq();
        bf.process("F:\\visualiasitweet\\tweets_lalinbdg_std_nodup_prepro_withkeyword_nodup_preproSyn_notimestamp.txt");
    }
}
