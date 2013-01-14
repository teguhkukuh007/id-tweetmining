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
import java.util.TreeMap;
import java.util.Scanner;
import java.util.Map;
import java.util.TreeSet;
/**
 *
 * @author Yudi Wibisono (yudi@upi.edu)
 */
public class UnigramFreq {

    public int thresholdFreq=0;
    public String fileTag="";
    public void process(String inputFile) {

        String inputFileWoExt = inputFile.substring(0, inputFile.lastIndexOf('.')); //without ext
        TreeMap<String,Integer> countWord  = new TreeMap<String,Integer>();  //terurut berd keyword
        TreeSet<String> countWord2  = new TreeSet<String>();  //terurut berd freq
        
        String namaFileOutput1 = inputFileWoExt+"_wordfreq_bykeyword"+fileTag+".txt";
        String namaFileOutput2 = inputFileWoExt+"_wordfreq_byfreq"+fileTag+".txt";
        Integer freq;
        String  kata;
        File f = new File(inputFile);
        try {
            Scanner sc = new Scanner(f);
            //hitung frekuensi
            while (sc.hasNext()) {
                kata = sc.next();
                freq = countWord.get(kata);  //ambil kata

                //jika kata itu tidak ada, isi dengan 1, jika ada increment
                countWord.put(kata, (freq == null) ? 1 : freq + 1);
            }
            sc.close();
            //simpan hasilnya ke file teks
            PrintWriter pw = new PrintWriter(namaFileOutput1);
            PrintWriter pw2 = new PrintWriter(namaFileOutput2);

            

            //loop untuk semua isi countWord, terurut berdasarkan keyword
            for (Map.Entry<String,Integer> entry : countWord.entrySet()) {
		kata = entry.getKey();
                freq = entry.getValue();
                if (freq > thresholdFreq ) {
                    pw.println(kata+"="+freq); //tulis ke file
                    String strFreq = String.format("%05d",freq);
                    countWord2.add(strFreq+"="+kata);
                }  
            }

            for (String freqKeyword:countWord2) {
                pw2.println(freqKeyword);  //tulis ke file
             }
            pw.close();
            pw2.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] Args) {
        UnigramFreq fc = new UnigramFreq();
        fc.fileTag = "_threshold10";
        fc.thresholdFreq = 10;

        //fc.process("F:\\visualiasitweet\\tweets_lalinbdg_std_nodup_prepro_withkeyword_nodup_preproSyn_notimestamp.txt");
        //fc.process("G:\\eksperimen\\corpus_tweet_opini\\corpus_0_20000_sort_nodup.txt");
        //fc.process("e:\\tweetmining\\corpus_0_10000_prepro_nots_preproSyn.txt");
        fc.process("G:\\eksperimen\\corpus_tweet_opini\\corpus_mentah_gabungan_0_75000_nodup.txt");
    }
}
