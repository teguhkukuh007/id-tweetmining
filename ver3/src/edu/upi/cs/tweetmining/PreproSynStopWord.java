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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.logging.Logger;

/**
 * 
 *  AKAN DIHAPUS DAN PINDAH KE PREPODB  JANGAN  DIEDIT LAGI!!
 *  AKAN DIHAPUS AKAN DIHAPUS AKAN DIHAPUS AKAN DIHAPUS AKAN DIHAPUS 
 *
 * @author Yudi Wibisono (yudi@upi.edu)
 * - Mengganti sinonim dengan pasangannya, misal  yg = yang
 * - Menghilangkan stopwords
 * - buang kata dengan satu karakter
 * 
 *  DIPINDAHKE KE PREPRO DB!
 * 
 */
public class PreproSynStopWord {

    private HashMap<String,String> hmSyn = new HashMap<String,String>();  //pasangan kata sinonim
    private ArrayList<String> alStopWords = new ArrayList<String>();      //kata stopwordss
    private static final Logger log = Logger.getLogger("edu.cs.upi.tweetmining");
    
    
    private void loadStopWords(String fileStopW) {

        try {
            FileInputStream fstream = new FileInputStream(fileStopW);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            int cc=0;
            while ((strLine = br.readLine()) != null)   {
                if (strLine.equals("")) {continue;}
                cc++;
                alStopWords.add(strLine);
            }
        } catch (Exception e) {
            log.severe(e.toString());
        }
    }

    private void loadSyn(String fileSyn) {
        try {
            FileInputStream fstream = new FileInputStream(fileSyn);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            int cc=0;
            String[] w;
            while ((strLine = br.readLine()) != null)   {
                if (strLine.equals("")) {continue;}
                cc++;
                w=strLine.split("=");
                hmSyn.put(w[0],w[1]);
            }
        } catch (Exception e) {
            log.severe(e.toString());
        }
    }
    
    
    public void splitIndosatTelkomsel(String inputFile) {
    //utility, split dokumen berdasarkan keyword 'telkomsel' dan 'indosat'
    //input: file berisi tweet (xxx) yang sudah diprepro (case folding) 
    //output: dua file xxx_indosat.txt dan xxx_telkomsel.txt
    //yap mungkin sebaiknya buat yang generik, tapi untuk sekarang belum butuh
    	
    	String inputFileWoExt = inputFile.substring(0, inputFile.lastIndexOf('.')); //without ext
        String namaFileOutputIndosat = inputFileWoExt+"_indosat.txt";
        String namaFileOutputTelkomsel = inputFileWoExt+"_telkomsel.txt";
        try {
            PrintWriter pwIndosat   = new PrintWriter(namaFileOutputIndosat);
            PrintWriter pwTelkomsel = new PrintWriter(namaFileOutputTelkomsel);
            
            FileInputStream fstream = new FileInputStream(inputFile);
            DataInputStream in      = new DataInputStream(fstream);
            BufferedReader br       = new BufferedReader(new InputStreamReader(in));
            String strLine;
            
            int cc=0;
            while ((strLine = br.readLine()) != null)   {
                cc++;
                if (strLine.contains("telkomsel"))  {              //masalah ada pada   	
                	pwTelkomsel.println(strLine);                	
                } else if (strLine.contains("indosat"))  {                	
                	pwIndosat.println(strLine);
                }
            } //while
            br.close();
            in.close();
            fstream.close();
            pwIndosat.close();
            pwTelkomsel.close();
        } catch (Exception e) {
            log.severe(e.toString());
            //e.printStackTrace();
        }
    	
    }

    public void process(String fileSyn,String fileStopWords,String inputFile) {
        //file input sebaiknya sudah di proses di genericprepro 
    	
    	//replace variasi kata menjadi satu kata
        loadSyn(fileSyn);
        loadStopWords(fileStopWords);
        String inputFileWoExt = inputFile.substring(0, inputFile.lastIndexOf('.')); //without ext
        String namaFileOutput = inputFileWoExt+"_preproSyn.txt";
        try {
            PrintWriter pw = new PrintWriter(namaFileOutput);
            FileInputStream fstream = new FileInputStream(inputFile);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            
            int cc=0;
            String w,w2;
            while ((strLine = br.readLine()) != null)   {
                cc++;
                System.out.println(cc);
                Scanner sc = new Scanner(strLine);
                StringBuilder sb = new StringBuilder();
                while (sc.hasNext()) {
                    w = sc.next();
                    if (w.length()==1) {                //hanya satu char? buang
                        continue;
                    }
                    w2 = hmSyn.get(w);
                    if (w2!=null) {                     //ganti dengan sinonimi
                        w = w2;
                    }
                    if (!alStopWords.contains(w)) {    //ada di stopwords? jangan dimasukkan
                        sb.append(w).append(" ");
                    }
                }               
                pw.println(sb.toString());
            } //while
            pw.close();
        } catch (Exception e) {
            log.severe(e.toString());
            //e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        //testing
        PreproSynStopWord ps = new PreproSynStopWord();
        
        ps.splitIndosatTelkomsel("G:\\eksperimen\\corpus_tweet_opini\\gabungan\\positif_siap_opini2.txt");
        /*String fileSyn= "G:\\eksperimen\\corpus_tweet_opini\\catatan_kata_sinonim.txt";
        String fileStopWords = "G:\\eksperimen\\corpus_tweet_opini\\catatan_stopwords.txt";
        ps.process(fileSyn,fileStopWords,"G:\\eksperimen\\corpus_tweet_opini\\corpus_testing_classification2.txt");
		*/
    	
        //PreproSynStopWord ps = new PreproSynStopWord();
        //String fileSyn= "F:\\visualiasitweet\\catatan_kata_sinonim_lalinbdg.txt";
        //String fileStopWords = "F:\\visualiasitweet\\catatan_stopwords_lalinbdg.txt";
        //ps.process(fileSyn,fileStopWords,"F:\\visualiasitweet\\tweets_lalinbdg_std_nodup_prepro_withkeyword_nodup.txt");
       
    }
}
