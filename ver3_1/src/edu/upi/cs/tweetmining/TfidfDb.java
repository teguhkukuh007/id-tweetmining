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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Comparator;


/**
 *
 * @author Yudi Wibisono (yudi@upi.edu)
 * Memberikan bobot kepada setiap term (rawTF*IDF) pada dokumen
 * 
 * input adalah tw_jadi yang sudah diprepro (gunakan ProsesTwMentahDB untuk mendapatkan tw_jadi dan selanjutnya PreproDB untuk memprepro tweet)
 * output: table tfidf (def table ada di bawah)

 * tambahan:
 *   - term yg muncul di dalam  < MINFREQ  tweet akan dibuang (sekalian saat hitung idf)
 *   
 *   
 */


public class TfidfDb {
	 private static final Logger logger = Logger.getLogger("TfidfDb");
     public static final int MINTWEET = 3;  //minimum kemunculam tweet yg mengandung term supaya dihitung
     public static final int MINFREQ  = 3;  //minimum kemunculam tweet yg mengandung term supaya dihitung
     
     public String dbName;
     public String userName;
     public String password;
     public String tableTfidf = "tf_idf";  //table output
     public String tableTwJadi="tw_jadi";    //table input
     
     
     //private ArrayList<String>  alExtStopWords = new ArrayList<String>();
     
     private class TermStatComparable implements Comparator<TermStat>{
		@Override
		public int compare(TermStat o1, TermStat o2) {
			return (o1.getAvg()>o2.getAvg() ? -1 : (o1.getAvg()==o2.getAvg() ? 0 : 1));
		}
     }

   


//     private void loadExtStopWords(String inputExtStopWords) {
//         Logger logger = Logger.getLogger("edu.cs.upi.TFIDF");
//         try {
//                FileInputStream fstream = new FileInputStream(inputExtStopWords);
//                DataInputStream in = new DataInputStream(fstream);
//                BufferedReader br = new BufferedReader(new InputStreamReader(in));
//                String strLine;
//                int cc=0;
//                while ((strLine = br.readLine()) != null)   {
//                   alExtStopWords.add(strLine);
//                }
//                br.close();
//                in.close();
//            }catch (Exception e) {
//                logger.severe(e.toString());
//            }
//     }
     
     public void clearTable() {
    	 //hapus semua isi tableTfidf
    	 //set 
    	 
    	 //idealnya nanti diganti dengan proses incremental (online), 
    	 //tapi untuk sekarang dihapus dulu lalu diisi dengan yang baru
    	 Connection conn=null;       
         PreparedStatement pDelete = null;
         try {
         	 Class.forName("com.mysql.jdbc.Driver");
             String strCon = "jdbc:mysql://"+dbName+"?user="+userName+"&password="+password;
             conn = DriverManager.getConnection(strCon);
             conn.setAutoCommit(false);
             String SQLdelete = "delete from "+tableTfidf;
             pDelete  =  conn.prepareStatement (SQLdelete);
             pDelete.executeUpdate();
             
             conn.commit();
         } catch (Exception e) {
         	 e.printStackTrace();
             logger.severe(e.toString()); 
             logger.severe(e.toString()); //ROLLBACK
        		if (conn != null) {
                try {
 					conn.rollback();
 				} catch (SQLException e1) {
 					logger.log(Level.SEVERE, null, e1);   
 				}
                System.out.println("Connection rollback...");
            }
        }   
        finally {
            try {
         	   pDelete.close();         	   
               conn.close();
            } catch (Exception e) {
                logger.log(Level.SEVERE, null, e);
            }    
        }
    	 
     }

     /**
      * Menghitung rawtfidf
	  *
      * 

      *
      *  output adalah tabel berisi pasangan kata dan bobot rawtfidfnya
      *  hitung term freq tf(i,j): frekuensi term i di tweet j / jumlah kata dalam tweet tsb
      *  hitung idf(i): log (jumlah semua tweet dalam corpus / jumlah tweet yg mengandung term i)
      *  hitung tf-idf(i,j) = tf(i,j) * idf (i)
      *  
      *  table untuk menampung output tfidif sudah dikosongkan  (TBD: perlu dikosongkan terlebih dulu??)
      *  field yang dipreoses adalah twjadi.text_prepro
      *
      */
 
    public void process(String filter) {
    //filter untuk mengambil data, harus diawali dengan 'and'.
    //contoh: "created_at_wib between "2013-02-9 13:00" and "2013-02-10 10:00" "
        logger.info("mulai");
        //loadExtStopWords(inputExtStopWords);
        Connection conn=null;       
        PreparedStatement pTw = null;
        PreparedStatement pInsertTfIdf = null;
        String kata;
        try {
        	Class.forName("com.mysql.jdbc.Driver");
            String strCon = "jdbc:mysql://"+dbName+"?user="+userName+"&password="+password;
            System.out.println(strCon);
            conn = DriverManager.getConnection(strCon);
            conn.setAutoCommit(false);
            

            int cc=0;

            HashMap<String,Integer> tweetsHaveTermCount  = new HashMap<String,Integer>();               //jumlah tweet yg mengandung sebuah term
            ArrayList<HashMap<String,Integer>> arrTermCount = new ArrayList<HashMap<String,Integer>>(); //freq kata untuk setiap tweet
            ArrayList<Long>  arrIdInternalTw = new ArrayList<Long>(); //untuk menyimpan id 
            
            Integer freq;
            
            String SQLambilTw = "select id_internal,text_prepro from "+tableTwJadi+ " where is_prepro=1 "+filter;
            
            System.out.println(SQLambilTw);
            
            pTw  =  conn.prepareStatement (SQLambilTw);
            pInsertTfIdf =
                    conn.prepareStatement("insert into "+ tableTfidf + " (id_internal_tw_jadi,tfidf_val)"
                                        + "values (?,?) ");
            
            
            ResultSet rsTw = pTw.executeQuery();
            while (rsTw.next())   {                           //loop untuk setiap tweet    
            	long idInternalTw = rsTw.getLong(1);
            	arrIdInternalTw.add(idInternalTw);
            	String tw = rsTw.getString(2);
            	HashMap<String,Integer> termCount  = new HashMap<String,Integer>(); //freq term dalam satu tweet
                cc++;
                System.out.println(cc);
                Scanner sc = new Scanner(tw);
                while (sc.hasNext()) {
                    kata = sc.next();
                    freq = termCount.get(kata);  //ambil kata
                    //jika kata itu tidak ada, isi dengan 1, jika ada increment
                    termCount.put(kata, (freq == null) ? 1 : freq + 1);
                }
                sc.close();  //satu baris selesai diproses (satu tweet)
                arrTermCount.add(termCount);  //tambahkan

               //termCount berisi kata dan freq di sebuah tweet
               //berd termCount hitung jumlah tweet yg mengandung sebuah term
                for (String term : termCount.keySet()) {
                    //jika kata itu tidak ada, isi dengan 1, jika ada increment
                    freq = tweetsHaveTermCount.get(term);  //ambil kata
                    tweetsHaveTermCount.put(term, (freq == null) ? 1 : freq + 1);
                }
            }  //while
            
            double numOfTweets = cc;
            
            // hitung idf(i) = log (NumofTw / countTwHasTerm(i))
            HashMap<String,Double> idf = new HashMap<String,Double>();
            double jumTweet=0;
            for (Map.Entry<String,Integer> entry : tweetsHaveTermCount.entrySet()) {
                //System.out.println(entry.getKey()+"="+entry.getValue());
                //modif, untuk term hanys satu kali muncul set idf dengan 0, sebagai mark agar term tersebut dihapus
                jumTweet = entry.getValue();
                String key = entry.getKey();
                
                
                //hanya proses yg minimal muncul di x tweet
                //System.out.println(key+"="+jumTweet);
                if (jumTweet>=MINTWEET) {
//                    if (alExtStopWords.contains(entry.getKey())) {  //ada di ext stopwords, skip
//                        idf.put(key, -1.0);
//                    } else {
                    idf.put(key, Math.log(numOfTweets/jumTweet));
//                    }
                } else {
                    idf.put(key, -1.0);
                }
            }

            //hitung tfidf, tf yg digunakan tidak dibagi dengan jumlah kata di dalam tweet karena diasumsikan relatif sama
            double tfidf;cc=0;
            
            //for (HashMap<String,Integer> hm : arrTermCount) {   //untuk semua tweets 
            for (int i=0;i<arrTermCount.size();i++) {
            	HashMap<String,Integer> hm = arrTermCount.get(i);
            	Long idInternalTw = arrIdInternalTw.get(i);
            	cc++;
                //System.out.println(cc+":");
                double idfVal;
                String key;
                StringBuilder sb = new StringBuilder();
                for (Map.Entry<String,Integer> entry : hm.entrySet()) {  //untuk term dalam satu tweet
                    key =entry.getKey();
                    idfVal = idf.get(key);
                    if (idfVal>=0) {   //kalau < 0 artinya diskip karena jumlah tweet yg mengandung term tersbut terlalu sedikit
                        tfidf  = entry.getValue() * idfVal ;     //rawtf * idf
                        sb.append(entry.getKey()+"="+tfidf+";");
                    } 
                }
                pInsertTfIdf.setLong(1, idInternalTw);            
                pInsertTfIdf.setString(2, sb.toString());  
                pInsertTfIdf.addBatch();
            }
            pInsertTfIdf.executeBatch();
            System.out.println("selesai");
        } catch (Exception e) {
        	e.printStackTrace();
            logger.severe(e.toString()); //ROLLBACK
       		if (conn != null) {
               try {
					conn.rollback();
				} catch (SQLException e1) {
					logger.log(Level.SEVERE, null, e1);   
				}
               System.out.println("Connection rollback...");
           }
       }   
       finally {
           try {
        	   pInsertTfIdf.close();
               pTw.close();
               conn.commit();
               conn.setAutoCommit(true);
               conn.close();
           } catch (Exception e) {
               logger.log(Level.SEVERE, null, e);
           }    
       }
     }
    
    public void stat(String inputTFIDFFile) {
        /*input: file tfidf corpus (hasil dari fungsi process dibawah. Contoh baris:
           di=2.1972245773362196;ayo=5.123963979403259;cinta=5.198497031265826;
        //output: rata-rata bobot tfidf untuk semua kata yg terurut dari besar ke kecil
        //bisa digunakan untuk menentukan kata yang akan masuk ke stopwords
        */
        String inputFileWoExt = inputTFIDFFile.substring(0, inputTFIDFFile.lastIndexOf('.')); //without ext
        String namaFileOutput = inputFileWoExt+"_tfidfStat.txt";
        try {
            PrintWriter pw = new PrintWriter(namaFileOutput);
            FileInputStream fstream = new FileInputStream(inputTFIDFFile);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            int cc=0;

            HashMap<String,TermStat> termMap = new HashMap<String,TermStat>();
            String[] str;
            TermStat ts;
            while ((strLine = br.readLine()) != null)   {                
                cc++;
                System.out.println(cc);
                //di=2.1972245773362196;ayo=5.123963979403259;cinta=5.198497031265826;
                Scanner sc = new Scanner(strLine);
                sc.useDelimiter(";");
         
                while (sc.hasNext()) {
                     String item = sc.next(); //pasangan term=val
                     str=item.split("=");
                     ts = termMap.get(str[0]);
                     double val = Double.parseDouble(str[1]);
                     if (ts==null) {
                         termMap.put(str[0], new TermStat(str[0],val));
                     } else
                     {
                         //ts.incFreq();
                         ts.addVal(val);
                     }
                }

            } //semua baris sudah dibaca
            br.close();
            in.close();
            ArrayList<TermStat> arrTS = new ArrayList<TermStat>();
            for (Map.Entry<String,TermStat> term : termMap.entrySet()) {                
                ts = term.getValue();
                if (ts.getFreq()<MINFREQ) {
                    continue;
                } //skip term yg hanya muncul xx kali
                ts.calcAvg();
                arrTS.add(ts);
            }
            Collections.sort(arrTS, new TermStatComparable());
            for (TermStat t: arrTS) {
                 pw.println(t.term+"="+t.getAvg()+"; freq="+t.getFreq());
            }
            pw.close();
        } catch (Exception e) {
            logger.severe(e.toString());
        }
     }

     public static void main(String[] Args) {
        //testing
        TfidfDb t = new TfidfDb();
        t.dbName="localhost/obama2";
        t.userName="yudi3";
        t.password="rahasia";
        t.tableTwJadi="tw_jadi";
        t.tableTfidf="tfidf_2000";
        t.process("");
        //t.process("g:\\eksperimen\\data_weka\\tw_obama.txt","g:\\eksperimen\\data_weka\\stopwords.txt");
        //t.process("e:\\tweetmining\\corpus_0_1000_prepro_nots_preproSyn.txt","e:\\tweetmining\\catatan_stopwords_weight.txt");
        //t.stat("e:\\tweetmining\\corpus_0_1000_prepro_nots_preproSyn_tfidf.txt");
       //  t.stat("g:\\eksperimen\\data_weka\\tw_obama_tfidf.txt");
     }
/*     

     drop table tfidf;
     CREATE TABLE IF NOT EXISTS `tfidf` (
       `id_internal` bigint(20) NOT NULL AUTO_INCREMENT,
       `id_internal_tw_jadi` bigint(20) NOT NULL,
       `tfidf_val` varchar(1000) NOT NULL,
       PRIMARY KEY (`id_internal`),
       KEY `tfidf_id_internal_tw_jadi` (`id_internal_tw_jadi`)
     ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
     
*/
     
}
