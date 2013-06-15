//antonim: wordnet
//kata negasi
//paper standfor: contradiction detection
//child 
//klasifier di root 
//klasifier di level child
//setuju atau tidak setuju -->cari  paper?
//kredilbitiy dan penjelasan satu paket


/*
 *  Copyright (C) 2012 yudi wibisono (yudi@upi.edu)
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Naive Bayes classification
 * @author Yudi Wibisono (yudi@upi.edu)
 * 
 * 
 * asumsi data sudah ada di tabel, sudah diprepro. Untuk crawl data gunakan TwCrawler --> ProsesTwMentah --> PreproDB --> TfIdfDB
 * is_duplicate=1 tidak akan diproses
 *  
 * output adalah model 
 * 
 */

public class TwNaiveBayesDB {
	
	public String dbName;           //  format: localhost/mydbname
	public String userName;
	public String password;
	public String fieldClass;      // field yang berisi id kelas yang diisi secara manual (untuk proses learning)
								   // id kelas mengikuti tabel 	
	
	private final  Logger logger = Logger.getLogger("naive bayes DB");
	
	//output dari method learn  TBD
	private  class HasilLearn {
		double akurasi;
		//nanti tambah confusion matrix
	}
	
	//prob kelas hasil learning
	public class ProbKelas {
		int idKelas;
		String namaKelas;
		int jumTweet;      //jumlah tweet dalam kelas tersebut
		double probKelas;
		double probDef;    //probabilitas default untuk kata yang freq=0
		HashMap<String,Integer> countWord;  //count word per class
		HashMap<String,Double> probWord = new HashMap<String,Double>();    //prob word per class
		
		public void print() {
			//untuk kepentingan debug
			System.out.println("id kelas="+idKelas);
			System.out.println("nama kelas="+namaKelas);
			System.out.println("jumtweet="+jumTweet);
			System.out.println("probKelas="+probKelas);
			System.out.println("probDef="+probDef);
			
			//print count word
			System.out.println("count word");
			for (Map.Entry<String,Integer> entry : countWord.entrySet())  {
				 String kata = entry.getKey();
        		 Integer freq = entry.getValue();
        		 System.out.println(kata+":"+freq);
			}
			
			//print prob word
			System.out.println("prob word");
			for (Map.Entry<String,Double> entry : probWord.entrySet())  {
				 String kata = entry.getKey();
        		 Double prob = entry.getValue();
        		 System.out.println(kata+":"+prob);
			}
			
			
		}
	}
	
	
	
	private ArrayList<ProbKelas> learn(String tambahanFilter) {
		//tambahan filter berguna misalnya untuk memproses 10 fold cross validation
		//contoh --> learn("and partisi<>1")   maka hanya partisi<>1 yang diproses utk dijadikan model
		//harus diawali dengan "and"
		
		//input tabel kelas (id_internal,id_kelas,nama_kelas,desc)
		/*
		
		CREATE TABLE IF NOT EXISTS `kelas` (
				  `id_internal` bigint(20) NOT NULL AUTO_INCREMENT,
				  `id_kelas`    bigint(20) NOT NULL,
				  `nama_kelas` varchar(50) NOT NULL,
				  `desc` varchar(75) NOT NULL,
				  PRIMARY KEY (`id_internal`),
				  KEY `id_kelas` (`id_kelas`)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8;
		*/
		
		Logger log = Logger.getLogger("naive bayes DB learning ");

		//String[] className = new String[inputFile.length];
		
		
//		System.out.println("Naive Bayes: Learning");
//        System.out.println("=================================");
        
        //ambil data, pindahkan ke memori
        
        
        //  hitung freq kemunculan setiap kata
        //  hitung jumlah total kata
        //  hitung jumlah tweet
        
        // int jumClass  = className.length;
        
        //	int[] jumTweetPerClass = new int[inputFile.length];
       
        
       
        
        int jumTweetTotal  = 0;
        int jumWordTotal   = 0;  //jumlah distinct kata (total kosakata)
        ArrayList<ProbKelas> alProbKelas = new ArrayList<ProbKelas>();
        
        String strSQLJumKelas        = "select count(*) from kelas";
        String strSQLAmbilIdKelas    = "select id_internal,id_kelas,nama_kelas from kelas";
        String strSQLAmbilTw         = "select id_internal,text_prepro from tw_jadi where "+fieldClass+" = ? "+tambahanFilter; 	
        System.out.println(strSQLAmbilTw);
        
        Connection conn=null;       
       
        PreparedStatement pJumKelas=null;
        PreparedStatement pAmbilIdKelas=null;
        PreparedStatement pAmbilTweet = null;
        
        ResultSet rsJumKelas=null;
        ResultSet rsAmbilIdKelas=null;
        ResultSet rsTw=null;
       
       
        
        int jumKelas=-1;
        try  {
        	 Class.forName("com.mysql.jdbc.Driver");
             String strCon = "jdbc:mysql://"+dbName+"?user="+userName+"&password="+password;
        	 //ambil id_class
        	 conn = DriverManager.getConnection(strCon);
        	 pJumKelas  =  conn.prepareStatement (strSQLJumKelas);
        	 rsJumKelas = pJumKelas.executeQuery();
        	 if (rsJumKelas.next()) {
        		 jumKelas = rsJumKelas.getInt(1);
        		 //System.out.println("Jumlah kelas:"+jumKelas);
        	 } else  {
        		 System.out.println("Tidak bisa mengakses tabel kelas, abort");
        		 System.exit(1);
        	 }
        	
        	 //int[] jumTweetPerClass = new int[jumKelas];
        	 
        	 pAmbilIdKelas = conn.prepareStatement(strSQLAmbilIdKelas);
        	 rsAmbilIdKelas = pAmbilIdKelas.executeQuery();
        	 pAmbilTweet = conn.prepareStatement(strSQLAmbilTw);
        	 
//        	 String kata;
// 	         Integer freq;
 	        
        	 while (rsAmbilIdKelas.next()) {  //loop untuk setiap kelas
        		 ProbKelas pk = new ProbKelas();
        		 HashMap<String,Integer> countWord  = new HashMap<String,Integer>();  
    	         pk.countWord=countWord;
        		 
        		 int idKelas = rsAmbilIdKelas.getInt(2); 
        		 String namaKelas = rsAmbilIdKelas.getString(3);
        		 //System.out.println("Memproses kelas:"+namaKelas);
        		 pk.idKelas = idKelas;
        		 pk.namaKelas = namaKelas; 
        		 
//        		 System.out.println("id kelas"+idKelas);
//        		 System.out.println("Nama kelas:"+namaKelas);
        		 //ambil tweet untuk kelas tsb
        		 pAmbilTweet.setInt(1, idKelas);
        		 rsTw = pAmbilTweet.executeQuery();
        		 
        		 int cc=0;
        		 while (rsTw.next()) {  //untuk setiap tweet
        			 cc++;
        			 String tw = rsTw.getString(2);
        			 //System.out.println("Proses:"+tw);
        			 Scanner sc = new Scanner(tw);
		             //hitung frekuensi kata dalam tweet               
		             while (sc.hasNext()) {
		                   String kata = sc.next();
		                   Integer freq = countWord.get(kata);  //ambil kata
		                   countWord.put(kata, (freq == null) ? 1 : freq + 1);      //jika kata itu tidak ada, isi dengan 1, jika ada increment
		             }
		             sc.close();
        		 }
        		 pk.jumTweet = cc;
		         jumTweetTotal =  jumTweetTotal  + cc;
		         jumWordTotal =   jumWordTotal   + countWord.size();
		         alProbKelas.add(pk);
        	 } //loop untuk setiap kelas
        	 
        	//hitung probablitas
 	        //hitung p(vj) = jumlah tweet kelas j / jumlah total tweet        
 	        //hitung p(wk | vj ) = (jumlah kata wk dalam kelas vj + 1 )  / jumlah total kata dalam kelas vj + |kosa kata|
        	
        	HashMap<String,Integer> countWord;
	 	    double prob;
	 	    int jumWord;
        	 
        	for (ProbKelas pk:alProbKelas) {
        		pk.probKelas = Math.log( (double) pk.jumTweet / jumTweetTotal);
        		countWord = pk.countWord;
	        	jumWord = countWord.size();  //jumlah kata di dalam kelas
	        	pk.probDef = Math.log((double) (1) / (jumWord + jumWordTotal)); //default value kata tanpa freq
	        	
	        	//prob untuk setiap kata dalam kelas tsb
	        	for (Map.Entry<String,Integer> entry : countWord.entrySet()) {
	        		 String kata = entry.getKey();
		        		 Integer freq = entry.getValue();
		        		 prob = Math.log((double) (freq + 1) / (jumWord + jumWordTotal ));  //dalam logiritmk
		        		 pk.probWord.put(kata, prob);
	        	}
        	}
        	
        }
        catch (Exception e)
        {
        		//ROLLBACK
     	    	logger.log(Level.SEVERE, null, e);
        		if (conn != null) {
                try {
 					conn.rollback();
 				} catch (SQLException e1) {
 					logger.log(Level.SEVERE, null, e1);   
 				}
                System.out.println("Fatal Error, rollback...");
                //isError = true;
            }
        }   
        finally {       
        	try {   		
    	        pAmbilIdKelas.close();
    	        pAmbilTweet.close();
    	        rsJumKelas.close();
    	        rsAmbilIdKelas.close();
    	        rsTw.close();
            	pJumKelas.close();
                conn.close();
                
            } catch (Exception e) {
                logger.log(Level.SEVERE, null, e);
                //isError = true;
            }    
        }
        return alProbKelas;
	}	
	

	public void learnToDB() {
		/*  Input:
		 *
		 *   
		 *     
		 *     
		 *     Field yang digunakan:
		 *     - fieldClass adalah class (integer) dari record
		 * 	   - text_prepro berisi tweet
		 * 
		 *     Output ke DB
		 *     	
		 *     modelnb_prob_kelas     (id,id_kelas,prob_class,jumkata_class,prob_freq_nol)  --> nama kelas dan prob kelas
		 *     modelnb_prob_kata      (id,id_kelas,kata,prob) 
		 *     
		 *       
		 */
		    
		
			Logger log = Logger.getLogger("naive bayes DB learning DB");

			//String[] className = new String[inputFile.length];
			
			
			System.out.println("Naive Bayes: Learning & tulis ke DB");
	        System.out.println("=================================");
	        
	        
	       
	        
	        ArrayList<ProbKelas> alProbKelas = learn("");
	        
	        
	        Connection conn=null;       
	        PreparedStatement pInsertProbKelas = null;
	        PreparedStatement pInsertProbKata = null;
	       
	        
	       
	        String strCon = "jdbc:mysql://"+dbName+"?user="+userName+"&password="+password;
	        try  {
	        	 //ambil id_class
	        	 Class.forName("com.mysql.jdbc.Driver");
	        	 conn = DriverManager.getConnection(strCon);
	        	 conn.setAutoCommit(false);
	        	 
	        	
	        	 //tulis ke database
	        	 String strSQLinsertProbKelas =   "insert into modelnb_class_prob (id_kelas,nama_class,prob_class,jumkata_class,prob_freq_nol) values (?,?,?,?,?)";	
	        	 String strSQLinsertProbKata  =   "insert into modelnb_prob_kata  (id_kelas,kata,prob) values (?,?,?)";
	        
	        	 pInsertProbKelas  =  conn.prepareStatement (strSQLinsertProbKelas);
	        	 pInsertProbKata   =  conn.prepareStatement (strSQLinsertProbKata);
	        	 
				 for (ProbKelas pk: alProbKelas) {
					    pInsertProbKelas.setInt(1,pk.idKelas);
					    pInsertProbKelas.setString(2,pk.namaKelas);
					    pInsertProbKelas.setDouble(3,pk.probKelas);
					    pInsertProbKelas.setInt(4,pk.countWord.size());
					    pInsertProbKelas.setDouble(5,pk.probDef);
					    pInsertProbKelas.executeUpdate();
					    for (Map.Entry<String,Double> entry : pk.probWord.entrySet()) {
					    	pInsertProbKata.setInt(1,pk.idKelas);
					    	String kata = entry.getKey();
			        		Double probKata = entry.getValue();
					    	pInsertProbKata.setString(2,kata);
					    	pInsertProbKata.setDouble(3,probKata);
					    	pInsertProbKata.addBatch();
					    }
				  }
				  pInsertProbKata.executeBatch();
	        }
	        catch (Exception e)
	        {
	        		//ROLLBACK
	     	    	logger.log(Level.SEVERE, null, e);
	        		if (conn != null) {
	                try {
	 					conn.rollback();
	 				} catch (SQLException e1) {
	 					logger.log(Level.SEVERE, null, e1);   
	 				}
	                System.out.println("Fatal Error, rollback...");
	                //isError = true;
	            }
	        }   
	        finally {
	            try {
	                pInsertProbKata.close();
	                pInsertProbKelas.close();
	                conn.commit();
	                conn.setAutoCommit(true);
	                conn.close();
	            } catch (Exception e) {
	                logger.log(Level.SEVERE, null, e);
	                //isError = true;
	            }    
	        }
	}
	
	
	public ProbKelas classifyMem(ArrayList<ProbKelas> model,String tweet) {   //model diambil dari memori, cocok untuk x fold cross
	    //System.out.println("klasifikasi tweet:"+tweet);
	    double maxProb = -Double.MAX_VALUE;
	    HashMap<String,Double> probWord;
	    double totProb = 0;
	    ProbKelas maxClass=null;
	    
	    //cari kelas dengan prob maksimal
	    for (ProbKelas pk: model) {
	       //System.out.println("kelas:"+pk.namaKelas);
	       probWord =  pk.probWord; //  wordProbinClass.get(i);  //hashmap
	       Scanner sc = new Scanner(tweet);                
	       totProb = pk.probKelas;  //probClass[i];              //inisialisasi
	       while (sc.hasNext()) {               //loop untuk semua kata
	           String kata = sc.next();
	           Double prob = probWord.get(kata);  //ambil probilitas kata
	           if (prob!=null) {
	               //System.out.println("kata:"+kata+"; Prob="+prob);  //debug
	           } else {  //kata tidak ada, menggunakan nilai standar
	               //System.out.println(" Menggunakan nilai def kata:"+kata+"; Prob="+pk.probDef);  //debug
	               prob = pk.probDef;
	           } 	
	           totProb = totProb + prob;
	        } //while sc            		
	        sc.close();
	        //System.out.println("Total prob:"+totProb);
	        if (totProb>maxProb)  {   //ambil yang paling besar
	        	//System.out.println("Tukar dengan maxprob:"+maxProb);
	        	maxProb = totProb;
	        	maxClass = pk;
	        }
	    }//for
	    return maxClass;
	}
	
	public void xFoldCrossVal() {
	/*
	 *  tabel tw_jadi punya field partisi (int, indexed)
	 *  field class sudah terdefinisi
	 *  tabel partisi tsb 	
	 *  
	 *  ALTER TABLE `tw_jadi` ADD COLUMN `partisi` INT NULL DEFAULT '0' AFTER `sanggahan_dukungan`;
	 *  ALTER TABLE `tw_jadi` ADD INDEX `partisi` (`partisi`);
	 */
		
	   int jumFold = 10;	
		
		//cek validitas model
		
		//partisi
		//hitung jumlah record total
	   //bagi dengan jumFold
	   //loop untuk semua data, secara random
	   //isi field partisi dengan angka yg akan menjadi group (menggunakan mod)
	   
	   //bagian where-nya nanti diganti
	   String strSQLJumTw         = "select count(*)  from tw_jadi where " +fieldClass+" is not null";
	   String strSQLtw            = "select id_internal from tw_jadi where " +fieldClass+" is not null order by rand()";
	   String strSQLupdatePartisi = "update tw_jadi set partisi = ? where id_internal= ?";
	   String strSQLtwPartisi     = "select id_internal,text,text_prepro,"+fieldClass+" from tw_jadi where partisi = ? and " +fieldClass+" is not null";
	   
	   Connection conn=null;       
       PreparedStatement pJumTw = null;
       PreparedStatement pTw = null;
       PreparedStatement pUpdatePartisi = null;
       PreparedStatement pTwPartisi = null;
       ResultSet rsJumTw = null;
       ResultSet rsTw = null;
       ResultSet rsTwPartisi = null;
       
      
       try  {
       	 //ambil id_class
    	 Class.forName("com.mysql.jdbc.Driver");
         String strCon = "jdbc:mysql://"+dbName+"?user="+userName+"&password="+password;
       	 conn = DriverManager.getConnection(strCon);
       	 conn.setAutoCommit(false);
       	 pTwPartisi = conn.prepareStatement(strSQLtwPartisi);
       	 pJumTw  =  conn.prepareStatement (strSQLJumTw);
       	 rsJumTw = pJumTw.executeQuery();
       	 int jumTw;
       	 if (rsJumTw.next()) {
       		 jumTw = rsJumTw.getInt(1);
       		 System.out.println("Jumlah tweet"+jumTw);
       	 } else {
       		 System.out.println("tidak ada ada atau error mengakses db, abort");
       		 System.exit(1);
       	 }  
       	 
       	 //ambil data tweet
       	 pTw = conn.prepareStatement(strSQLtw);
       	 pUpdatePartisi = conn.prepareStatement(strSQLupdatePartisi);
       	 rsTw = pTw.executeQuery();
       	 int cc = 0;
       	 while (rsTw.next()) {
       		 int id = rsTw.getInt(1);
       		 int partisi = (int) (cc % jumFold+1);
//       		 System.out.println("id="+id);
//       		 System.out.println("partisi:"+partisi);
       		 pUpdatePartisi.setInt(1, partisi);
       		 pUpdatePartisi.setInt(2, id);
       		 pUpdatePartisi.addBatch();
       		 cc++;
       	 }	 
       	 pUpdatePartisi.executeBatch();
       	 
       	 ArrayList<ProbKelas> model;
       	 double totAkurasi=0;
       	 //loop untuk setiap partisi
       	 for (int i=1;i<=jumFold;i++)  {  //partisi mulai dari 1       		 
       		 //learn, lalu klasifikasikan
       		 System.out.println("Proses partisi ke:"+i);
       		 System.out.println("learn");
       		 model = learn("and partisi<>"+i);   //partisi i menjadi testing, partisi lainnya menjadi training  jd tidak masuk     		 
//       		 for (ProbKelas pk: model) {
//       			 pk.print();
//       		 }
       		 
       		 
       		 //loop untuk semua tweet dalam partisi i
       		 pTwPartisi.setInt(1,i);            
       		 rsTwPartisi = pTwPartisi.executeQuery();
       		 ProbKelas pk;
       		 int jumCocok = 0;
       		 int jumSalah = 0;
       		 while (rsTwPartisi.next()) {
       			 int id = rsTwPartisi.getInt(1);
       			 String twOrig = rsTwPartisi.getString(2); //belum diprepro
       			 String tw = rsTwPartisi.getString(3);
       			 int kelas = rsTwPartisi.getInt(4);
       			 pk = classifyMem(model,tw);
//       			 System.out.println("Kelas yang benar:"+kelas);
//       			 System.out.println("Kelas prediksi:"  +pk.idKelas);
       			 if (pk.idKelas == kelas) {
       				// System.out.println("cocok:");
       				 jumCocok++;
       			 } else {
       				// System.out.println("tdk cocok:");
       				 
       				 System.out.println("tweet:"+twOrig);
       				 System.out.println("id:"+id);
       				 System.out.println("Kelas yang benar:"+kelas);
       				 System.out.println("Kelas prediksi:"  +pk.idKelas);
       				 System.out.println();
      				 jumSalah++;
       			 }
       		 }
       		 System.out.println("Jum cocok:"+jumCocok);
       		 System.out.println("Jum salah:"+jumSalah);
       		 double akurasi = (double) jumCocok / (jumCocok+jumSalah);
       		 totAkurasi = totAkurasi + akurasi;
       		 System.out.println( "Akurasi:"+akurasi);
       		 //System.exit(1); //debug dulu
       	 }
       	 double avgAkurasi = totAkurasi / jumFold;
       	 System.out.println();
       	 System.out.println("rata2 akurasi="+avgAkurasi);
       
       }  catch (Exception e)
       {
   		//ROLLBACK
	       logger.log(Level.SEVERE, null, e);
   		   if (conn != null) {
           try {
				conn.rollback();
			} catch (SQLException e1) {
				logger.log(Level.SEVERE, null, e1);   
			}
           System.out.println("Fatal Error, rollback...");
           //isError = true;
       }
   }   
   finally {
       try {
    	   pJumTw.close();
           pTw.close();
           pUpdatePartisi.close();
           rsTw.close();
           rsJumTw.close();
       	   conn.commit();
       	   conn.setAutoCommit(true);
           conn.close();
       } catch (Exception e) {
           logger.log(Level.SEVERE, null, e);
           //isError = true;
       }    
   }
	}	
	
	
	
	
	
	public static void main(String[] args) {
			  TwNaiveBayesDB nb = new TwNaiveBayesDB();
//		    	ptm.dbName = "";
//		    	ptm.userName = "yudi3";
//		    	ptm.password = "rahasia";
			  
			  nb.dbName =    "localhost/obama2";
			  nb.userName =  "yudi3"; 
			  nb.password =  "rahasia";
			  nb.fieldClass=  "sanggahan_dukungan";
			  nb.xFoldCrossVal();
			  
			  
			 /*
			  String[] input   = new String[2]; 
			  input[0]         = "G:\\eksperimen\\corpus_tweet_opini\\positif_siap_opini2.txt";
			  input[1]         = "G:\\eksperimen\\corpus_tweet_opini\\negatif_siap_opini2.txt";
			  String output    = "G:\\eksperimen\\corpus_tweet_opini\\model_pos_neg_opini_2.txt";
			  nb.learn(input, output); 
			 */
			  //nb.classify("G:\\eksperimen\\corpus_tweet_opini\\model_opini_2.txt", "G:\\eksperimen\\corpus_tweet_opini\\corpus_testing_classification2_preproSyn.txt","G:\\eksperimen\\corpus_tweet_opini\\");
			  
			  //nb.classify("G:\\eksperimen\\corpus_tweet_opini\\model_pos_neg_opini_2.txt", "G:\\eksperimen\\corpus_tweet_opini\\corpus_testing_posneg.txt","G:\\eksperimen\\corpus_tweet_opini\\");	
	}

}
