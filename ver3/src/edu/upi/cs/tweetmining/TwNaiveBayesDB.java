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
	private class ProbKelas {
		int idKelas;
		String namaKelas;
		int jumTweet;      //jumlah tweet dalam kelas tersebut
		double probKelas;
		double probDef;    //probabilitas default untuk kata yang freq=0
		HashMap<String,Integer> countWord;  //count word per class
		HashMap<String,Double> probWord = new HashMap<String,Double>();    //prob word per class
	}
	

	public void learn() {
		/*  Input:
		 *
		 *     table 
		 *     kelas (id,id_kelas,nama_kelas,desc)
		 * 
		 *     className dalam string, tapi yang disimpan di tabel.fieldClass adalah indexnya
		 *     contoh: array berisi {hujuan,cerah} maka berartti 
		 *     0: hujan, 1:cerah. Yang disimpan di tabel hanya 0 dan 1 
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
			Logger log = Logger.getLogger("naive bayes DB");

			//String[] className = new String[inputFile.length];
			
			
			System.out.println("Naive Bayes: Learning");
	        System.out.println("=================================");
	        
	        //ambil data, pindahkan ke memori
	        
	        
	        //String namaFileOutput = outModelFile;
	        
	        //untuk setiap file class
	        //  hitung freq kemunculan setiap kata
	        //  hitung jumlah total kata
	        //  hitung jumlah tweet
	        
	        //coba satu file dulua
	        String kata;
	        Integer freq;
	        
	        
	        
	       // int jumClass  = className.length;
	        
//	        int[] jumTweetPerClass = new int[inputFile.length];
	       
	        
	       
	        
	        int jumTweetTotal  = 0;
	        int jumWordTotal   = 0;  //jumlah distinct kata (total kosakata)
	        ArrayList<ProbKelas> alProbKelas = new ArrayList<ProbKelas>();
	        
	        String strSQLJumKelas        = "select count(*) from kelas";
	        String strSQLAmbilIdKelas    = "select id,id_kelas,nama_kelas from kelas";
	        String strSQLAmbilTw         = "select id_internal,text_prepro from tw_jadi where tw_jadi."+fieldClass+" = ?"; 	
	        
	        
	        Connection conn=null;       
	        PreparedStatement pTw = null;
	        PreparedStatement pJumKelas=null;
	        PreparedStatement pAmbilIdKelas=null;
	        PreparedStatement pAmbilTweet = null;
	        PreparedStatement pInsertProbKelas = null;
	        PreparedStatement pInsertProbKata = null;
	        
	        ResultSet rsJumKelas;
	        ResultSet rsAmbilIdKelas;
	        ResultSet rsTw;
	        Class.forName("com.mysql.jdbc.Driver");
	        String strCon = "jdbc:mysql://"+dbName+"?user="+userName+"&password="+password;
	       
	        
	        int jumKelas=-1;
	        try  {
	        	 //ambil id_class
	        	 conn = DriverManager.getConnection(strCon);
	        	 pJumKelas  =  conn.prepareStatement (strSQLJumKelas);
	        	 
	        	 rsJumKelas = pJumKelas.executeQuery();
	        	 if (rsJumKelas.next()) {
	        		 jumKelas = rsJumKelas.getInt(1);
	        		 System.out.println("Jumlah kelas:"+jumKelas);
	        	 } else  {
	        		 System.out.println("Tidak bisa mengakses tabel kelas, abort");
	        		 System.exit(1);
	        	 }
	        	
	        	 int[] jumTweetPerClass = new int[jumKelas];
	        	 
	        	 pAmbilIdKelas = conn.prepareStatement(strSQLAmbilIdKelas);
	        	 rsAmbilIdKelas = pAmbilIdKelas.executeQuery();
	        	 pAmbilTweet = conn.prepareStatement(strSQLAmbilTw);
	        	 
	        	 
	        	 while (rsAmbilIdKelas.next()) {  //loop untuk setiap kelas
	        		 ProbKelas pk = new ProbKelas();
	        		 HashMap<String,Integer> countWord  = new HashMap<String,Integer>();  
	    	         pk.countWord=countWord;
	        		 
	        		 int idKelas = rsAmbilIdKelas.getInt(2); 
	        		 String namaKelas = rsAmbilIdKelas.getString(3);
	        		 System.out.println("Memproses kelas:"+namaKelas);
	        		 pk.idKelas = idKelas;
	        		 pk.namaKelas = namaKelas; 
	        		 
	        		 //ambil tweet untuk kelas tsb
	        		 pAmbilTweet.setInt(1, idKelas);
	        		 rsTw = pAmbilTweet.executeQuery();
	        		 
	        		 int cc=0;
	        		 while (rsTw.next()) {  //untuk setiap tweet
	        			 cc++;
	        			 String tw = rsTw.getString(2);
	        			 Scanner sc = new Scanner(tw);
			             //hitung frekuensi kata dalam tweet               
			             while (sc.hasNext()) {
			                   kata = sc.next();
			                   freq = countWord.get(kata);  //ambil kata
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
		        		 kata = entry.getKey();
 		        		 freq = entry.getValue();
 		        		 prob = Math.log((double) (freq + 1) / (jumWord + jumWordTotal ));  //dalam logiritmk
 		        		 pk.probWord.put(kata, prob);
 		        		 //pw.println(kata+","+prob);  //tulis ke file
		        	}
	        	}
	        	
	        //tulis ke database
		    String strSQLinsertProbKelas =    "insert into modelnb_class_prob (id_kelas,nama_class,prob_class,jumkata_class,prob_freq_nol) values (?,?,?,?,?)";	
		    String strSQLinsertProbKata =     "insert into modelnb_prob_kata  (id_kelas,kata,prob) values (?,?,?)";
	        
		    pJumKelas  =  conn.prepareStatement (strSQLJumKelas);
		    pJumKelas  =  conn.prepareStatement (strSQLJumKelas);
	         	
	        
	        	
	        	
	        	 
	        	 
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
	                System.out.println("Connection rollback...");
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
	                pTw.close();
	                conn.setAutoCommit(true);
	                conn.close();
	            } catch (Exception e) {
	                logger.log(Level.SEVERE, null, e);
	                //isError = true;
	            }    
	        }
	        
	}     
	    
	
	
	
	
	public void classify(String inputFileModel, String inputFileProses, String dirOutput) {
	  /*
	   *    input:
	   *    	
	   *        - file yang akan diklasifiksikan dan sudah diprepro + synonim+stwopwords (inputfileproses)
	   *    	- file model hasil learning                          (inputfilemodel)
	   *    
	   *    output: file sebanyak jumlah class
	   * 
	   */
	   //contoh input: 
		//2                                                    <--- jumlah class 
		//G:\eksperimen\corpus_tweet_opini\siap_nonopini_1     <--- nama class 1
		//G:\eksperimen\corpus_tweet_opini\siap_opini_1        <--- nama class 2
		//-2.2782152230971129                                  <--- prob class 1 
		//-7.217847769028871                                   <----prob class 2
		//=====>jum_kata,498                                   <---- ambil jumlah kata untuk class 1
		//-7.60339933974067                                    <---- prob untuk kata yang freq-nya 0
		//ngk,-6.910252159180724                               <---  prob kata untuk class 1 (log)
		//chance,-6.910252159180724
		//..... <sampai 498>
		//=====>jum_kata,1009                                  <--- jum kata class 2
		//-7.60339933974067                                    <---- prob untuk kata yang freq-nya 0
		//bara,-7.1372784372603855							   <--- prob kata untuk class 2 (log) 
		//siap,-7.1372784372603855					
		//dst.
		
	    //load model, kedepan perlu dipikirkan masasalah scalability (memori terbatas)
		Logger log = Logger.getLogger("edu.cs.upi");
		try {
            FileInputStream fstream = new FileInputStream(inputFileModel);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            
            int jumClass=-1;
            
            //jumlah class
            if ((strLine = br.readLine()) != null) {
            	jumClass = Integer.parseInt(strLine);  
            	System.out.println("jum class="+strLine);
            }
            
            String[] namaClass = new String[jumClass]; 
            double[] probClass = new double[jumClass];
            double[] probClassDef = new double[jumClass];  //default prob untuk kelas dengan freq 0
            ArrayList<HashMap<String,Double>> wordProbinClass = new ArrayList<HashMap<String,Double>>();  //prob word per class
            
            //ambil nama class
            for (int i=0;i<jumClass;i++) {
            	if ((strLine = br.readLine()) != null) {
                	namaClass[i] = strLine.substring(strLine.lastIndexOf('\\')+1,strLine.length()); 
                	System.out.println(namaClass[i]);  //debug
                }
            }
            
            
            //ambil prob tiap class
            for (int i=0;i<jumClass;i++) {
            	if ((strLine = br.readLine()) != null) {
                	probClass[i] = Double.parseDouble(strLine);  
                	System.out.println(probClass[i]);  //debug
                }
            }
            
            String [] str;
            int jumKata=0;
            int posClass=0;
            while ((strLine = br.readLine()) != null)   {
               str = strLine.split(",");  //=====>jum_kata,498
               jumKata = Integer.parseInt(str[1]);
               System.out.println("Jumkata = "+jumKata); //debug
               
               if ((strLine = br.readLine()) != null) {   //ambil nilai prob default untuk kata dengan freq 0
            	   probClassDef[posClass] = Double.parseDouble(strLine);   
               }
               
               
               HashMap<String,Double> wp = new HashMap<String,Double>();
               wordProbinClass.add(wp);               
               for (int i=0;i<jumKata;i++) {
            	   //ambil prob kata
            	   if ((strLine = br.readLine()) != null) {
	            	   str = strLine.split(",");
	            	   wp.put(str[0], Double.parseDouble(str[1]));
	            	   System.out.println(str[0]+","+str[1]);
            	   }
               }
               posClass++;
            }
            br.close();
            in.close();
            fstream.close();
            
            //---------------------> prob sudah masuk, tinggal proses file input
            
            String s = inputFileProses.substring(0, inputFileProses.lastIndexOf('.')); //without ext
            String inputFileWoExt = s.substring(s.lastIndexOf('\\')+1,s.length());  //without dir
            
            //inisialiasasi file yang akan ditulis, ini juga berbahaya dari sisi skalabilitas 
            //karena ada batasan file yang dapat diopen 
            PrintWriter[] arrPw = new PrintWriter[jumClass]; 
            for (int i=0;i<jumClass;i++) {
            	PrintWriter pw = new PrintWriter(dirOutput+inputFileWoExt+"_classresult_"+namaClass[i]+".txt");
            	arrPw[i] = pw;
            }
            
            
            fstream = new FileInputStream(inputFileProses);
            in = new DataInputStream(fstream);
            br = new BufferedReader(new InputStreamReader(in));
            String kata;
            Double prob;
            HashMap<String,Double> probWord;
            System.out.println("klasifikasi dimulai");  //debug
            
            double maxProb = -Double.MAX_VALUE; 
            int maxClass   = -1; //kelas yang paling tepat dengan prob terbesar
            double totProb = 0;
            while ((strLine = br.readLine()) != null)   {
        		System.out.println("tweet:"+strLine);
            	maxProb = -Double.MAX_VALUE;
            	for (int i=0;i<jumClass;i++) {   //loop untuk semua kelas
            		System.out.println("kelas ke:"+i);
            		probWord = wordProbinClass.get(i);  //hashmap
            		Scanner sc = new Scanner(strLine);                
            		totProb = probClass[i];              //inisialisasi
            		while (sc.hasNext()) {              //loop untuk semua kata
	                   kata = sc.next();
	                   prob = probWord.get(kata);  //ambil probilitas kata
	                   if (prob!=null) {
	                	   System.out.println("kata:"+kata+"; Prob="+prob);  //debug
	                	   //totProb = totProb + prob;
	                   } else {  //kata tidak ada menggunakan nilai standar
	                	   System.out.println(" Menggunakan nilai def kata:"+kata+"; Prob="+probClassDef[i]);  //debug
	                	   prob = probClassDef[i];
	                   } 	
	                   totProb = totProb + prob;
	                }            		
            		sc.close();
            		
            		System.out.println("Total prob:"+totProb);
            		if (totProb>maxProb)  {   //ambil yang paling besar
            			System.out.println("Tukar dengan maxprob:"+maxProb);
            			maxProb = totProb;
            			maxClass = i;
            		}
	             }
            	 //masukan strLine ke file sesuai dengan maxClass
            	System.out.println("kelas yang dipilih:"+maxClass);
            	arrPw[maxClass].println(strLine);
            }  //while strLine = br.readLine
            
            //close file output
            for (int i=0;i<jumClass;i++) {
            	arrPw[i].close();
            }
            
            
        } catch (Exception e) {
            log.severe(e.toString());
        }
	}
    
        
	}
	
	public static void main(String[] args) {
			  NaiveBayesDB nb = new NaiveBayesDB();
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
