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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Logger;

/**
 * Naive Bayes classification
 * @author Yudi Wibisono (yudi@upi.edu)
 * 
 * 
 * asumsi data sudah ada di tabel, sudah diprepro. Untuk crawl data gunakan TwCrawler, lalu untuk membersihkan gunakan PreproDB
 * is_duplicate=1 tidak akan diproses
 *  
 * output adalah file model (model sepertinya lebih efisien dengan file dibandingkan dengan DB)
 * 
 */

public class TwNaiveBayesDB {
	
	//output dari method learn  TBD
	public class HasilLearn {
		double akurasi;
		//nanti tambah confusion matrix
	}
	
	
	//TBD, selain menghasilkan 
	public void learn(String[] className, String fieldClass, String outModelFile) {
		/*  Input:
		 *     className dalam string, tapi yang disimpan di tabel.fieldClass adalah indexnya
		 *     contoh: array berisi {hujuan,cerah} maka berartti 
		 *     0: hujan, 1:cerah. Yang disimpan di tabel hanya 0 dan 1 
		 *     
		 *     Field yang digunakan:
		 *     - fieldClass adalah class (integer) dari record
		 * 	   - text_prepro berisi tweet
		 * 

		 *  
		 *  Output (classifier model):
		 *    satu file yang berisi
		 *    probabilitas P(class[i])
		 *    probalitas P(kata[j] | kelas[i])
		 *    isinya:
		 *       jumlah class
		 *       nama class
		 *       ========
		 *       P(class[0]) 
		 *       P(class[1])
		 *       ====>jumkata, jumlahKata(class[0])
		 *       NILAI_DEFAULT_UNTUK_KATA_DENGAN_FREQ_NOL  <--- untuk kelas 0
		 *       kata[0],prob(class[0],kata[0])     <-- untuk klas 0
		 *       kata[1],prob(class[0],kata[1])
		 *       ====>jumkata, jumlahKata(class[1])
		 *       NILAI_DEFAULT_UNTUK_KATA_DENGAN_FREQ_NOL  <--- untuk kelas 1
		 *       kata[0],prob(class[1],kata[0])            <-- untuk kelas 1
		 *       kata[1],prob(class[1],kata[0])
		 */
			Logger log = Logger.getLogger("naive bayes DB");

			//String[] className = new String[inputFile.length];
			
			
			System.out.println("Naive Bayes: Learning");
	        System.out.println("=================================");
	        
	        //ambil data, pindahkan ke memori
	        
	        
	        String namaFileOutput = outModelFile;
	        
	        //untuk setiap file class
	        //  hitung freq kemunculan setiap kata
	        //  hitung jumlah total kata
	        //  hitung jumlah tweet
	        
	        //coba satu file dulua
	        String kata;
	        Integer freq;
	        
	        int jumClass = className.length;
	        
//	        int[] jumTweetPerClass = new int[inputFile.length];
	        int[] jumTweetPerClass = new int[jumClass];
	        
	        ArrayList<HashMap<String,Integer>> wordCountinClass = new ArrayList<HashMap<String,Integer>>();  //count word per class
	        
	        int jumTweetTotal  = 0;
	        int jumWordTotal   = 0;  //jumlah distinct kata (total kosakata)
	        
	        for (int k=0;k<jumClass;k++) {
	        	HashMap<String,Integer> countWord  = new HashMap<String,Integer>();  
	        	wordCountinClass.add(countWord);
	        	try {
//		            FileInputStream fstream = new FileInputStream(inputFile[k]);
//		            DataInputStream in = new DataInputStream(fstream);
//		            BufferedReader br = new BufferedReader(new InputStreamReader(in));
//		            String strLine;
		            int cc=0;  //jumlah tweet dalam file
//		            while ((strLine = br.readLine()) != null)   {
			        while ((strLine = br.readLine()) != null)   {
		               cc++;	               
		               System.out.println(cc);
		               Scanner sc = new Scanner(strLine);
		               //hitung frekuensi kata dalam tweet               
		               while (sc.hasNext()) {
		                   kata = sc.next();
		                   freq = countWord.get(kata);  //ambil kata
		                   //jika kata itu tidak ada, isi dengan 1, jika ada increment
		                   countWord.put(kata, (freq == null) ? 1 : freq + 1);
		               }
		               sc.close();
		            }
		            br.close();
		            in.close();
		            fstream.close();
		            //cc adalah jumlah tweet untuk kelas 
		            jumTweetPerClass[k] = cc;
		            jumTweetTotal =  jumTweetTotal + cc;
		            jumWordTotal =   jumWordTotal   + countWord.size();
		        } catch (Exception e) {
		            log.severe(e.toString());
		        }
	        }
	        
	        //hitung probablitas
	        //hitung p(vj) = jumlah tweet kelas j / jumlah total tweet        
	        //hitung p(wk | vj ) = (jumlah kata wk dalam kelas vj + 1 )  / jumlah total kata dalam kelas vj + |kosa kata|

	        double[] probClass     = new double[inputFile.length];       //probbalitas class
	        
	        HashMap<String,Integer> countWord;
	        double prob;
	        int jumWord;
	        
	        try {
	    	    PrintWriter pw = new PrintWriter(namaFileOutput);
		        pw.println(Integer.toString(inputFile.length));  //jumlah class
		        
		        
		        
		        //nama kelas, sesuai nama file
		        for (int k=0;k<inputFile.length;k++) {
		        	pw.println(className[k]);  //nama class           	
	            }
		        
	            for (int k=0;k<inputFile.length;k++) {
	           	    probClass[k] = Math.log( (double) jumTweetPerClass[k] / jumTweetTotal);
	           	    pw.println(Double.toString(probClass[k]));  // prob setiap class
	            }
	            
	            for (int k=0;k<inputFile.length;k++) {        	 
		        	 countWord = wordCountinClass.get(k);
		        	 jumWord = countWord.size();  //jumlah kata di dalam kelas
		        	 pw.println("=====>jum_kata,"+jumWord);
		        	 pw.println( Math.log((double) (1) / (jumWord + jumWordTotal)) ); //default value kata tanpa freq
		        	 for (Map.Entry<String,Integer> entry : countWord.entrySet()) {
		        		 kata = entry.getKey();
		        		 freq = entry.getValue();
		        		 prob = Math.log((double) (freq + 1) / (jumWord + jumWordTotal ));  //dalam logiritmk
		        		 pw.println(kata+","+prob);  //tulis ke file
		             } //endfor
		        }
	            pw.close();
	        }  catch (Exception e) {
	            log.severe(e.toString());
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
