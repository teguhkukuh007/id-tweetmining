/*
 *  Copyright (C) 2010 yudi wibisono (yudi@upi.edu / yudi1975@gmail.com)
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Kmeans clustering
 * @author Yudi Wibisono (yudi@upi.edu)
 */
public class KMeansClusteringDB {
	
    public String dbName;
    public String userName;
    public String password;
    public String namaFileOut;  //TBD output lebih baik ke DB atau ke file teks ya?
	
   
    /**
     *  clustering dengan KMeans
     *  @param K jumlah cluster
     *  @param vectorFile file berisi represetnasi vector doc (dihasilkan oleh TFIDF.process)
     *  @param inputTweetFile  file berisi tweet (input dari TFIDF.process), baris di file ini harus berkorespondensi dengan baris di vectorFile
     *  output adalah file vectorFile+"_kmeans.txt"
     */

    public void process(int K) {
        System.out.println("KMeansClustering");
        System.out.println("Lab Basdat Ilkom UPI (cs.upi.edu)");
        System.out.println("=================================");
        //ambil data, pindahkan ke memori
        Logger log = Logger.getLogger("edu.cs.upi.kmeans");


        ArrayList<DocKMeans> alTweet = new ArrayList<DocKMeans>();
        ArrayList<ClusterKMeans> alCluster = new ArrayList<ClusterKMeans>();
        Connection conn=null;       
        PreparedStatement pTw = null;
        try {         
        	Class.forName("com.mysql.jdbc.Driver");            
            String strCon = "jdbc:mysql://"+dbName+"?user="+userName+"&password="+password;
            System.out.println(strCon);
            conn = DriverManager.getConnection(strCon);
            
            pTw  =  
              conn.prepareStatement ("select tj.text_prepro as tw,t.tfidf_val as tfidf from tfidf t, tw_jadi tj where t.id_internal_tw_jadi = tj.id_internal");
            ResultSet rsTw = pTw.executeQuery();
            int cc=0;
            while (rsTw.next())   {  //bobotnya	
                cc++;
                //strLine2 = br2.readLine();          //tweet-nya
            	String strLine2 = rsTw.getString(1);  //tweet
            	String strLine = rsTw.getString(2);   //tfidf
            	
                DocKMeans tweet = new DocKMeans();
                tweet.text = strLine2;
                tweet.addTermsInLine(strLine);
                System.out.println("-->"+tweet.text);
                alTweet.add(tweet);
            }

        } catch (Exception e) {
            log.severe(e.toString());
        }
        finally {
            try {
                pTw.close();
                conn.close();
            } catch (Exception e) {
                log.log(Level.SEVERE, null, e);
            }    
        }
        
        
        
        //inisiasi cluster
        DocKMeans t;

        for (int i=0;i<K;i++) {
            ClusterKMeans c = new ClusterKMeans(i);
            do {
                int idx = (int) (Math.random()*alTweet.size());
                t = alTweet.get(idx);
            } while(t.idCluster!=null);  //sudah diassign, cari yg lain 
            //fs: t.idCluster == null
            c.addDoc(t);
            c.calcCentroid();
            alCluster.add(c);
            //c.print();
        }

        //boolean terus = false;
        //do {
        Cluster junkCluster= new ClusterKMeans(-1);
        int numChange=0;
        do  {    //kondisi berhentinya nanti diganti
           numChange=0;
           //System.out.println(i);
           //bersihkan cluster
           for (Cluster c:alCluster) {
               c.clear();
           }

           //cari yang paling dekat
           //int cc=0;
           for (DocKMeans d:alTweet) {
               if (d.idCluster==junkCluster) {continue;}  //sekali masuk sampah, tidak dikeluarkan
               if (d.isJunk()) {                //tweet sampah, kebanyakan term weightnya 0
                    junkCluster.addDoc(d);
                    continue;
               } 
               //cc++;
               //System.out.println(cc);
               double maxVal= -Double.MIN_VALUE;
               ClusterKMeans nearestCluster = null;
               double val=0;
               for (ClusterKMeans c:alCluster) {  //bandingkan dengan semua cluster
                       val = d.similar(c.centroid); //
                       if (val>maxVal) {
                            maxVal = val;
                            nearestCluster = c;
                       }
               }
               if (nearestCluster==null)  {  //ada yg error
                   log.log(Level.SEVERE, "Error!!{0}");
               } 
               if (d.oldIdCluster!=nearestCluster) {
                   //terjadi perubahan cluster
                   numChange++;
               }

               nearestCluster.addDoc(d);
            }  //traversal semua doc

            //hitung ulang centroid
            for (ClusterKMeans c:alCluster) {
                c.calcCentroid();
            }
            System.out.println("Jumlah perubahan:"+numChange);
            
        } while (numChange!=0);


        //print ke output file
        try {
           PrintWriter pw = new PrintWriter(namaFileOut);
           //PrintWriter pwRowNum = new PrintWriter(namaFileOutputRowNum);
           //hitung ulang centroid
           for (Cluster c:alCluster) {  //bandingkan dengan semua cluster
              pw.append(c.toString());
 //             pwRowNum.append(c.printWithRowNum());
           }
           pw.append(junkCluster.toString());
           pw.close();
 //          pwRowNum.close();
        } catch (Exception e) {
            log.severe(e.toString());
        }

        //inisiasi:
        // isi cluster dengan tweet secara random (satu cluster satu tweet)
        // centroid diisi dengan tweet
        
        //loop sd kondisi berhenti (tidak ada perubahan)
          // for i  = 1 to K
          //    kosongkan cluster(i)
          // for j = 1 to N (N: jumlah semua tweet)
          //     cari tweet(j) paling dekat dengan centroid mana
          //     masukan tweet(j) ke cluster yg paling dekat
          // for i = 1 to K
          //     hitung ulang centroid

        

          //rumus kedekatan dua tweets   (sum (x[i] x y[i]) / (  sqr(sum(x[i]^2)) x sqr(sum(y[i]^2))  )
    }

    public static void main(String[] args) {
        //testing
        KMeansClusteringDB km = new KMeansClusteringDB();
        //km.process("e:\\tweetmining\\corpus_0_1000_prepro_nots_preproSyn_tfidf.txt","e:\\tweetmining\\corpus_0_1000_prepro_nots_preproSyn.txt",50);
        ////km.process("g:\\eksperimen\\data_weka\\tw_obama_tfidf.txt","g:\\eksperimen\\data_weka\\tw_obama.txt",50);("g:\\eksperimen\\data_weka\\tw_obama_tfidf.txt","g:\\eksperimen\\data_weka\\tw_obama.txt",50);
        km.dbName="localhost/obama";
        km.userName="yudi3";
        km.password="rahasia";
        km.namaFileOut= "g:\\eksperimen\\obama\\cluster.txt";
        km.process(70);
    }
}
