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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * PAMClustering =  Partitioning Around Medoids
 * source: Finding Groups in Data An Introduction to Cluster Analysis
 * LEONARD KAUFMAN, PETER J. ROUSSEEUW
 * 
 *
 *
 * @author yudi wibisono
 */

public class PAMClustering {
    private static final Logger log = Logger.getLogger("edu.cs.upi.PAMClustering");

    /**
     *  tweet representastion
    */
     private class DocPAM extends DocKMeans{
        boolean isMedoid=false;
     }

     private class DissMatrix {   //matrix perbedaan antara tweet  
        private double [][] disMatrix;

        public double getDis(int id1,int id2) {
            if (id1>id2) {
                return disMatrix[id1][id2];
            }
            else {
                return disMatrix[id2][id1];
            }
        }
        
        public void process(ArrayList<DocPAM> alTweet) {
            //matrix            
            //1-1
            //2-1 2-2            -> jarak tweet 2 ke tweet 1, tweet 2 ke 2 
            //3-1 3-2 3-3
            //4-1 4-2 4-3 4-4

            disMatrix = new double[alTweet.size()][];
            for ( int i = 0; i<disMatrix.length ; i++ ) {  //baris
                disMatrix[i] = new double[ i + 1 ];
                //isi elemenjaraknya
                DocPAM tw = alTweet.get(i);
                tw.id = i;
                for (int j=0;j<disMatrix[i].length;j++) {    //loop kolom, dikurangi 1 karena tidak perlu bagian terkahir
                    double jarak = tw.similar(alTweet.get(j));
                    disMatrix[i][j] = jarak;
                }

            }

        }
     }

     private class ClusterPAM extends Cluster {

        private DocPAM medoid;
        private DissMatrix dMatrix; //dissimilarity matrix
        private double totDis=0;      //total dissimilarty

        @Override
        public void clear() {
            super.clear();
            medoid.isMedoid = false;
            medoid=null;
            totDis=0;
        }

        @Override
        public void addDoc(Doc d) {
           super.addDoc(d);
           totDis += similarWithMedoid(d);
        }

        public double getTotDis() {
            return totDis;
        }

        //cari perbedaan t dengan medoid
        public double similarWithMedoid(Doc t) {
            //req: dMatrix harus terisi
            return dMatrix.getDis(medoid.id, t.id);            
        }

        public ClusterPAM(int idCluster,DissMatrix dm) {
            super(idCluster);
            dMatrix = dm;
        }

        public void addMedoid(DocPAM t) {
            medoid = t;
            t.isMedoid=true;
            //this.addDoc(t);
        }

        public void replaceMedoid(DocPAM t) {
            medoid.isMedoid = false;
            addMedoid(t);
        }


        @Override
        public String getLabel() {
            return medoid.text+"\n";
        }

		@Override
		public double innerQualityScore() {
			// TODO Auto-generated method stub
			return 0;
		}

   
 }

    //menghasilkan nilai objektif, semakin besar semakin bagus
    private double findNearestCluster( ArrayList<ClusterPAM> alCluster, ArrayList<DocPAM> alTweet) {
           //masukan semua tweet yang lain ke dalam cluster yang sesuai
           for (DocPAM t2: alTweet) {
               //cari tweet paling dekat ke cluster mana
               double maxVal = -Double.MIN_VALUE;
               ClusterPAM nearestCluster = null;
               double val=0;
               for (ClusterPAM c:alCluster) {  //bandingkan dengan semua cluster
                       val = c.similarWithMedoid(t2); //
                       if (val>maxVal) {
                            maxVal = val;
                            nearestCluster = c;
                       }
               }
               if (nearestCluster==null)  {  //ada yg error
                   log.log(Level.SEVERE, "Error!!{0}");
               }
               nearestCluster.addDoc(t2);
           }
           double objVal=0;
           for (ClusterPAM c:alCluster) {
               objVal += c.getTotDis();
           }
           return objVal;
    }

    private void debugMedoid(ArrayList <DocPAM> alT) {
        int jum=0;
        for (DocPAM t:alT ) {
            if (t.isMedoid) {
                jum++;
            }
        }
        System.out.println("Jumlah medoid="+jum);
    }


     /*
        Given k
        Randomly pick k instances as initial medoids
        Assign each data point to the nearest medoid x
        Calculate the objective function
           the sum of dissimilarities of all points to their nearest medoids. (squared-error criterion)
        Randomly select an point y
        Swap x by y if the swap reduces the objective function
        Repeat (3-6) until no change
    */
    public void process(String inputVectorFile,String inputTweetFile, int K) {
        System.out.println("PAMClustering");
        System.out.println("Lab Basdat Ilkom UPI (cs.upi.edu)");
        System.out.println("=================================");
        // ambil representasi tweet secara random
        
        String inputFileWoExt = inputVectorFile.substring(0, inputVectorFile.lastIndexOf('.')); //without ext
        String namaFileOutput = inputFileWoExt+"_PAMCluster.txt";
        String namaFileOutputRowNum = inputFileWoExt+"_PAMCluster_RowNum.txt";  //dengan row number, untuk menghitung akurasi

        System.out.println("Nama file input (berbentuk vector):"+inputVectorFile);
        System.out.println("Nama file input (berbentuk tweet):"+inputTweetFile);
        System.out.println("Nama file output:"+namaFileOutput);
        System.out.println("Nama file output dengan baris:"+namaFileOutputRowNum);
        
        ArrayList<DocPAM> alTweet = new ArrayList<DocPAM>();
        ArrayList<ClusterPAM> alCluster = new ArrayList<ClusterPAM>();
        try {
            // file tweet dalam bentuk vector bobot
            FileInputStream fStreamVector = new FileInputStream(inputVectorFile);
            DataInputStream inVec = new DataInputStream(fStreamVector);
            BufferedReader brVec = new BufferedReader(new InputStreamReader(inVec));
            String strLineVec;

            //file tweet dalam bentuk aslinya
            FileInputStream fStreamStr = new FileInputStream(inputTweetFile);
            DataInputStream inStr = new DataInputStream(fStreamStr);
            BufferedReader brStr = new BufferedReader(new InputStreamReader(inStr));
            String strLineStr;

            int cc=0;
            while ((strLineVec = brVec.readLine()) != null)   {  //bobotnya
                cc++;
                if (strLineVec.equals("")) { //baris kosong diskip (akibat tweet yg kena prepro)
                    strLineStr = brStr.readLine();                 //supaya sinkron
                    continue;
                }
                strLineStr = brStr.readLine();                 //tweet dalam bentuk string nya
                DocPAM tweet = new DocPAM();
                tweet.text = strLineStr;
                tweet.addTermsInLine(strLineVec);
                alTweet.add(tweet);
           } //end while, altweet sudah berisi tweet2

           //hitung dismatrix
           System.out.println("Proses dissimilarity matrix...");
           DissMatrix dm = new DissMatrix();
           dm.process(alTweet);
           System.out.println("END Proses dissimilarity matrix");

           //pilih secara random medoid sebanyak K
           DocPAM t;
           for (int i=1;i<=K;i++) {
                ClusterPAM c = new ClusterPAM(i,dm);
                do {
                    int idx = (int) (Math.random()*alTweet.size());
                    t = alTweet.get(idx);
                } while(t.idCluster!=null);  //sudah diassign, cari yg lain
                //fs: t.idCluster == null
                c.addMedoid(t);
                alCluster.add(c);
           }
           
           //masukan ke cluster terdekat
           double objVal=findNearestCluster(alCluster,alTweet);
           DocPAM newMedoid;
           for (int j=0;j<5000;j++) {
                   
                   System.out.println("");
                   System.out.println("nilai obj="+objVal);
                   //cari medoid baru secara random
                   do {
                     int idx = (int) (Math.random()*alTweet.size());
                     newMedoid = alTweet.get(idx);
                     if (!newMedoid.isMedoid) {break;} //bukan medoid, pake
                   } while(true);  //sudah merupakan medoid, cari yg lain

                   
                   
                   //siapkan cluster baru, copy medoid dari cluster yang lama
                   
                   ArrayList<ClusterPAM> alNewCluster = new ArrayList<ClusterPAM>();
                   for (int i=0;i<K;i++) {
                        ClusterPAM c = new ClusterPAM(i,dm);
                        c.addMedoid(alCluster.get(i).medoid);
                        alNewCluster.add(c);
                   }
                   
                   //pilih secara random cluster yang akan ditukuar medoidnya
                   
                   int idx = (int) (Math.random()*alNewCluster.size());
                   alNewCluster.get(idx).replaceMedoid(newMedoid);
                   //note: bisa berbahaya, saat direplace, ismedoid-nya diisi false, padahal
                   //untuk cluster yang lama (alCluster) dia masih jadi medoid (tidak konesisten,
                   //jadi medoid tapi dengan
                   //ismedoidnya false) efeknya saat random mencari medoid baru maka
                   //bisa terpilih lagi jadi medoid
                   //tapi soluasi lain sptnya tidak efisien

                   //hack:  reset lagi medoid

                   //hitung ulang

                   
                   double newObjVal=findNearestCluster(alNewCluster, alTweet);
                   System.out.println("nilai obj baru="+newObjVal);

                   System.out.println("iterasi ke-"+j);

                   

                   
                   if (newObjVal > objVal) {  //lebih bagus, replace                      
                      System.out.println("hasil lebih baik, ditukar");
                      alCluster = alNewCluster;
                      objVal = newObjVal;
                   } else {
                       //hack:  reset lagi medoid, baca comment di line 259
                       //debugMedoid(alTweet);
                      newMedoid.isMedoid=false;     //tidak digunakan sebagai medoid lagi
                      for (ClusterPAM pc:alCluster) {
                           pc.medoid.isMedoid = true;
                      }

                   }
                   
           }
           //loop, kembali ke awal sampai tidak ada perubahan nilai objektif
           
           
            //print ke output file
           try {
               PrintWriter pwRowNum = new PrintWriter(namaFileOutputRowNum);
               PrintWriter pw = new PrintWriter(namaFileOutput);
               //hitung ulang centroid
               for (Cluster c:alCluster) {  //bandingkan dengan semua cluster
                  pw.append(c.toString());
               }
               pw.close();
               pwRowNum.close();
           } catch (Exception e) {
            log.severe(e.toString());
           }
        //buat matrikx dis untuk semua tweets
        } catch (Exception e) {
            log.severe(e.toString());
        }
        
    }


    public static void main(String[] args) {
        //class test
        PAMClustering pc = new PAMClustering();
        
        pc.process("e:\\tweetmining\\corpus_0_1000_prepro_nots_preproSyn_tfidf.txt","e:\\tweetmining\\corpus_0_1000_prepro_nots_preproSyn.txt",50);

        //pc.process("e:\\tweetmining\\corpus0_10000\\corpus_0_10000_prepro_nots_preproSyn_tfidf.txt","e:\\tweetmining\\corpus0_10000\\corpus_0_10000_prepro_nots_preproSyn.txt",500);
    }
}
