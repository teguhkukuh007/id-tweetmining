/*
 *  Copyright (C) 2010 yudi wibisono (yudi1975@gmail.com/cs.upi.edu)
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
import java.util.ArrayList;
import java.util.logging.Logger;



/**
 *
 * @author yudi wibisono
 */
public class ClusterEvaluation {
    private ArrayList<ArrayList<Integer>> listClassGold = new ArrayList<ArrayList<Integer>>();
    //private ArrayList<Integer> Cluster1;
    
    private ArrayList<ArrayList<Integer>> listCluster = new ArrayList<ArrayList<Integer>>();
    //private ArrayList<Integer> Cluster2;


    private int loadCluster(ArrayList<ArrayList<Integer>> arrCluster, String fileName) {
         //load file ke cluaster
         Logger log = Logger.getLogger("edu.cs.upi.clusterevaluation");
         

         try {
            FileInputStream fstream = new FileInputStream(fileName);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;
            String[] data;
            int countPerClus=0;
            int numLine;
            int cc=0;
            ArrayList<Integer> cluster=null;
            int dataCount=0;
            while ((strLine = br.readLine()) != null)   {  //bobotnya
                cc++;
                //System.out.println("Baris ke:"+cc);
                strLine = strLine.trim();
                if (strLine.equals("<start>")) {
                     cluster = new ArrayList<Integer>();
                     countPerClus=0;
                } else
                {
                   if (strLine.equals("<end>")) {
                      arrCluster.add(cluster);
                      //System.out.println(debug);
                      dataCount += countPerClus;
                   } else
                   if (!strLine.equals("") && strLine.contains("|")) {
                       //System.out.println(strLine);
                       data = strLine.split("\\|");
                       numLine = Integer.parseInt(data[0]);
                       cluster.add(numLine);
                       //dataCount++;
                       countPerClus++;
                   }
                }
            }  //loop file
            fstream.close();
            in.close();
            br.close();
            return (dataCount);
         } catch (Exception e) {
             //e.printStackTrace();
             log.severe(e.toString());
             return (-1);
        }
        
    }
    
    private void printCluster(ArrayList<ArrayList<Integer>> listCluster) {
        //debug
        for (ArrayList<Integer> cluster: listCluster) {
            System.out.println("-start-");
            for (Integer numRow:cluster) {
                System.out.println(numRow);
            }
            System.out.println("-end-");
        }
        
    }


    /*
     *  cluster yg ada di file1 dan file2, diawali dengan <start> diakhiri dengan <end> sisanya diignore
     */
    public void evaluate(String goldClass,String cluster) {
        
        System.out.println("Evaluasi Cluster");
        System.out.println("Lab Basdat Ilkom UPI (cs.upi.edu)");
        System.out.println("=================================");
        //ambil data, pindahkan ke memori
        Logger log = Logger.getLogger("edu.cs.upi.clusterevaluation");
        String file1WoExt = goldClass.substring(0, goldClass.lastIndexOf('.')); //without ext
        String file2WoExt = cluster.substring(0, cluster.lastIndexOf('.')); //without ext
        String outPurity  = file1WoExt+file2WoExt+"_eval_purity.txt";
        System.out.println("Nama file standard:"+goldClass);
        System.out.println("Nama file cluster:"+cluster);
        System.out.println("Nama file output (eval purity):"+outPurity);

        loadCluster(listClassGold, goldClass);
        //printCluster(listClassGold);

        int itemNum = loadCluster(listCluster, cluster);
        //printCluster(listCluster);

        //cari purity
        //purity =  (1 / JUM_TOTAL_TWEET) * SUM(MAX_COUNT(culster IRISAN class))
        //(http://nlp.stanford.edu/IR-book/html/htmledition/evaluation-of-clustering-1.html))
        int cc=0;
        int totMatch=0;
        int bestCluster=-1;
        int bestClusterPair=-1; //pasangan dengan gold
        int bestClusterScore = -99;
        for (ArrayList<Integer> c:listCluster) {                
                System.out.println("Proses Cluster ke-"+cc);
                int max = -9999;
                int maxIdGold = -1;
                int dd=0;
                for (ArrayList<Integer> gold:listClassGold) { //loop untuk setiap gold standard                    
                    int match=0;
                    for (int i:c) {                          //loop untuk setiap item di dalam cluster
                        for (int g:gold) {
                           if (i==g) {
                               match++;  //cocok
                               break;
                           }
                        }
                    }
                    if (match>max) {
                        max = match;
                        maxIdGold = dd;
                    }
                    dd++;
                }
                System.out.println("Yang paling cocok");
                System.out.println("  Jum cocok:"+max);
                totMatch += max;
                System.out.println("  id clsss gold:"+maxIdGold);
                System.out.println();                
                if (max>bestClusterScore) {
                    bestClusterScore = max;
                    bestCluster = cc;
                    bestClusterPair = maxIdGold;
                }
                cc++;
        }
        System.out.println("Jumlah cocok="+totMatch);
        System.out.println("Jumlah total item="+itemNum);
        double purity =  ((double) 1/itemNum) * totMatch;

        System.out.println("");
        System.out.println("ID Cluster terbaik="+bestCluster);
        System.out.println("ID Cluster pasangan di gold standard="+bestClusterPair);
        System.out.println("Jumlah match cluster terbaik="+bestClusterScore);
        

        System.out.println();
        System.out.format("Purity: %4.3f \n",purity);
    }

    public static void main(String[]args) {
        ClusterEvaluation ce = new ClusterEvaluation();
        ce.evaluate("e:\\tweetmining\\cluster_ideal_human_corpus_0_1000.txt","e:\\tweetmining\\corpus_0_1000_prepro_nots_preproSyn_tfidf_PAMCluster_RowNum.txt");
        //ce.evaluate("e:\\tweetmining\\cluster_ideal_human_corpus_0_1000.txt","e:\\tweetmining\\corpus_0_1000_prepro_nots_preproSyn_tfidf_kmeansCluster_RowNum.txt");
    }
}
