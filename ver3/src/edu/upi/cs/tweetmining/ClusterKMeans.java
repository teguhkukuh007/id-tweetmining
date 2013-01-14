package edu.upi.cs.tweetmining;

import java.util.HashMap;
import java.util.Map;

public class ClusterKMeans extends Cluster {
	 /**
     * representasi cluster dengan centroid
     *
     *
     */
        public boolean flag=false;  //untuk berbagai keperluan
        public DocKMeans centroid = new DocKMeans();

        public ClusterKMeans(int idCluster) {
            super(idCluster);
        }
        
        public String getLabel() {
           return centroid.bestToString();
        }
        
        /*
         *   menghitung jarak antar cluster 
         *   (menggunakan jarak antar centroid)
         * 
         */
        public double calcJarakAntarCluster(ClusterKMeans c) {        	
        	return c.centroid.similar(this.centroid);
        }

        public void calcCentroid() {
            centroid = new DocKMeans();  //clear centroid
            HashMap<String,TermStat> hmTerm = new HashMap<String,TermStat>(); //term yg akan ada di centroid
            for (Doc d : alDoc) {  //semua dok dalam cluster
                TermStat ts;
                String s;
                Double val;
                for (Map.Entry<String,Double> entry : d.vector.entrySet()) {  //for semua term dalam dok
                      s = entry.getKey();
                      val = entry.getValue();
                      ts = hmTerm.get(s);
                      if (ts==null)  {  //belum ada, add
                          hmTerm.put(s,new TermStat(s,val));
                      } else  {         //sudah ada, update weightnya
                          ts.addVal(val);
                      }
                } //for each term dalam doc
            } //semua doc dalam cluster

            //pindahkan hmTerm menjadi centroid
            TermStat ts;
            double val;
            double numDoc;
            for (Map.Entry<String,TermStat> entry : hmTerm.entrySet()) {
                ts = entry.getValue();
                //ts.calcAvg();  //tidak dibagi freq, tapi dibagi jumlah doc
                numDoc = alDoc.size();
                val = ts.getTotVal() / numDoc;
                //centroid.addTerm(entry.getKey(),ts.getAvg());
                centroid.addTerm(entry.getKey(),val);
            }
        }


} //class clusterKMeans



