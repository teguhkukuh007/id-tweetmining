package edu.upi.cs.tweetmining;

import java.util.HashMap;
import java.util.Map;

public class ClusterKMeans extends Cluster {
	 /**
     * representasi cluster dengan centroid
     *
     *
     */
	
	   @Override
	   public String toString() {
	            StringBuilder sb = new StringBuilder();
	            sb.append("\nIDCluster:").append(idCluster).append("\n");
	            sb.append("Label"+"\n");
	            sb.append(this.getLabel());
	            sb.append("Medoid:"+"\n");
	            sb.append(this.getMedoid());
	            sb.append("\n");
	            sb.append("Isi Cluster:  \n");
	            for (Doc d : alDoc) {
	                sb.append(d.text).append("\n");
	                //sb.append(d.toString()).append("\n");  //debug,
	            }
	            sb.append("\n");
	            return sb.toString();
	    }
	
        public DocKMeans centroid = new DocKMeans();

        public ClusterKMeans(int idCluster) {
            super(idCluster);
        }
        
        public String getLabel() {
           return centroid.bestToString();
        }
        
        public String getMedoid() {
        	//return tweet yang paling dekat dengan centroid
        	double val;
        	double minVal = Double.MAX_VALUE;
        	Doc m=null;
        	for (Doc d : alDoc) {  //semua dok dalam cluster
        		val = centroid.similar((DocKMeans)d);
        		if (val<minVal) {
        			minVal = val;
        			m = d;
        		}
        	}
        	return m.text;
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



