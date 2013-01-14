package edu.upi.cs.tweetmining;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.upi.cs.tweetmining.DocKMeans.TermStatComparable;

public class AggloClustering {
	
	
	private class JarakCluster  {
		public int i;
		public int j;
		double jarak; 
		public JarakCluster(int vi,int vj,double vJarak) {
			i = vi;
			j = vj;
			jarak = vJarak;
		}
	}
	
	//berdasarkan JarakCluster
    public class JarakClusterComparable implements Comparator<JarakCluster>{
        @Override
        public int compare(JarakCluster o1, JarakCluster o2) {
                return (o1.jarak>o2.jarak ? -1 : (o1.jarak==o2.jarak ? 0 : 1));
        }
    }
	
	
	public String dbName;
    public String userName;
    public String password;
    public String namaFileOut;
	
	
	public void process() {
		//mulai dari jumlah cluster sebanyak data
		//loop untuk semua cluster, cari cluster yang jarak antar keduanya terpendek, gabung
		//kondisi berhenti jika sisa cluster=2
		
		
		//load data tweet 
		System.out.println("AggloClustering");
        System.out.println("Lab Basdat Ilkom UPI (cs.upi.edu)");
        System.out.println("=================================");
        //ambil data, pindahkan ke memori
        Logger log = Logger.getLogger("edu.cs.upi.kmeans");


        ArrayList<DocKMeans> alTweet = new ArrayList<DocKMeans>();            //kumpulan tweet
        
        
        
        Connection conn=null;       
        PreparedStatement pTw = null;
        try {         
        	Class.forName("com.mysql.jdbc.Driver");            
            String strCon = "jdbc:mysql://"+dbName+"?user="+userName+"&password="+password;
            System.out.println(strCon);
            conn = DriverManager.getConnection(strCon);
            
            
            //nanti limitnya dimatikan
            pTw  =  
              conn.prepareStatement ("select tj.text_prepro as tw,t.tfidf_val as tfidf from tfidf t, tw_jadi tj where t.id_internal_tw_jadi = tj.id_internal and trim(t.tfidf_val)<>'' limit 0,100");
            ResultSet rsTw = pTw.executeQuery();
            int cc=0;
            while (rsTw.next())   {  //bobotnya	
                cc++;
            	String strLine2 = rsTw.getString(1);  //tweet
            	String strLine = rsTw.getString(2);   //tfidf
            	
                DocKMeans tweet = new DocKMeans();
                tweet.text = strLine2;
                tweet.rowNum = cc;
                tweet.addTermsInLine(strLine);
                //System.out.println("-->"+tweet.text);
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
        //fs: tweet sudah masuk ke array
        
        ArrayList<ClusterKMeans> alCluster = new ArrayList<ClusterKMeans>();  //kumpulan cluster
        
        //insialisasi cluster, satu cluster satu tweet
        DocKMeans t;
		for (int i=0;i<alTweet.size();i++) {
			ClusterKMeans c = new ClusterKMeans(i);
			t = alTweet.get(i);
			c.addDoc(t);
			c.calcCentroid();
	        alCluster.add(c);
		}
		

		//loop, sampai jumlah cluster =2
			//cari pasangan tweet yang paling dekat
			//buat matrix jarak
			//jarak[a,b] 
			//cari a,b yang paling kecil jaraknya dimana a<>b
			//gabung a,b
			//jumlah cluster akan berkurang separuh
		
		ArrayList<JarakCluster> alJarakCluster = new ArrayList<JarakCluster>();  //kumpulan cluster
		ClusterKMeans c1,c2;
		for (int i=0;i<alCluster.size();i++) {
			c1 = alCluster.get(i);
			for (int j=i;j<alCluster.size();j++) {
				if (i!=j) {
					c2 = alCluster.get(j);
					double jarakcluster = c1.calcJarakAntarCluster(c2);
					alJarakCluster.add(new JarakCluster(i,j,jarakcluster));
				}
			}
		}
		Collections.sort(alJarakCluster, new JarakClusterComparable()); //sort berdasarkan terpendek
		
		JarakCluster jc;
		ClusterKMeans ca,cb,cgab;
		
		ArrayList<ClusterKMeans> alClusterBaru = new ArrayList<ClusterKMeans>();
		
		
		int cc=0;
		for (int i=0;i<alJarakCluster.size();i++) {    //sudah terurut, bisa digabung dari yg terkecil
			jc = alJarakCluster.get(i);
			ca = alCluster.get(jc.i);
			cb = alCluster.get(jc.j);
			//gabung cluster, tandai agar tidak digabung lagi
			if (!ca.flag&&!cb.flag) {
				System.out.println("Gabung "+jc.i+" dengan "+jc.j);
				ca.flag = true;
				cb.flag = true;
				ca.mergeCluster(cb);  //
				//cc++;
				//System.out.println(cc);
				
			}
			
			
//debug
//			System.out.println(ca.centroid.toString());
//			System.out.println("++");
//			System.out.println(cb.centroid.toString());
//			System.out.println(jc.jarak);
//			System.out.println("------");
		}
	}
	
	
	public static void main(String[] args) {
		AggloClustering aggC= new AggloClustering();
		aggC.dbName="localhost/obama";
		aggC.userName="yudi3";
		aggC.password="rahasia";
		aggC.namaFileOut= "g:\\eksperimen\\obama\\cluster.txt";
		aggC.process();
	}
}
