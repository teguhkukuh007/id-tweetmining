package edu.upi.cs.tweetmining;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;


public class AggloClusteringDB {
/*
 *   Agglomerative clustering (biner)
 *   
 *   tabel input: 
 *   - twjadi hasil dari PrepoTwMentahDB
 *   - tfidf hasil  dari TfidfDB
 *   
 *   output ke file teks (dalam format html)
 *   - tbd: output ke table juga??   
 *    
 *   note: cek querynya, mungkin menggunakan limit
 *   
 */
	
	private final  Logger log = Logger.getLogger("edu.cs.upi.aggloclustering");
	public String dbName;
    public String userName;
    public String password;
    //public String namaFileOutput;
    public String tableNameTwJadi="tw_jadi";  //default
    public String tableNameTfidf ="tfidf";
	
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
    private class JarakClusterComparable implements Comparator<JarakCluster>{
        @Override
        public int compare(JarakCluster o1, JarakCluster o2) {
                return (o1.jarak>o2.jarak ? -1 : (o1.jarak==o2.jarak ? 0 : 1));
        }
    }
    
    //berdasarkan kohesi
    private class CohesionClusterComparable implements Comparator<ClusterAgglo>{
        @Override
        public int compare(ClusterAgglo o1, ClusterAgglo o2) {
            double coh1 = o1.getCohesion();     
            double coh2 = o2.getCohesion();
        	return (coh1>coh2 ? -1 : (coh1==coh2 ? 0 : 1));
        }
    }
    
    //berdasarkan inner quality
    private class InnerQClusterComparable implements Comparator<ClusterAgglo>{
        @Override
        public int compare(ClusterAgglo o1, ClusterAgglo o2) {
            double coh1 = o1.innerQualityScore();     
            double coh2 = o2.innerQualityScore();
        	return (coh1>coh2 ? -1 : (coh1==coh2 ? 0 : 1));
        }
    }
    
    //berdasarkan level
    private class LevelClusterComparable implements Comparator<ClusterAgglo>{
        @Override
        public int compare(ClusterAgglo o1, ClusterAgglo o2) {
            double coh1 = o1.level;     
            double coh2 = o2.level;
        	return (coh1<coh2 ? -1 : (coh1==coh2 ? 0 : 1));
        }
    }

    
    private ArrayList<ClusterAgglo> alAllCluster = new ArrayList<ClusterAgglo>();  //semua cluster dalam tree
	
    public void printTerbaik(String namaFile,int jumMaxCluster) {
    	//panggil seteah
    }
	
    public void printAll(String namaFile) {
    	//panggil setelah this.process dipanggil
    	try {
			PrintWriter pw = new PrintWriter(namaFile);
			//Collections.sort(alAllCluster, new CohesionClusterComparable()); //sort berdasarkan kohesi 
			//Collections.sort(alAllCluster, new InnerQClusterComparable()); //sort berdasarkan quality 
			Collections.sort(alAllCluster, new LevelClusterComparable()); //sort berdasarkan level
			double sumK=0;
			for  (int i=0;i<alAllCluster.size();i++) {
				ClusterAgglo c = alAllCluster.get(i);
				pw.println("Kohesi="+c.getCohesion()+" ");
				pw.println("Clus Level="+c.level +" ");
				pw.println("innerq="+c.innerQualityScore());
				sumK = sumK + c.getCohesion();
				pw.println("Clus ID="+c.idCluster+" ");
				pw.println(c.getMedoid());
				pw.println();
			}
			double avgK = sumK/alAllCluster.size();
			pw.println("Rata2 kohesi:"+avgK);
			pw.close();
		}//try	
		catch(Exception e) {
			e.printStackTrace();
			log.severe(e.toString());
		}
    	
    }
    
    
    public void process(String filter) {
		//menggunakan LIMIT, cek dulu!!
		//mulai dari jumlah cluster sebanyak data
		//loop untuk semua cluster, cari cluster yang jarak antar keduanya terpendek, gabung
		//kondisi berhenti jika sisa cluster=2
		
		
		//load data tweet 
		System.out.println("AggloClustering");
        System.out.println("Lab Basdat Ilkom UPI (cs.upi.edu)");
        System.out.println("=================================");
        //ambil data, pindahkan ke memori
        


        ArrayList<DocKMeans> alTweet = new ArrayList<DocKMeans>();            //kumpulan tweet
        
        Connection conn=null;       
        PreparedStatement pTw = null;
        try {         
        	Class.forName("com.mysql.jdbc.Driver");            
            String strCon = "jdbc:mysql://"+dbName+"?user="+userName+"&password="+password;
            System.out.println(strCon);
            conn = DriverManager.getConnection(strCon);
            
            //nanti limitnya dimatikan?
            //data yang duplikat tidak diproses
            //ambil tweet original karena untuk perhitungan yg digunakan adalah tfidf
            
            String q = "select  concat('(',tj.id_internal,')',tj.text) as tw,t.tfidf_val as tfidf "+
            		   "from "+ tableNameTfidf +" t,"+ tableNameTwJadi + " tj " + 
            		   "where t.id_internal_tw_jadi = tj.id_internal and trim(t.tfidf_val)<>'' and tj.is_duplicate=0 " +
            		   filter+
            		   " limit 0,1000";
            
            pTw  =  conn.prepareStatement (q);

            ResultSet rsTw = pTw.executeQuery();
            //load tweet ke memori;
            while (rsTw.next())   {  
//                cc++;
            	String strTw     = rsTw.getString(1);  //tweet
            	String strTfidf  = rsTw.getString(2);   //tfidf
                DocKMeans tweet = new DocKMeans();
                tweet.text = strTw;
                tweet.addTermsInLine(strTfidf);
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
        //fs: tweet sudah masuk ke arrayList
        
        //hitung total level
        long level=-99;
     	double lg = Math.log(alTweet.size()) / Math.log(2);
     	if (lg % 1.0 > 0) {  //ada pecahan, 
     			level = (long) (Math.floor(lg)+2);
     	} else if (lg % 1.0 == 0) {
     			level =(long) ( lg+1);
     	}
     	long maxLevel = level; 
     		
     	System.out.println("Jum data:"+alTweet.size());
     	System.out.println("Total level:"+level);
        
        ArrayList<ClusterAgglo> alCluster = new ArrayList<ClusterAgglo>();  //kumpulan cluster per level
        
        //insialisasi cluster, satu cluster satu tweet
        DocKMeans t;
		for (int i=0;i<alTweet.size();i++) {
			ClusterAgglo c = new ClusterAgglo(i);
			t = alTweet.get(i);
			c.addDoc(t);
			c.calcCentroid();
			c.level = level;
			c.maxLevel = maxLevel;
	        alCluster.add(c);
	        alAllCluster.add(c);
		}
		

		//loop, sampai jumlah cluster =1
			//cari pasangan tweet yang paling dekat
			//buat matrix jarak
			//jarak[a,b] 
			//cari a,b yang paling kecil jaraknya dimana a<>b
			//gabung a,b
			//jumlah cluster akan berkurang separuh
		
		
	 
		
		long incID= 0;
			
		
		while (alCluster.size()>1) {
			level--;
			//hitung semua jarak antara cluster 
			ArrayList<JarakCluster> alJarakCluster = new ArrayList<JarakCluster>();  //kumpulan jarak cluster
			ClusterAgglo c1,c2;
			double totalJarak=0;
			int cc=0;
			for (int i=0;i<alCluster.size();i++) {
				c1 = alCluster.get(i);
				for (int j=i;j<alCluster.size();j++) {
					if (i!=j) {
						c2 = alCluster.get(j);
						double jarakCluster = c1.calcJarakAntarCluster(c2);
						totalJarak = totalJarak + jarakCluster;
						cc++;
						alJarakCluster.add(new JarakCluster(i,j,jarakCluster));
					}
				}
			}
			
			double avgJarak=0;
			if (cc!=0) {
				avgJarak = totalJarak / cc;
			}
			
			//hitung rata2 kohesi
			double totKohesi=0;
			cc=0;
			for (ClusterAgglo cl:alCluster) {
				totKohesi = totKohesi + cl.getCohesion();
				cc++;
			}
			
			double avgKohesi = 0;
			if (cc!=0) avgKohesi = totKohesi/cc;
			
			
			System.out.println("level:"+(level+1)+" avg jarak ="+avgJarak);  //semakin jauh (kecil) semakin bagus
			System.out.println("jum cluster:"+alCluster.size());
			System.out.println("avg kohesi="+avgKohesi);    //semakin dekat (besar) semakin bagus
//			double bobotJarak  = 80;
//			double bobotKohesi = 20;
			//(1-((double)level/maxLevel))
			double nilaiKombinasi = ( (1-avgJarak)  + avgKohesi)/2;         //semakin besar semakin bagus
			System.out.println("kombinasi="+nilaiKombinasi);
			
			
			Collections.sort(alJarakCluster, new JarakClusterComparable()); //sort berdasarkan terdekat (1:paling dekat, 0: paling jauh, krn pake cosine)
			
			
			//penggabungan cluster yang terdekat
			JarakCluster jc;
			ClusterAgglo ca,cb;
			ArrayList<ClusterAgglo> alClusterBaru = new ArrayList<ClusterAgglo>();
//			int cc=0;
			for (int i=0;i<alJarakCluster.size();i++) {    //sudah terurut, bisa digabung dari yg terdekat
				jc = alJarakCluster.get(i);
				ca = alCluster.get(jc.i);
				cb = alCluster.get(jc.j);
				//gabung cluster, tandai agar tidak digabung lagi
				if (!ca.flag&&!cb.flag) {
					//System.out.println("Gabung "+jc.i+" dengan "+jc.j+" dengan jarak:"+jc.jarak);
					//ca.print();
					//cb.print();
					//System.out.println("-------------");
					ca.flag = true;
					cb.flag = true;
					ClusterAgglo cGab = new ClusterAgglo(incID*1000+ca.idCluster);
					cGab.level = level;
					cGab.maxLevel = maxLevel;
					cGab.mergeCluster(ca);
					cGab.mergeCluster(cb);  
					cGab.calcCentroid();
					cGab.calcCohesion();
					ca.parent = cGab;
					cb.parent = cGab;
					cGab.addChild(ca);
					cGab.addChild(cb);
					alClusterBaru.add(cGab);
					alAllCluster.add(cGab);
					//cc++;
					//System.out.println(cc);
				}
	//debug
	//			System.out.println(ca.centroid.toString());
	//			System.out.println("++");
	//			System.out.println(cb.centroid.toString());
	//			System.out.println(jc.jarak);
	//			System.out.println("------");
			} //end for
			//yang sisa belum bergabung dimasukkan
			for (ClusterAgglo c:alCluster) {
				if (!c.flag) {
//					System.out.println("tdk punya pasangan:");
//					c.print();
					//tambah ke cluster baru
					//level cluster tidak berubah
					alClusterBaru.add(c);
				}
			}
			alCluster = alClusterBaru;	
			incID++;
		} //end loop
		//alCluster berisi cluster paling atas
		//print rekursif
		
		//beri label
		
		//beriLabel(alCluster,"1");
		//printTreeMedoid(alCluster,"",1);
		//printTree(alCluster);
		
		
		//
		
		
        //tampilkan cluster berdasarkan kohesinya 		
		
		System.out.println("Seesai");
		
//		//tampilkan dalam bentuk hirarki
//		try {
//			PrintWriter pw = new PrintWriter(namaFileOutput);
//			pw.println("<html>");
//			pw.println("<ul>");
//			printTreeHTML(alCluster.get(0),pw); //cluster paling atas hanya ada satu
//			pw.println("</ul>");
//			pw.println("</html>");
//			pw.close();
//		}
//		catch (Exception e) {
//			log.severe(e.toString());
//		}
	}
	
	public void printTreeHTML(ClusterAgglo c, PrintWriter pw) {		
		
		//print 
//		if (c.parent!=null) {
//			System.out.println("idParent:"+c.parent.idCluster);
//		} else {
//			System.out.println("idParent: <none>  ROOT");
//		}
		pw.println("<li>");
		pw.println("idcluster="+c.idCluster);
		pw.println(";Level"+c.level+";Kohesi="+c.getCohesion()+";");
		pw.println(c.getMedoid());
		pw.println("</li>");
		
		//print  anak
		if (c.child.size()>0)  //punya anak
		{
			pw.println("<ul>");
			for (ClusterAgglo ch:c.child) {
				printTreeHTML(ch,pw);			
			}
			pw.println("</ul>");
		}
	}
	

	
	public void printTree(ArrayList<ClusterAgglo> alCluster) {
		for (ClusterAgglo c: alCluster) {
			if (c.parent!=null) {
				System.out.println("idParent:"+c.parent.idCluster);
			}
			c.print();
			System.out.println("anak-->");
			if (c.child!=null) {
				printTree(c.child);
			} 		
		}
	}
	
	
	public static void main(String[] args) {
		AggloClusteringDB aggC= new AggloClusteringDB();
		aggC.dbName="localhost/obama2";
		aggC.userName="yudi3";
		aggC.password="rahasia";
		aggC.tableNameTwJadi="tw_jadi";
		aggC.tableNameTfidf = "tfidf_2000";
		//aggC.namaFileOutput = "D:\\xampp\\htdocs\\obama\\obama_2000_level.txt";
		//aggC.tableNameTwJadi ="tw_jadi_sandyhoax_nodup_dukungan";
		aggC.process("");
		aggC.printAll("D:\\xampp\\htdocs\\obama\\obama_2000_level.txt");
	}
}
