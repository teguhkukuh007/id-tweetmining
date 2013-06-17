/*
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

/*     
 *     casefolding, stopwords removal, url removal, mention removal
 *     
 *     tabel input minimal mengandung field: id_internal, text (tweetnya), text_prepro (output), is_prepro, is_duplicate_checeked, is_duplicate
 *     
 *     akan memproses record dengan field is_prepro=0
 *     
 *     input: 
 *     	tabel tw_jadi.text (gunakan ProsesTwMentahDB untuk mendapatkan tw_jadi dari tw_mentah)
 *      
 *     output
 *      tw_jadi.text_prepro 
 *      
 *     setelah diproses akan merubah field tw_jadi.is_prepro menjadi 1 
 *     
 *     def table untuk stopwords ada di bawah
 *     atau bisa dibuat dengan utility fileStopwordsToDB dan fileSinonimToDB
 *     
 *     
 *     
 *     	parameter salah pada bagian:" +strLast+" Gunakan parameter dengan format:
    	-db databasename -u dbusername -p dbpassword -tablename namatabel -buangcharberurutan -flagduplicate yes_or_no -delay delay_antar_query_dlm_detik -buangmention -sinonim  -buangstopwords -buanghashtag
    	Contoh:
    	G:\\LibTweetMining2\\bin>java -classpath .;../libs/mysql-connector-java-5.0.8-bin.jar;../libs/jackson-all-1.9.9.jar edu.upi.cs.tweetmining.PreproDB -db localhost/obamarumor -u yudi3 -p rahasia  -tablename tw_jadi -buangcharberurutan -flagduplicate yes -delay 10 -buangmention -sinonim  -buangstopwords -buanghashtag
    	
 *     
 */

public class PreproDB {
	private static final Logger logger = Logger.getLogger("Prepodb");
	
    private HashMap<String,String> hmSinonim = new HashMap<String,String>();  //pasangan kata sinonim
    private ArrayList<String> alStopWords = new ArrayList<String>();          //kata stopwordss
	public int jumTdkDiproses=0;
	public int jumDiproses=0;
	
	public String dbName;           // format: localhost/mydbname
	public String userName;
	public String password;
	
	//tabel input minimal mengandung field: id_internal, text (tweetnya), text_prepro (output), is_prepro, is_duplicate_checked, is_duplicate
	public String tableName="tw_jadi";        // table tweet_jadi (lihat output class ProsesTwMentah) 
	                                	      //tweet disimpan di field text, output prepro akan disimpan di field text_prepro, 
										      //flag difield is_prepro  (0 belum diprepro, 1 sudah) <-- jgn lupa diindex
	
	
	public boolean isBuangMention=false;
	public boolean isBuangHashtag=false;
	public boolean isProsesSinonim=false;
    public boolean isBuangStopwords=false;
    public boolean isBuangCharBerurutan=false;
	
	public PreproDB() {
		 Handler consoleHandler = new ConsoleHandler();
		 logger.addHandler(consoleHandler);  
	}
	
	
	public void fileSinonimToDB(String fileName,String tableName,String fieldNameKata, String fieldNameSinonim) {   //sinonim, smiley
	  //utility memindahkan isi file teks ke tabel
	  //berguna untuk menambahkan data stopwords baru
	  //melakukan pengecekan, kalau ada duplikasi maka tidak dimasukkan
	
	  //file teks berformat kata=sinonim	
	  //struktur DB:
//		CREATE TABLE IF NOT EXISTS `sinonim` (
//				  `id_internal` bigint(20) NOT NULL AUTO_INCREMENT,
//				  `kata` varchar(100) DEFAULT NULL,
//				  `sinonim` varchar(100) DEFAULT NULL,
//				  PRIMARY KEY (`id_internal`),
//				  UNIQUE KEY `kata` (`kata`)
//				)	
	   System.out.println("filetodb");
       Connection conn=null;      
       PreparedStatement pSdhAda=null;        
       PreparedStatement pIns=null;
       jumTdkDiproses=0;
	   jumDiproses=0;
       try {
		        Class.forName("com.mysql.jdbc.Driver");
	            conn = DriverManager.getConnection  ("jdbc:mysql://"+dbName+"?user="+userName+"&password="+password);
	            pSdhAda = conn.prepareStatement     (" select id_internal from  "+ tableName + " where "+ fieldNameKata +" = ?");
	            pIns    =  conn.prepareStatement    (" insert into  "+ tableName + "("+fieldNameKata+","+fieldNameSinonim+") values (?,?)");  
	    
			 	FileInputStream fstream = new FileInputStream(fileName);
	            DataInputStream in = new DataInputStream(fstream);
	            BufferedReader br = new BufferedReader(new InputStreamReader(in));
	            String strLine;
	            ResultSet rs = null;
	            String delimiter = "=";
	            while ((strLine = br.readLine()) != null)   {
	                if (strLine.equals("")) {continue;}
	                String[] strData = strLine.split(delimiter);
	                //sudah ada di tabel?
	                pSdhAda.setString(1,strData[0].trim());
	                rs = pSdhAda.executeQuery();
	                if (rs.next()) {
	                   //sudah ada, batalkan masuk
	                	jumTdkDiproses++;	                    
	                } else {
	                	jumDiproses++;
	                	pIns.setString(1,strData[0].trim());
	                	pIns.setString(2,strData[1].trim());
	                	pIns.executeUpdate();
	                }
	            }
	        } catch (Exception e) {
	            logger.log(Level.SEVERE, null, e);         	
	        }
	        finally  {
	            try  {
	                      if (pSdhAda != null) {pSdhAda.close();}
	                      if (pIns != null)    {pIns.close();}
	                      if (conn != null) {conn.close();}				   
	                  } catch (Exception e) {
	                      logger.log(Level.SEVERE, null, e);
	             }
	        }
	}
	
	
	
	public void fileStopwordsToDB(String fileName,String tableName,String fieldName) {
		 //utility memindahkan isi file teks ke tabel
		 //berguna untuk menambahkan data stopwords baru
		 //melakukan pengecekan, kalau ada duplikasi maka tidak dimasukkan
		System.out.println("filetodbstopwords");
        Connection conn=null;      
        PreparedStatement pSdhAda=null;        
        PreparedStatement pIns=null;
        jumTdkDiproses=0;
		jumDiproses=0;
        try {
		        Class.forName("com.mysql.jdbc.Driver");
	            conn = DriverManager.getConnection  ("jdbc:mysql://"+dbName+"?user="+userName+"&password="+password);
	            pSdhAda = conn.prepareStatement     (" select id_internal from  "+ tableName + " where "+ fieldName +" = ?");
	            pIns    =  conn.prepareStatement    (" insert into  "+ tableName + "("+fieldName+") values (?)");  
	    
			 	FileInputStream fstream = new FileInputStream(fileName);
	            DataInputStream in = new DataInputStream(fstream);
	            BufferedReader br = new BufferedReader(new InputStreamReader(in));
	            String strLine;
	            ResultSet rs = null;
	            while ((strLine = br.readLine()) != null)   {
	                if (strLine.equals("")) {continue;}
	                //masuk ke tabel?
	                pSdhAda.setString(1,strLine);
	                rs = pSdhAda.executeQuery();
	                if (rs.next()) {
	                   //sudah ada, batalkan masuk
	                	jumTdkDiproses++;	                    
	                } else {
	                	jumDiproses++;
	                	pIns.setString(1,strLine);
	                	pIns.executeUpdate();
	                }
	            }
	        } catch (Exception e) {
	            logger.log(Level.SEVERE, null, e);         	
	        }
	        finally  {
	            try  {
	                      if (pSdhAda != null) {pSdhAda.close();}
	                      if (pIns != null)    {pIns.close();}
	                      if (conn != null) {conn.close();}				   
	                  } catch (Exception e) {
	                      logger.log(Level.SEVERE, null, e);
	             }
	        }
	 }
	
	
	 private void loadStopWords() {
 		    //memindahkan data stopwords dari tabel ke memori		 
			System.out.println("loadStopWords");
	        Connection conn=null;      
	        PreparedStatement pSel=null;
	        alStopWords.clear();
	        try {
	            Class.forName("com.mysql.jdbc.Driver");
	            conn = DriverManager.getConnection("jdbc:mysql://"+dbName+"?user="+userName+"&password="+password);
	            pSel  = conn.prepareStatement ("select id_internal,kata from stopwords");  
	            ResultSet rs = pSel.executeQuery();
	            jumDiproses = 0;
	            while (rs.next())  {
	            	String kata = rs.getString(2).trim();
	            	alStopWords.add(kata);
	            	//System.out.println(kata);
	            	jumDiproses++;
	            }
	        }
	        catch (Exception e) {
	            logger.log(Level.SEVERE, null, e);         	
	        }
	        finally  {
	            try  {
	                      if (pSel!= null) {pSel.close();}
	                      if (conn != null) {conn.close();}				   
	                  } catch (Exception e) {
	                      logger.log(Level.SEVERE, null, e);
	             }
	          }
	 }

	    private void loadSinonim() {
	        //memindahkan data dari tabel sinonim ke memori
	     	System.out.println("loadSinonim");
	        Connection conn=null;      
	        PreparedStatement pSel=null;
	        hmSinonim.clear();
	        try {
	            Class.forName("com.mysql.jdbc.Driver");
	            conn = DriverManager.getConnection("jdbc:mysql://"+dbName+"?user="+userName+"&password="+password);
	            pSel  = conn.prepareStatement ("select id_internal,kata,sinonim from sinonim");  
	            ResultSet rs = pSel.executeQuery();
	            jumDiproses = 0;
	            while (rs.next())  {
	            	String kata = rs.getString(2).trim();
	            	String sinonim = rs.getString(3).trim();
	            	hmSinonim.put(kata,sinonim);
	            	System.out.println(kata+"="+sinonim);
	            	jumDiproses++;
	            }
	        }
	        catch (Exception e) {
	            logger.log(Level.SEVERE, null, e);         	
	        }
	        finally  {
	            try  {
	                      if (pSel!= null) {pSel.close();}
	                      if (conn != null) {conn.close();}				   
	                  } catch (Exception e) {
	                      logger.log(Level.SEVERE, null, e);
	             }
	          }
	    }
	
	
	private String preproDasar(String strIn) {
		//satu char dibuang, url, dan casefolding
		//tergantung setting: buang hastag, mention, sinonim, prepro,
		//url pasti dibuang
		
		String msg;
		
		msg = strIn.toLowerCase();                //casefolding
        
		
		
		if (isBuangHashtag) {
        	msg = msg.replaceAll("#[\\w|:_]*", " ");  //buang #xxxx hashtag
        }
        if (isBuangMention) {
        	msg = msg.replaceAll("@[\\w|:_]*", " ");  //buang mention @xxxx
        }
        
        //buang URL
        msg = msg.replaceAll("http://[-A-Za-z0-9+&@#/%?=~_()|!:,.;]*[-A-Za-z0-9+&@#/%=~_()|]"," "); // buang url
		
        //giilaaa = gila
        if (isBuangCharBerurutan) {
			//System.out.println(msg);
        	msg = msg.replaceAll("([.!?^\\w])\\1{1,}", "$1");
        	//System.out.println("setelah "+ msg);
		}
		
        
        if ( (isProsesSinonim) || (isBuangStopwords) )  {
	        //System.out.println("sinonoim:"+msg);
        	Scanner sc = new Scanner(msg);
	        StringBuilder sb = new StringBuilder();
	        String w;
	        String w2;
	        while (sc.hasNext()) {
	            w = sc.next();
	            if (w.length()==1) {                   //hanya satu char? buang
	                continue;
	            }
	            
	            if (isProsesSinonim) {
		            w2 = hmSinonim.get(w);
		            if (w2!=null) {                     //ganti dengan sinonim
		                w = w2;
		            }
	            }
	            
	            if (isBuangStopwords) {
		            if (!alStopWords.contains(w)) {     //ada di stopwords? jangan dimasukkan
		                sb.append(w).append(" ");
		            }
	            } else {
	            	//masukan semua
	            	sb.append(w).append(" ");
	            }
	        }               
	        return sb.toString();
        } else {
        	return msg;
        }
	}
	
	
	public void init() {
		 if ( isBuangStopwords )  {
	        	loadStopWords();
	     } 
	        
	     if  ( isProsesSinonim )  {
	        	loadSinonim();
	     }
	}
	
	
	public void proses() {
		//init dipanggil lebih dulu (di luar loop)
		
		//setiap proses: 2000 record, lakukan berulang2 sampai jumdiProses = 0
		//level=1: dasar: casefolding, hilangkan url, mention, hashtag 
		
		// 
        //input:
		// tablename terisi table tweet_jadi (lihat output class ProsesTwMentah) 	
		// tweet disimpan di field text
		//output:
		//	field text_prepro:output hasil prepro 
		//	field is_prepro: flag ada akan diproses yg bernilai 0, dan setelah diproses bernilai 1 
		System.out.println("Mulai proses prepro");
        Connection conn=null;      
        PreparedStatement pUsr=null;        
        PreparedStatement pTw=null;
        PreparedStatement pFlag = null;
        
        
       
        
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://"+dbName+"?user="+userName+"&password="+password);
            conn.setAutoCommit(false);
            pUsr = conn.prepareStatement  ("update "+tableName+" set text_prepro = ? where id_internal = ?");
            pFlag = conn.prepareStatement ("update "+tableName+" set is_prepro = 1 where id_internal = ?");
            pTw  =  conn.prepareStatement (" select id_internal, text from "+ tableName +" where  is_prepro = 0 limit 0,2000");  
            
            
            ResultSet rsTw = pTw.executeQuery();
            jumDiproses = 0;
            while ( rsTw.next())  {
            	long idInternal = rsTw.getLong(1);
            	String tw = rsTw.getString(2);
            	jumDiproses++;
            	//proses disini
            	String strOut = preproDasar(tw);
            	//System.out.println(strOut);
            	pUsr.setString(1,strOut);
            	pUsr.setLong(2,idInternal);
                pUsr.executeUpdate();
                pFlag.setLong(1,idInternal);
                pFlag.executeUpdate();
            }
           // System.out.println("-------");
           // System.out.println("selesai");
        }
        catch (Exception e) {
        	//ROLLBACK
    	    logger.log(Level.SEVERE, null, e);
       		if (conn != null) {
               try {
					conn.rollback();
				} catch (SQLException e1) {
					logger.log(Level.SEVERE, null, e1);   
				}
               System.out.println("Connection rollback...");
           }         	
        }
        finally  {
            try  {
            		  conn.commit();
                      if (pUsr != null)  {pUsr.close();}      
                      if (pFlag != null) {pUsr.close();} 
                      if (pTw != null)   {pUsr.close();} 
                      if (conn != null) {conn.close();}				   
                  } catch (Exception e) {
                      logger.log(Level.SEVERE, null, e);
             }
        }
		
	}
	
	
	public void flagDuplicate() {
		
		//LAMA SEKALI PROSESNYA akibat sort? atau pembandingan? --> TBD cari cara untuk dipercepat. 
		//TBD --> tambahkan commit/rollback
		//memflag record yang duplikasi
		//tidak dihapus karena mungkin dibutuhkan
		//panggil process() terlebih dulu, agar field text_prepro terisi
		
		//tantangannya: tidak efisien jika menggunakan SQL--> order by text_prepro jika ukuran database besar
		//solusi: ambil sebagian-sebagian, sort, buang yang dobel
		//menggunakan flag is_duplicate_checked
		//mengisi is_duplicate dengan 1 jika ada duplikasi (nanti record ini bisa diabaikan dalam proses clustering, learning dst)
		
		System.out.println("Mulai menandai duplikasi tweet");
        Connection conn=null;      
        PreparedStatement pTw=null;
        PreparedStatement pFlag = null;
        PreparedStatement pDup = null;
        
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://"+dbName+"?user="+userName+"&password="+password);
            
            pFlag = conn.prepareStatement ("update "+tableName+" set is_duplicate_checked = 1 where id_internal = ?");
            pDup = conn.prepareStatement  ("update "+tableName+" set is_duplicate = 1 where id_internal = ?");
            
            pTw  =  conn.prepareStatement ("select id_internal,text_prepro from "+ tableName +" where is_duplicate_checked = 0 and text_prepro is not null order by text_prepro limit 0,500 ");  
            
            //harus disort!
            //kode ditasa punya resiko performance kalau di order dulu baru dilimint
            //idalnya mungkin menggunakan limit saja, diambil ke memori baru disort  <-- lebih kompleks
            
            String oldTw = "";
            ResultSet rsTw = pTw.executeQuery();
            jumDiproses = 0;
            while ( rsTw.next())  {
            	long idInternal = rsTw.getLong(1);
            	String tw = rsTw.getString(2).trim();
            	
            	
            	if (tw.equals(oldTw)) {
            		jumDiproses++;
            		//sama, beri flag duplicate
            		//System.out.println("Duplikasi:"+tw);
            		pDup.setLong(1,idInternal);	
            		pDup.executeUpdate();
            	} else {  //tidak sama
            		oldTw = tw;
            	}
                pFlag.setLong(1,idInternal);
                pFlag.executeUpdate();
            }
        }
        catch (Exception e) {
            logger.log(Level.SEVERE, null, e);         	
        }
        finally  {
            try  {
                      if (pFlag!= null) {pFlag.close();}
            	      if (pDup != null) {pDup.close();}                
                      if (conn != null) {conn.close();}				   
                  } catch (Exception e) {
                      logger.log(Level.SEVERE, null, e);
             }
          }
		
	}
	
	public void spamRemoval() {
	//memberikan flag kepada 
		
	}
	
	
/*
 * 
 * 
 * CREATE TABLE  `sinonim` (
  `id_internal` int(10) NOT NULL AUTO_INCREMENT,
  `kata` varchar(100) DEFAULT NULL,
  `sinonim` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id_internal`),
  UNIQUE KEY `kata` (`kata`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;	


CREATE TABLE  `stopwords` (
  `id_internal` int(10) NOT NULL AUTO_INCREMENT,
  `kata` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id_internal`),
  UNIQUE KEY `kata` (`kata`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



 */
	
	
	
	
	
	public static void main(String[] args) {
		//proses param 
    	String strDb=""; 
    	String strDbUser="";
    	String strDbPass="";
    	String strTableName="";
    	boolean isBuangMention=false;
    	boolean isBuangHashtag=false;
    	boolean isProsesSinonim=false;
    	boolean isBuangStopwords=false;
    	boolean isBuangCharBerurutan=false; //giiilaaa = gila
    	boolean isFlagDuplicate = true;
    	
    	
    	String strDelay="";
    	
    	String strParam;
    	
    	StringBuilder sb = new StringBuilder();
    	for (String str:args) {
    		sb.append(str);
    		sb.append(" ");
    	}
    	
    	strParam = sb.toString();
    	String[] arrParam = strParam.split("-");
    	
    	//lebih efisien pake group regex mungkin
    	String strLast="";
    	for  (String par:arrParam) {
    		try
    		{
	    		if (par.equals("")) continue;
    			String[] arrDetPar = par.split(" "); 
	    		if (arrDetPar[0].equals("db")) {
	    			strLast="db";
	    			strDb = arrDetPar[1]; 
	    		} else
	    		if (arrDetPar[0].equals("u")) {  //user
	    			strLast="u";
	    			strDbUser = arrDetPar[1]; 	    			
	    		} else 
	    		if (arrDetPar[0].equals("p")) {  //passwrd
	    			strLast="p";
	    			strDbPass = arrDetPar[1];
	    		} else
				if (arrDetPar[0].equals("tablename")) {
				    strLast="tablename";
				    strTableName = arrDetPar[1]; 	    			
				}  
	    		else
			    if (arrDetPar[0].equals("delay")) {
			    	strLast="delay";
			    	strDelay = arrDetPar[1]; 	    			
				} else  
				if (arrDetPar[0].equals("buangmention")) {
				    strLast="isBuangMention";
				    isBuangMention = true; 	    			
				} else
		    	if (arrDetPar[0].equals("sinonim")) {   
					 strLast="isProsesSinonim";
					 isProsesSinonim = true;
				} else
				if (arrDetPar[0].equals("buangstopwords")) { 
					 strLast="isBuangStopwords";
					 isBuangStopwords = true; 	    			
				} else	
				if (arrDetPar[0].equals("buanghashtag")) {
					 strLast="isBuangHashtag";
					 isBuangHashtag = true;  	    			
				} else
				if (arrDetPar[0].equals("flagduplicate")) {
						 strLast="isFlagDuplicate";
						 if (arrDetPar[1].equals("yes")) {
							 isFlagDuplicate = true;
						 } else 
					     if (arrDetPar[1].equals("no")) {
							 isFlagDuplicate = false;
						 }  else {
							 System.out.println("flagduplicate harus berisi yes or no ");
						 }
						   	    			
				} else 
				if (arrDetPar[0].equals("buangcharberurutan")) {
						 strLast="isBuangCharBerurutan";
						 isBuangCharBerurutan = true;  	    			
				} else
	    		{
	    			//parameter tidak dikenal
	    			System.out.println("Error parameter tdk dikenal : "+par);
	    			System.exit(1);
	    		}
    		}
    		catch (Exception e) {
    			System.out.println("parameter salah pada bagian:" +strLast+" Gunakan parameter dengan format:");
    			System.out.println("-db databasename -u dbusername -p dbpassword -tablename namatabel -buangcharberurutan -flagduplicate yes_or_no -delay delay_antar_query_dlm_detik -buangmention -sinonim  -buangstopwords -buanghashtag");
    			System.out.println("Contoh:");
    			System.out.println("G:\\LibTweetMining2\\bin>java -classpath .;../libs/mysql-connector-java-5.0.8-bin.jar;../libs/jackson-all-1.9.9.jar edu.upi.cs.tweetmining.PreproDB -db localhost/obamarumor -u yudi3 -p rahasia  -tablename tw_jadi -buangcharberurutan -flagduplicate yes -delay 10 -buangmention -sinonim  -buangstopwords -buanghashtag");
    			System.exit(1);
    		}
    	}
    	
    	if (strDb.equals("") || strDbUser.equals("") || strDbPass.equals("")) {
    		System.out.println("parameter salah pada bagian:" +strLast+" Gunakan parameter dengan format:");
			System.out.println("-db databasename -u dbusername -p dbpassword -tablename namatabel -buangcharberurutan -flagduplicate yes_or_no -delay delay_antar_query_dlm_detik -buangmention -sinonim  -buangstopwords -buanghashtag");
			System.out.println("Contoh:");
			System.out.println("G:\\LibTweetMining2\\bin>java -classpath .;../libs/mysql-connector-java-5.0.8-bin.jar;../libs/jackson-all-1.9.9.jar edu.upi.cs.tweetmining.PreproDB -db localhost/obamarumor -u yudi3 -p rahasia  -tablename tw_jadi -buangcharberurutan -flagduplicate yes -delay 10 -buangmention -sinonim  -buangstopwords -buanghashtag");
			System.exit(1);
    	}
    	
    	if (strDelay.equals("")) {
    		strDelay = "60";}
    	
    	System.out.println("db-->"+strDb);
    	System.out.println("dbuser-->"+strDbUser);
    	System.out.println("dbpass-->"+strDbPass);
    	System.out.println("tablename-->"+strTableName);
    	System.out.println("delay-->"+strDelay);
    	System.out.println("buangmention-->"+isBuangMention);
    	System.out.println("buanghashtag-->"+isBuangHashtag);
    	System.out.println("buangstopwords-->"+isBuangStopwords);
    	System.out.println("proses sinonim-->"+isProsesSinonim);
    	
		
		
    	//System.exit(1); //debug
		
		
		PreproDB pdb = new PreproDB();
		pdb.tableName =  strTableName;
		pdb.dbName = strDb;
    	pdb.userName = strDbUser;
    	pdb.password = strDbPass;
    	pdb.isBuangMention=isBuangMention;
		pdb.isBuangHashtag=isBuangHashtag;
		pdb.isProsesSinonim=isProsesSinonim;
		pdb.isBuangStopwords=isBuangStopwords;
		pdb.isBuangCharBerurutan = isBuangCharBerurutan;
		pdb.init();
		int delay = Integer.parseInt(strDelay); //delay antar query dalam detik
		for (int i=0;;i++) {
            System.out.println("Proses ke---------------------------------->"+i);
            pdb.proses();
            System.out.println("Jumlah diproses="+pdb.jumDiproses);
            if (isFlagDuplicate) {
            	pdb.flagDuplicate();
            	System.out.println("Jumlah duplikasi="+pdb.jumDiproses);
            }
            try {
            	System.out.println("Tidur:"+delay+" detik");
            	Thread.sleep(delay*1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				System.out.println("thread sleep error");
				e.printStackTrace();
			} 
		}
//		pdb.dbName = "localhost/obama2";
//		pdb.userName = "yudi3";
//		pdb.password = "rahasia";
//		pdb.tableName="tw_jadi";
//		pdb.isBuangMention=true;
//		pdb.isBuangHashtag=false;
//		pdb.isProsesSinonim=false;
//		pdb.isBuangStopwords=true;
//		pdb.proses();
//		System.out.println("Jumlah diproses="+pdb.jumDiproses);
		
//		pdb.flagDuplicate();
//    	System.out.println("Jumlah duplikasi="+pdb.jumDiproses);
		
		
//		PreproDB pdb = new PreproDB(); 		
//		pdb.dbName = "localhost/masterchef";
//		pdb.userName = "yudi3";
//		pdb.password = "rahasia";
//		pdb.isBuangMention=false;
//		pdb.isBuangHashtag=false;
//		pdb.isProsesSinonim=true;
//		pdb.isBuangStopwords=true;
//		pdb.proses();
//		System.out.println("Jumlah diproses="+pdb.jumDiproses);
		

		
		
////      sinonim
//		PreproDB pdb = new PreproDB(); 		
//		pdb.dbName = "localhost/masterchef";
//		pdb.userName = "yudi3";
//		pdb.password = "rahasia";
//		pdb.fileSinonimToDB("G:\\eksperimen\\stopwords_sinonim_agt2012\\smiley.txt","sinonim","kata","sinonim");
//		System.out.println("Jumlah diproses="+pdb.jumDiproses);
//		System.out.println("Jumlah duplikasi="+pdb.jumTdkDiproses);
		

		
//		//file teks ke DB (stopwords)
//		PreproDB pdb = new PreproDB(); 		
//		pdb.dbName = "localhost/masterchef";
//		pdb.userName = "yudi3";
//		pdb.password = "rahasia";
//		pdb.fileStopwordsToDB("G:\\eksperimen\\stopwords_sinonim_agt2012\\catatan_stopwords.txt","stopwords","kata");
//		//pdb.fileToDB("G:\\eksperimen\\stopwords_sinonim_agt2012\\catatan_kata_sinonim.txt","stopwords","kata");
//		System.out.println("Jumlah diproses="+pdb.jumDiproses);
//		System.out.println("Jumlah duplikasi="+pdb.jumTdkDiproses);
    	
		
		
		
		
    	//pdb.flagDuplicate();
    	//System.out.println("Jumlah duplikasi="+pdb.jumDiproses);
		
		
		//testing class
//		PreproDB pdb = new PreproDB(); 
//		pdb.dbName = "localhost/indosattelkomsel";
//		pdb.userName = "yudi3";
//		pdb.password = "rahasia";
//    	pdb.tableName = "tw_jadi";
//		pdb.processJam(1);		
//    	System.out.println("Jumlah diproses="+pdb.jumDiproses);
    	
    	
    	//pdb.flagDuplicate();
    	//System.out.println("Jumlah duplikasi="+pdb.jumDiproses);
		
	}

}

/*

update tw_jadi_sandyhoax set is_prepro = 0;

CREATE TABLE IF NOT EXISTS `stopwords` (
  `id_internal` int(10) NOT NULL AUTO_INCREMENT,
  `kata` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id_internal`),
  UNIQUE KEY `kata` (`kata`)
) ENGINE=InnoDB AUTO_INCREMENT=185 DEFAULT CHARSET=utf8;

INSERT INTO `stopwords` (`id_internal`, `kata`) VALUES
	(11, 'a'),
	(12, 'about'),
	(13, 'above'),
	(14, 'after'),
	(15, 'again'),
	(16, 'against'),
	(17, 'all'),
	(18, 'am'),
	(19, 'an'),
	(20, 'and'),
	(21, 'any'),
	(22, 'are'),
	(23, 'aren\'t'),
	(24, 'as'),
	(25, 'at'),
	(26, 'be'),
	(27, 'because'),
	(28, 'been'),
	(29, 'before'),
	(30, 'being'),
	(31, 'below'),
	(32, 'between'),
	(33, 'both'),
	(34, 'but'),
	(35, 'by'),
	(36, 'can\'t'),
	(37, 'cannot'),
	(38, 'could'),
	(39, 'couldn\'t'),
	(40, 'did'),
	(41, 'didn\'t'),
	(42, 'do'),
	(43, 'does'),
	(44, 'doesn\'t'),
	(45, 'doing'),
	(46, 'don\'t'),
	(47, 'down'),
	(48, 'during'),
	(49, 'each'),
	(50, 'few'),
	(51, 'for'),
	(52, 'from'),
	(53, 'further'),
	(54, 'had'),
	(55, 'hadn\'t'),
	(56, 'has'),
	(57, 'hasn\'t'),
	(58, 'have'),
	(59, 'haven\'t'),
	(60, 'having'),
	(61, 'he'),
	(62, 'he\'d'),
	(63, 'he\'ll'),
	(64, 'he\'s'),
	(65, 'her'),
	(66, 'here'),
	(67, 'here\'s'),
	(68, 'hers'),
	(69, 'herself'),
	(70, 'him'),
	(71, 'himself'),
	(72, 'his'),
	(73, 'how'),
	(74, 'how\'s'),
	(75, 'i'),
	(76, 'i\'d'),
	(77, 'i\'ll'),
	(78, 'i\'m'),
	(79, 'i\'ve'),
	(80, 'if'),
	(81, 'in'),
	(82, 'into'),
	(83, 'is'),
	(84, 'isn\'t'),
	(85, 'it'),
	(86, 'it\'s'),
	(87, 'its'),
	(88, 'itself'),
	(89, 'let\'s'),
	(90, 'me'),
	(91, 'more'),
	(92, 'most'),
	(93, 'mustn\'t'),
	(94, 'my'),
	(95, 'myself'),
	(96, 'no'),
	(97, 'nor'),
	(98, 'not'),
	(99, 'of'),
	(100, 'off'),
	(101, 'on'),
	(102, 'once'),
	(103, 'only'),
	(104, 'or'),
	(105, 'other'),
	(106, 'ought'),
	(107, 'our'),
	(108, 'ours'),
	(109, 'ourselves'),
	(110, 'out'),
	(111, 'over'),
	(112, 'own'),
	(113, 'same'),
	(114, 'shan\'t'),
	(115, 'she'),
	(116, 'she\'d'),
	(117, 'she\'ll'),
	(118, 'she\'s'),
	(119, 'should'),
	(120, 'shouldn\'t'),
	(121, 'so'),
	(122, 'some'),
	(123, 'such'),
	(124, 'than'),
	(125, 'that'),
	(126, 'that\'s'),
	(127, 'the'),
	(128, 'their'),
	(129, 'theirs'),
	(130, 'them'),
	(131, 'themselves'),
	(132, 'then'),
	(133, 'there'),
	(134, 'there\'s'),
	(135, 'these'),
	(136, 'they'),
	(137, 'they\'d'),
	(138, 'they\'ll'),
	(139, 'they\'re'),
	(140, 'they\'ve'),
	(141, 'this'),
	(142, 'those'),
	(143, 'through'),
	(144, 'to'),
	(145, 'too'),
	(146, 'under'),
	(147, 'until'),
	(148, 'up'),
	(149, 'very'),
	(150, 'was'),
	(151, 'wasn\'t'),
	(152, 'we'),
	(153, 'we\'d'),
	(154, 'we\'ll'),
	(155, 'we\'re'),
	(156, 'we\'ve'),
	(157, 'were'),
	(158, 'weren\'t'),
	(159, 'what'),
	(160, 'what\'s'),
	(161, 'when'),
	(162, 'when\'s'),
	(163, 'where'),
	(164, 'where\'s'),
	(165, 'which'),
	(166, 'while'),
	(167, 'who'),
	(168, 'who\'s'),
	(169, 'whom'),
	(170, 'why'),
	(171, 'why\'s'),
	(172, 'with'),
	(173, 'won\'t'),
	(174, 'would'),
	(175, 'wouldn\'t'),
	(176, 'you'),
	(177, 'you\'d'),
	(178, 'you\'ll'),
	(179, 'you\'re'),
	(180, 'you\'ve'),
	(181, 'your'),
	(182, 'yours'),
	(183, 'yourself'),
	(184, 'yourselves');
	(184, 'rt');

*/