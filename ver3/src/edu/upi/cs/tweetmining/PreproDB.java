/*
 *  Copyright (C) 2012 yudi wibisono (yudi1975@gmail.com/cs.upi.edu)
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
 *     preprocessing tapi dilakukan di tabel
 *     
 *     
 *     
 *     
 *     
 *     
//query untuk menambahkan field yang dibutuhkan untuk prepro

alter table tw_jadi 
add text_prepro varchar(200) NOT NULL,
add is_prepro int(11) NOT NULL DEFAULT 0,
add KEY is_prepro (is_prepro);

     
 */

public class PreproDB {
	private static final Logger logger = Logger.getLogger("Prepodb");
	
    private HashMap<String,String> hmSinonim = new HashMap<String,String>();  //pasangan kata sinonim
    private ArrayList<String> alStopWords = new ArrayList<String>();      //kata stopwordss
	public int jumTdkDiproses=0;
	public int jumDiproses=0;
	public String dbName;           // format: localhost/mydbname
	public String userName;
	public String password;
	private final String tableName="tw_jadi";        // table tweet_jadi (lihat output class ProsesTwMentah) 
	                                	      //tweet disimpan di field text, output prepro akan disimpan di field text_prepro, 
										      //flag difield is_prepro  (0 belum diprepro, 1 sudah) <-- jgn lupa diindex
	
	
	public boolean isBuangMention=false;
	public boolean isBuangHashtag=false;
	public boolean isProsesSinonim=true;
    public boolean isBuangStopwords=true;
	
	public PreproDB() {
		 Handler consoleHandler = new ConsoleHandler();
		 logger.addHandler(consoleHandler);  
	}
	
	
	public void fileSinonimToDB(String fileName,String tableName,String fieldNameKata, String fieldNameSinonim) {   //sinonim, smiley
	  //utility memindahkan isi file teks ke tabel
	  //berguna untuk menambahkan data stopwords baru
	  //melakukan pengecekan, kalau ada duplikasi maka tidak dimasukkan
	
	  //file teks berformat kata=sinonim	
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
	        alStopWords.clear();
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
		
		String msg;
		
		msg = strIn.toLowerCase();                //casefolding
        
		
		if (isBuangHashtag) {
        	msg = msg.replaceAll("#[\\w|:_]*", " ");  //buang #xxxx hashtag
        }
        if (isBuangMention) {
        	msg = msg.replaceAll("@[\\w|:_]*", " ");  //buang mention @xxxx
        }
        
        msg = msg.replaceAll("http://[-A-Za-z0-9+&@#/%?=~_()|!:,.;]*[-A-Za-z0-9+&@#/%=~_()|]"," "); // buang url
		
        
        if ( (isProsesSinonim) || (isBuangStopwords) )  {
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
	            }
	        }               
	        return sb.toString();
        } else {
        	return msg;
        }
	}
	
	
	
	
	
	public void proses() {
		//setiap proses: 2000 record, lakukan berulang2 sampai jumdiProses = 0
		//level=1: dasar: casefolding, hilangkan url, mention, hashtag 
		
		// 
        //input:
		// tablename terisi table tweet_jadi (lihat output class ProsesTwMentah) 	
		// tweet disimpan di field text
		//output:
		//	field text_prepro:output hasil prepro 
		//	field is_prepro: flag ada akan diproses yg bernilai 0, dan setelah diproses bernilai 1 
		System.out.println("Prepro");
        Connection conn=null;      
        PreparedStatement pUsr=null;        
        PreparedStatement pTw=null;
        PreparedStatement pFlag = null;
        
        
        if ( isBuangStopwords )  {
        	loadStopWords();
        } 
        
        if  ( isProsesSinonim )  {
        	loadSinonim();
        }
        
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://"+dbName+"?user="+userName+"&password="+password);
            conn.setAutoCommit(false);
            pUsr = conn.prepareStatement  ("update "+tableName+" set text_prepro = ? where id_internal = ?");
            pFlag = conn.prepareStatement ("update "+tableName+" set is_prepro = 1 where id_internal = ?");
            pTw  =  conn.prepareStatement (" select id_internal, text from "+ tableName +" where is_prepro = 0 limit 0,2000");  
            
            
            ResultSet rsTw = pTw.executeQuery();
            jumDiproses = 0;
            while ( rsTw.next())  {
            	long idInternal = rsTw.getLong(1);
            	String tw = rsTw.getString(2);
            	jumDiproses++;
            	//proses disini
            	String strOut = preproDasar(tw);
            	System.out.println(strOut);
            	pUsr.setString(1,strOut);
            	pUsr.setLong(2,idInternal);
                pUsr.executeUpdate();
                pFlag.setLong(1,idInternal);
                pFlag.executeUpdate();
                conn.commit();
            }
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
		//mengisi is_duplicate dengan 1 jika ada duplikasi (nanti record ini diabaikan dalam proses clustering, learning dst)
		
		System.out.println("Prepro");
        Connection conn=null;      
        PreparedStatement pTw=null;
        PreparedStatement pFlag = null;
        PreparedStatement pDup = null;
        
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://"+dbName+"?user="+userName+"&password="+password);
            
            pFlag = conn.prepareStatement ("update "+tableName+" set is_duplicate_checked = 1 where id_internal = ?");
            pDup = conn.prepareStatement  ("update "+tableName+" set is_duplicate = 1 where id_internal = ?");
            
            pTw  =  conn.prepareStatement ("select id_internal,text_prepro from "+ tableName +" where is_duplicate_checked = 0 order by text_prepro limit 0,50 ");  
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
            		System.out.println("Duplikasi:"+tw);
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
		//query untuk tambah field yang dibutuhkan
		
		
		PreproDB pdb = new PreproDB(); 		
		pdb.dbName = "localhost/obama";
		pdb.userName = "yudi3";
		pdb.password = "rahasia";
//		pdb.isBuangMention=false;
//		pdb.isBuangHashtag=false;
//		pdb.isProsesSinonim=false;
//		pdb.isBuangStopwords=true;
//		pdb.proses();
//		System.out.println("Jumlah diproses="+pdb.jumDiproses);
		
		pdb.flagDuplicate();
    	System.out.println("Jumlah duplikasi="+pdb.jumDiproses);
		
		
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
