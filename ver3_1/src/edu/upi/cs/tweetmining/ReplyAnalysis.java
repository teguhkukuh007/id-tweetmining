package edu.upi.cs.tweetmining;


import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;


public class ReplyAnalysis {
/* menampilkan tweet reply dalam bentuk tree*/
	
public String namaFileOut;	
public String dbName;
public String userName;
public String password;
public String tabelTweetJadi;
private final  Logger logger = Logger.getLogger("Proses tweet mentah ");

public  void process() {
	Connection conn=null;       
	PreparedStatement pChild = null;
	PreparedStatement pTw = null;
	PreparedStatement pParent = null;
	PreparedStatement pChildberdTw = null;
	PreparedStatement pChildberdTwAda = null;
	PreparedStatement pAll = null;
	
	try {
	  ArrayList<Long> alStatParent  = new ArrayList<Long>();
      Class.forName("com.mysql.jdbc.Driver");
      String strCon = "jdbc:mysql://"+dbName+"?user="+userName+"&password="+password;
      System.out.println(strCon);
      conn = DriverManager.getConnection(strCon);
      conn.setAutoCommit(false);
      
      // idealnya spt ini, tapi super lambat querynya
      // select id_internal,id from tw_jadi where is_dispute=1 and id not in 
      //		  (select in_reply_to_status_id from tw_jadi where is_dispute=1 and in_reply_to_status_id is not null )
      
      //jadi ambil semua, 
      
      pAll =   conn.prepareStatement ("select id,id_internal,text,sanggahan_dukungan from "+ tabelTweetJadi +" where is_dispute=1");
      
      
      //cari id tweeet parent
      //dan direfer oleh in_reply_status_id
      pParent =conn.prepareStatement ("select in_reply_to_status_id from "+ tabelTweetJadi +" " +
      		                          " where in_reply_to_status_id is not null and is_dispute=1");
      
      //ambil data tweet berdasarkan id
      pTw = conn.prepareStatement    ("select id_internal,text,sanggahan_dukungan from "+ 
                                       tabelTweetJadi +" where id = ?");
      
      //ambil data child
      pChild= conn.prepareStatement  ("select id_internal,text,sanggahan_dukungan,to_user_id,to_user_name,in_reply_to_status_id from "+ 
                                       tabelTweetJadi +" where in_reply_to_status_id =? and is_dispute=1");
      
      
      
      //child berdasarkan string tweet, bukan in_reply_to_status
      pChildberdTw = conn.prepareStatement  ("select id_internal,text,sanggahan_dukungan,id from "+ tabelTweetJadi +" where text like ? and id<>?");
      
      //cek apakah ada
      pChildberdTwAda = conn.prepareStatement("select count(*) from "+ tabelTweetJadi +" where text like ? and id<>?");
      
      PrintWriter pw = new PrintWriter(namaFileOut);
      
      ResultSet rsTw;
      ResultSet rsChild;
      ResultSet rsChildberdTw;
      ResultSet rsChildberdTwAda;
      ResultSet rsAll;
      
      ResultSet rsParent = pParent.executeQuery();
      
      //cari parent, simpan idnya di arrayList
      int jumParent=0;
      while (rsParent.next())   {                           
    	  long statParent = rsParent.getLong(1);            //idstatus
    	  alStatParent.add(statParent); 
    	  jumParent++;
      }
//      pw.println("Jum total parent:"+jumParent);
//      pw.println("Keterangan angka dalam kurung siku: 1:menyanggah hoax,2:mendukung hoax,3:bertanya");

      System.out.println("Tahap 1, cari berdasarkan in_reply_status_id");      
      
      //cari child yang merefer parent
   //   int jumParentAda=0;
      for  (long statParent:alStatParent) {
    	  System.out.println("id parent:"+statParent);
    	  pTw.setLong(1, statParent); //ambil detil parent
    	  rsTw = pTw.executeQuery();
    	  if (rsTw.next()) {
    	//	  jumParentAda++;  
    		  //print parent
    		  pw.println("----------");
    		  String strTwParent = rsTw.getString(2);
    		  pw.println("Parent:"+strTwParent+" ["+rsTw.getInt(3)+"]");
    		  
    		  //cari dan print child
    		  pChild.setLong(1, statParent);
        	  rsChild = pChild.executeQuery();
        	  pw.println("Childreen:");
        	  System.out.println("Childreen:");
        	  //pw.println("child berdarkan in_reply_status_id:");
        	  int jumNetral   =0; //0
        	  int jumSanggah  =0; //1
        	  int jumDukung   =0; //2
        	  int jumTanya    =0; //3
        	  
        	  while (rsChild.next())  {
        	  	  int res = rsChild.getInt(3);
        	  	  if (res == 1) {
        	  	     jumSanggah++;
        	  	  }	else
        	  	  if (res == 2)  {
        	  	  	 jumDukung ++;
        	  	  } else 
        	  	  if (res == 3) {
        	  	     jumTanya++;
        	  	  } else {
        	  		  jumNetral++;
        	  	  }
        		  pw.println(rsChild.getString(2)+" ["+rsChild.getInt(3)+"]");
        		  //pw.println("");
        	  }
        	  //pw.println("Jumlah child sanggah:"+ jumSanggah +" ; dukung:"+ jumDukung + " tanya:"+jumTanya+" netral:"+jumNetral);
        	  
        	  //cari yang in_reply_status_id tidak cocok tapi merupakan RT
        	  //ambil 30 karakter untuk dicocokkan
        	  
        	  //System.out.print(strTwParent+"-->");
        	  String strSample; 
        	  if (strTwParent.length()>30) { 
        		  strSample = strTwParent.substring(0,30);
        	  }  else {
        		  strSample = strTwParent;
        	  }
        	  //System.out.println(strTwParent+"-->"+strSample);
        	  
        	  //ambil tweet yang mengandung potongan, tapi bukan tweet parent (dirinya sendiri)
        	  pChildberdTw.setString(1,"%"+strSample+"%");
        	  pChildberdTw.setLong(2,statParent);
        	  rsChildberdTw = pChildberdTw.executeQuery();
        	  pw.println("child berdasarkan tweet:");
        	  boolean isAda=false;
//        	  jumSanggah =0; //1  jangan direset karena akan ditotal
//        	  jumDukung  =0; //2
//        	  jumTanya   =0; //3
//        	  jumNetral  =0;
        	  while (rsChildberdTw.next())  {
        		  int res = rsChildberdTw.getInt(3);
        		  if (res == 1) {
         	  	     jumSanggah++;
         	  	  }	else
         	  	  if (res == 2)  {
         	  	  	 jumDukung ++;
         	  	  } else 
         	  	  if (res == 3) {
         	  	     jumTanya++;
         	  	  } else {
         	  		 jumNetral++;
         	  	  }
        		  isAda = true;
        		  pw.println(rsChildberdTw.getString(2)+" ["+rsChildberdTw.getInt(3)+"]");
        		  //pw.println("");
        	  }
        	  if (isAda) {
        		  pw.println("Jumlah child sanggah:"+ jumSanggah +" ; dukung:"+ jumDukung + " tanya:"+jumTanya+" netral:"+jumNetral);
        		  pw.println("");
        		  System.out.println("Jumlah child sanggah:"+ jumSanggah +" ; dukung:"+ jumDukung + " tanya:"+jumTanya+" netral:"+jumNetral);
        	  }
        	  else  {
        		  pw.println("-NA-");
        	  }
    	  }
      }
      
      pw.flush();
      
//      pw.println("Jum parent ada:"+jumParentAda);
//      pw.println("Jum parent tdk ada:"+(jumParent-jumParentAda));
      
      pw.println("cek semua, tidak berdasarkan reply_status_id");
      System.out.println("Mulai tahap dua");
      //cari semua data
      //perkecualian adalah yang sudah diproses diatas
      
      //select id,id_internal,text from "+ tabelTweetJadi +" where is_dispute=1
      rsAll = pAll.executeQuery();
      String tw;
      String twPotong;
      ArrayList<Long> alIdChild  = new ArrayList<Long>();
      while (rsAll.next())   { 
    	  //cari yang belum tercover sebagai parent
    	  long id = rsAll.getLong(1);     	  
    	  if (alStatParent.contains(id)||(alIdChild.contains(id))) {
    		 //sudah ada sebagai parent atau sudah ada sebagai child: skip
    		 System.out.print("#");
    	  } else {
    		  System.out.print(".");
    		  //belum ada, proses
    		  //potong 30 karakter didepan
        	  tw = rsAll.getString(3); 
        	  if (tw.length()>30) { 
        		  twPotong = tw.substring(0,30);
        	  }  else {
        		  twPotong = tw;
        	  }
        	  //ambil tweet yang mengandung potongan, tapi bukan tweet parent (dirinya sendiri)
        	  //select id_internal,text,sanggahan_dukungan from "+ tabelTweetJadi +" where text like ? and id<>?"
        	  //pChildberdTw = conn.prepareStatement  ("select id_internal,text,sanggahan_dukungan,id from "+ tabelTweetJadi +" where text like ? and id<>?");
        	  //cek dulu apakah ada
        	  pChildberdTwAda.setString(1,"%"+twPotong+"%");
        	  pChildberdTwAda.setLong(2,id);
        	  rsChildberdTwAda = pChildberdTwAda.executeQuery();
        	  rsChildberdTwAda.next(); //pasti ada hasil
        	  if (rsChildberdTwAda.getInt(1) > 0) { 
        		  //ada child, print parent ambil detilnya        		  
        		  //print parent
        		  pw.println("parent:"+tw+" ["+rsAll.getInt(4)+"]");
        		  System.out.println("parent:"+tw+" ["+rsAll.getInt(4)+"]");
	        	  pChildberdTw.setString(1,"%"+twPotong+"%");
	        	  pChildberdTw.setLong(2,id);
	        	  rsChildberdTw = pChildberdTw.executeQuery();
	        	  //ambil child
	        	  //pw.println("Childreen:");
	        	  System.out.println("Childreen:");
	        	  int jumSanggah =0; //1
	        	  int jumDukung  =0; //2
	        	  int jumTanya   =0; //3
	        	  int jumNetral  =0;
	        	  while (rsChildberdTw.next())  {
	        		  long idChild = rsChildberdTw.getLong(4);
	        		  if (alIdChild.contains(idChild)) {
	        			  //sudah ada skip
	        		  } else {
	        			  int res = rsChildberdTw.getInt(3);
	        			  if (res == 1) {
	             	  	     jumSanggah++;
	             	  	  }	else
	             	  	  if (res == 2)  {
	             	  	  	 jumDukung ++;
	             	  	  } else 
	             	  	  if (res == 3) {
	             	  	     jumTanya++;
	             	  	  } else {
	             	  		  jumNetral++;
	             	  	  }
		        		  alIdChild.add(idChild);  //id disimpan 
		        		  System.out.println(rsChildberdTw.getString(2));
		        		  pw.println(rsChildberdTw.getString(2)+" ["+rsChildberdTw.getInt(3)+"]");
//		        		  pw.println("");
	        		  }  
	        	  } //end while
	        	  pw.println("Jumlah child sanggah:"+ jumSanggah +" ; dukung:"+ jumDukung + " tanya:"+jumTanya+" netral:"+jumNetral);
	        	  pw.println("");
	        	  System.out.println("Jumlah child sanggah:"+ jumSanggah +" ; dukung:"+ jumDukung + " tanya:"+jumTanya+" netral:"+jumNetral);
	        	  pw.flush();
        	  } //end if ada	          	  
    	  }
    	  
      }
      pw.close();
      System.out.println("selesai");
	} catch (Exception e) {
		logger.log(Level.SEVERE, null, e);
	} finally {
        
		try {
            pChild.close();
            pParent.close();
            pTw.close();
            pChildberdTw.close();
            conn.close();
        } catch (Exception e) {
            logger.log(Level.SEVERE, null, e);
        }    
    }
}

public static void main(String[] args) {
	ReplyAnalysis ra = new ReplyAnalysis();
	ra.dbName = "localhost/obama2";
	ra.userName = "yudi3";
	ra.password = "rahasia";
	ra.tabelTweetJadi = "tw_jadi";
	ra.namaFileOut="G:\\obama_2000_reply_lengkap.txt";
	ra.process();
}

}
