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
	
	try {
	  ArrayList<Long> alStatParent  = new ArrayList<Long>();
      Class.forName("com.mysql.jdbc.Driver");
      String strCon = "jdbc:mysql://"+dbName+"?user="+userName+"&password="+password;
      System.out.println(strCon);
      conn = DriverManager.getConnection(strCon);
      conn.setAutoCommit(false);
      
      //pTw  =  conn.prepareStatement ("select id_internal,text,is_dispute,sanggahan_dukungan,to_user_id,to_user_name,in_reply_to_status_id from "+ tabelTweetJadi +" where in_reply_to_status_id is not null");
      pParent =conn.prepareStatement ("select in_reply_to_status_id from "+ tabelTweetJadi +" where in_reply_to_status_id is not null and is_dispute=1");
      pTw = conn.prepareStatement    ("select id_internal,text,sanggahan_dukungan from "+ tabelTweetJadi +" where id = ?");
      pChild= conn.prepareStatement  ("select id_internal,text,sanggahan_dukungan,to_user_id,to_user_name,in_reply_to_status_id from "+ tabelTweetJadi +" where in_reply_to_status_id =? and is_dispute=1");
      
      PrintWriter pw = new PrintWriter(namaFileOut);
      
      
      ResultSet rsParent = pParent.executeQuery();
      
      //cari parent
      int jumParent=0;
      while (rsParent.next())   {                           
    	  long statParent     = rsParent.getLong(1);            //idstatus
    	  alStatParent.add(statParent); 
    	  jumParent++;
      }
      pw.println("Jum parent:"+jumParent);
      
      //cari child
      int jumParentAda=0;
      for  (long statParent:alStatParent) {
    	  //pw.println("id parent:"+statParent);
    	  pTw.setLong(1, statParent);
    	  ResultSet rsTw = pTw.executeQuery();
    	  if (rsTw.next()) {
    		  jumParentAda++;
    		  pw.println("----------");
    		  pw.println("parent:"+rsTw.getString(2));
    		  pw.println("sanggahan_dukungan (1:menyanggah,2:mendukung,3:bertanya):"+rsTw.getInt(3));
    		  //print child
    		  pChild.setLong(1, statParent);
        	  ResultSet rsChild = pChild.executeQuery();
        	  pw.println("child:");
        	  while (rsChild.next())  {
        		  pw.println(rsChild.getString(2));
        		  pw.println("sanggahan_dukungan:"+rsChild.getInt(3));
        		  pw.println("");
        	  }
    	  }
      }
      
      pw.println("Jum parent ada:"+jumParentAda);
      pw.println("Jum parent tdk ada:"+(jumParent-jumParentAda));
      pw.close();
      System.out.println("selesai");
	} catch (Exception e) {
		logger.log(Level.SEVERE, null, e);
	} finally {
        
		try {
            pChild.close();
            pParent.close();
            pTw.close();
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
	ra.namaFileOut="D:\\xampp\\htdocs\\obama\\obama_2000_reply.txt";
	ra.process();
}

}
