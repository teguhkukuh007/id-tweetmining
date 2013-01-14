package edu.upi.cs.tweetmining;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TweetToArff {
	/**
	 *
	 * yudi@upi.edu
	 * 
	 * memproses tweet jadi yang ada di DB menjadi file teks
	 * (bagaimana mendapatkan tweet jadi: TwCrawler --> tweet mentah di db --> ProsesTwMentahDB --> tweet jadi di db)
	 * 
	 * lihat method main() untuk cara penggunaan 
	 * 
	 * 
	 * 
	 */
	
	private final  Logger logger = Logger.getLogger("tweettoarff");
	private PreparedStatement pIsTweet;
	public String dbName;
	public String userName;
	public String password;
	
	public void process() {
		   Connection conn=null;       
	       PreparedStatement pTw = null;
	       try {
	           Class.forName("com.mysql.jdbc.Driver");
	           
	           String strCon = "jdbc:mysql://"+dbName+"?user="+userName+"&password="+password;
	           System.out.println(strCon);
	           conn = DriverManager.getConnection(strCon);
	           pTw  =  conn.prepareStatement ("select id_internal,content from where status = 0");   
	           conn.commit();
	       } 
	       catch (Exception e)
	       {
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
	       finally {
	           try {
//	               pInsertMen.close();
//	               pInsertUrl.close();
//	               pInsertHt.close();
//	               pInsertTw.close();
//	               pInsertMedia.close();
	               pIsTweet.close();
//	               pFlagTwMentah.close();
	               pTw.close();
	               conn.setAutoCommit(true);
	               conn.close();
	           } catch (Exception e) {
	               logger.log(Level.SEVERE, null, e);
	           }    
	       }
	           
	}
	public static void main(String[] args) {
		//ambil data dari db
		//tulis ke file teks
		
	}
}
