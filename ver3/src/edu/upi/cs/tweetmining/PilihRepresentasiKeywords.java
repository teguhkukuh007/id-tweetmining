package edu.upi.cs.tweetmining;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PilihRepresentasiKeywords {
	//terkait dengan PilihRepresentasiTw
	//ambil keyword dalam satuan waktu seusai statistik (lihat PrepareWarehouse): jam, hari, bulan dan tahun
	//simpan keyword ke dalam database
	
	public String dbName;           //  format: localhost/mydbname
	public String userName;
	public String password;
	public String query;
	public int jumDiproses;
	public int jumKeywords=5;
	   
	private static final Logger logger = Logger.getLogger("PilihRepKeywords");	
	
    public void prosesJam() {
    	//tabel stat_per_jam sudah terisi
    	//data tweet dimabil dari tw_jadi, tw_jadi keywords sudah terisi
	
		System.out.println("Mulai proses jam :"+this.getClass().getName());
		
		Handler consoleHandler = new ConsoleHandler();
		logger.addHandler(consoleHandler);  
		Connection conn=null;      
	    PreparedStatement pStat=null;
	    PreparedStatement pTw=null;
	    PreparedStatement pFlag=null;
	    PreparedStatement pDel=null;
	    PreparedStatement pIns=null;
	    
	    
	    try {
	    	//proses jam 
	        Class.forName("com.mysql.jdbc.Driver");
	        conn = DriverManager.getConnection("jdbc:mysql://"+dbName+"?user="+userName+"&password="+password);
	        conn.setAutoCommit(false);
	        //hanya neg dan pos saja yang yang disimpan
	        //TBD masih dihardcode kelasnya
	        
	        //ambil stat_per_jam
	        pStat = conn.prepareStatement ("select id, tgl_jam, kelas from stat_per_jam where ((kelas = 1) or (kelas=2)) and is_loaded_rep_keywords = 0 limit 0,1000");  
	        
	        //ambil sekian tw yang sesuai dengan kelas pada periode waktu jam tsb (kita coba 100 dulu)
	        pTw   = conn.prepareStatement ("select id_internal,keywords from tw_jadi where kelas=? and  is_loaded=1 and created_at_wib>=? and created_at_wib<? order by bobot desc limit 0,100");
	        
	        //langsung dibersihkan aja ya?? atau dicek dulu kalau sudah ada 
	        pDel    =  conn.prepareStatement ("delete from keywords_stat_per_jam where id_stat_per_jam=?");
	        
	        //insert ke tabel keywords_stat_perj_jam
	        pIns    =  conn.prepareStatement ("insert into keywords_stat_per_jam(id_stat_per_jam,keywords) values (?,?)");
	        
	        //flag bahwa sudah diproses
	        pFlag   =  conn.prepareStatement ("update stat_per_jam set is_loaded_rep_keywords = 1 where id = ?");
	        
	        ResultSet rsStat = pStat.executeQuery();  
	        jumDiproses = 0;
	        java.sql.Timestamp tgl_jam; 
        	Integer freq;
        	String delims = ";";
        	String kata;
        	String tt;
        	
	        //pindahkan isi query agregat ke tabel stat_per_jam
	        while ( rsStat.next())  {
	        	jumDiproses++;
	        	long idStat     = rsStat.getLong(1);
	        	tgl_jam         = rsStat.getTimestamp(2);
	        	int kelas       = rsStat.getInt(3);
	        	
	        	//woy ruwet banget ya, cuma cari range
	        	Calendar c1 = Calendar.getInstance();
	        	c1.setTime(tgl_jam);
	        	java.sql.Timestamp  ts1 = new java.sql.Timestamp(c1.getTime().getTime());
	        	//System.out.println(ts1);
	        	
	        	Calendar c2 = Calendar.getInstance();
	        	c2.setTime(tgl_jam);
	        	c2.add(Calendar.HOUR,1);
	        	java.sql.Timestamp  ts2 = new java.sql.Timestamp(c2.getTime().getTime());
	        	//System.out.println(ts2);
	        	
	        	//hapus yg lama kalau ada
	        	pDel.setLong(1,idStat);
	        	pDel.executeUpdate();
	        	
	        	pTw.setInt(1,kelas);
	        	pTw.setTimestamp(2,ts1);
	        	pTw.setTimestamp(3,ts2);
	        	ResultSet rsTw = pTw.executeQuery(); 
	        	
	        	
	        	//ambil tweet pada range tersebut
	        	
	        	TreeMap<String,Integer> countWord  = new TreeMap<String,Integer>();  //terurut berd keyword
	        	while ( rsTw.next())  {
	        		long idTw          = rsTw.getLong(1);
	            	String keywords    = rsTw.getString(2);     
	            	//count freq
	            	String[] tokens = keywords.split(delims);
	            	for(String t:tokens) {
	            		t=t.trim();
	            		if (t.equals("")) continue;
	            		freq = countWord.get(t);  
	            		countWord.put(t.trim(), (freq == null) ? 1 : freq + 1);
	            	}
	        	}
	        	
	        	//proses keywords (hitung freq tertinggi, TBD untuk alg. yg lebih bagus)
	        	TreeSet<String> countWord2  = new TreeSet<String>( Collections.reverseOrder());  //terurut berd freq
	            for (Map.Entry<String,Integer> entry : countWord.entrySet()) {
	            	kata = entry.getKey();
	                freq = entry.getValue();
	                //System.out.println(kata+"="+freq);  //tulis ke file
	                String strFreq = String.format("%05d",freq);
	                countWord2.add(strFreq+";"+kata);
	            }	            
	            int cc=0;
	            StringBuffer sb = new StringBuffer();
	            for (String freqKeyword:countWord2) {	            	
	            	if (cc>jumKeywords-1) break;
	            	String[] tokens = freqKeyword.split(delims);
	            	System.out.println(tokens[1]);  
	            	sb.append(tokens[1]+";");
	            	cc++;
	             }
	        	
	        	//insert
            	pIns.setLong(1,idStat);
            	pIns.setString(2,sb.toString());
            	pIns.executeUpdate();
            	
	        	//flag sudah diproses
	        	pFlag.setLong(1,idStat);
	        	pFlag.executeUpdate();
	        	conn.commit();
	        }
	        System.out.println("Selesai..., jum diproses "+jumDiproses+";"+this.getClass().getName());
	    } //try
	    catch (Exception e) {
	        //rollback
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
	            	if (pFlag != null)    {pFlag.close(); }
	            	if (pDel != null)     {pDel.close(); }
	            	if (pIns != null)     {pIns.close(); }
	            	if (pFlag != null)    {pFlag.close(); }	  
	            	if (pStat != null)    {pStat.close(); }
	                if (pTw != null) 	  {pTw.close();}
	                if (conn != null)     {conn.close();}				   
	              } catch (Exception e) {
	                  logger.log(Level.SEVERE, null, e);
	         }
	    }
    }
    
    public static void main(String[] args) {
    	//update stat_per_jam set is_loaded_rep_tw=0;
    	//update stat_per_jam set is_loaded_rep_keywords=0;
    	PilihRepresentasiKeywords pr = new PilihRepresentasiKeywords();
    	pr.dbName = "localhost/indosat1";
		pr.userName = "yudi3";
		pr.password = "rahasia";    	
		pr.prosesJam();
    }
}
