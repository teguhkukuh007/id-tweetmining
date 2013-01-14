package edu.upi.cs.tweetmining;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PilihRepresentasiTw {
	
   /*    masih terkait dengan pengisian warehouse (lihat class PrepareWarehouse)
    *    memilih twitter yang palin reprsentatf sesuai satuan (jam,hari,bulan dst)
    * 	 stateawal: tabel tw_stat_per_jam, ..hari, bulan dst sudah ada
    *    
    *    TBD: kelas jangan diharcode
    * 
    * 
  CREATE TABLE IF NOT EXISTS `tw_stat_per_jam` (
  `id_internal` bigint(20) NOT NULL AUTO_INCREMENT,
  `id_stat_per_jam` bigint(20) NOT NULL DEFAULT '0',
  `id_tw_jadi` bigint(20) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id_internal`),
  KEY `id_stat_per_jam` (`id_stat_per_jam`),
  KEY `id_tw_jadi` (`id_tw_jadi`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT='tweet yang merepresentasikan tweet dalam jam terentu. Harusnya diisi oleh algoritma yang mencari tweet yang paling relevan dan penting';
    */
   
	public String dbName;           //  format: localhost/mydbname
	public String userName;
	public String password;
	public String query;
	public int jumDiproses;
	   
	private static final Logger logger = Logger.getLogger("PilihRepTw");	
	
    public void prosesJam() {
    //tabel stat_per_jam sudah terisi
    //data tweet dimabil dari tw_jadi, tw_jadi bobot sudah terisi
    	
    	System.out.println("Mulai proses jam+"+this.getClass().getName());
    	
    	Handler consoleHandler = new ConsoleHandler();
		logger.addHandler(consoleHandler);  
		System.out.println("Cari tw representatif untuk stat_tw_per_jam");
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
            pStat     = conn.prepareStatement ("select id, tgl_jam, kelas from stat_per_jam where ((kelas = 1) or (kelas=2)) and is_loaded_rep_tw = 0 limit 0,1000");  
            
            //ambil 10 tw yang sesuai dengan kelas pada periode waktu jam tsb
            pTw   = conn.prepareStatement ("select id_internal,text,created_at_wib,bobot from tw_jadi where kelas=? and  is_loaded=1 and created_at_wib>=? and created_at_wib<? order by bobot desc limit 0,10");
            
            //langsung dibersihkan aja ya?? atau dicek dulu kalau sudah ada 
            pDel    =  conn.prepareStatement ("delete from tw_stat_per_jam where id_stat_per_jam=?");
            
            //insert ke tabel tw_stat_perj_jam
            pIns    =  conn.prepareStatement ("insert into tw_stat_per_jam(id_stat_per_jam,id_tw_jadi) values (?,?)");
            
            //flag sudah diproses
            pFlag   =  conn.prepareStatement ("update stat_per_jam set is_loaded_rep_tw = 1 where id = ?");
            
            ResultSet rsStat = pStat.executeQuery();  
            jumDiproses = 0;
            java.sql.Timestamp tgl_jam; 
            //pindahkan isi query agregat ke tabel stat_per_jam
            while ( rsStat.next())  {
            	jumDiproses++;
            	long idStat     = rsStat.getLong(1);
            	tgl_jam         = rsStat.getTimestamp(2);
            	int kelas       = rsStat.getInt(3);
            	//System.out.println(tgl_jam);
            	
            	//woy ruwet banget ya, cuma cari range
            	Calendar c1 = Calendar.getInstance();
            	c1.setTime(tgl_jam);
            	//System.out.println(c1.getTime());
            	java.sql.Timestamp  ts1 = new java.sql.Timestamp(c1.getTime().getTime());
            	System.out.println(ts1);
            	
            	Calendar c2 = Calendar.getInstance();
            	c2.setTime(tgl_jam);
            	c2.add(Calendar.HOUR,1);
            	//System.out.println(c2.getTime());
            	java.sql.Timestamp  ts2 = new java.sql.Timestamp(c2.getTime().getTime());
            	System.out.println(ts2);
            	
            	//hapus yg lama kalau ada
            	pDel.setLong(1,idStat);
            	pDel.executeUpdate();
            	
            	pTw.setInt(1,kelas);
            	pTw.setTimestamp(2,ts1);
            	pTw.setTimestamp(3,ts2);
            	ResultSet rsTw = pTw.executeQuery(); 
            	
            	
            	//ambil tweet pada range tersebut
            	while ( rsTw.next())  {
            		long idTw          = rsTw.getLong(1);
                	String strTw       = rsTw.getString(2);     //tidak digunakan hanya utk debug
                	Timestamp tsTw     = rsTw.getTimestamp(3);  //tidak digunakan
                	double bobot       = rsTw.getDouble(4);     //tidak digunakan
                	System.out.println(kelas+":"+bobot+":"+tsTw+"->"+strTw);
                	
                	//insert
                	pIns.setLong(1,idStat);
                	pIns.setLong(2,idTw);
                	pIns.executeUpdate();
            	}
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
    	PilihRepresentasiTw pr = new PilihRepresentasiTw();
    	pr.dbName = "localhost/indosat1";
		pr.userName = "yudi3";
		pr.password = "rahasia";    	
		pr.prosesJam();
    }
}
