package edu.upi.cs.tweetmining;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PrepareWarehouse {
  /* pindahkan data ke format warehouse
  
  dimensi yang ada: 
  	   - waktu:     tahun, bulan, hari, jam
       - sentimen:  positif, negatif, non opini	
  
  fact: 
    	-jumlah	
        -keyword
   * 
   * 
   * 	
   */
	
	public String dbName;           //  format: localhost/mydbname
	public String userName;
	public String password;
	public String query;
	public int jumDiproses;
	   
	private static final Logger logger = Logger.getLogger("Warehouse");
	
	public void prosesTahun() {
		
	}
		
	public void prosesBulan() {
		
	}
	
    public void prosesHari() {
    	//input: tabel stat_per_jam 
    	//output: tabel stat_per_hari, stat_per_jam terisi 1
    	//        tabel stat_per_jam, hari, bulan, tahun terisi
    	// saat proses ini, prosesJam tidak boleh dilangsungkan (sudah selesai)
    	// lakukan 
    	
    	//TBD: pemrosesan parsial, agar lebih scalable, saat ini semua tabel diproses sekaligus
		//TBD: cek kebenaran output program untuk update 
    	
    	Handler consoleHandler = new ConsoleHandler();
		logger.addHandler(consoleHandler);  
		System.out.println("Load ke warehouse hari");
        Connection conn=null;      
        PreparedStatement pTw=null;
        PreparedStatement pFlag = null;
        PreparedStatement pSdhAda = null;
        PreparedStatement pUpd = null;
        PreparedStatement pIns = null;
        
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://"+dbName+"?user="+userName+"&password="+password);
            
            //proses hari
            conn.setAutoCommit(false);   //harus atomic
            pTw     = conn.prepareStatement ("select tahun, bulan, hari, kelas, sum(jum), sum(old_jum) FROM stat_per_jam WHERE is_loaded=0 GROUP BY tahun, bulan, hari, kelas");  
            pSdhAda = conn.prepareStatement ("select id,jum from stat_per_hari where tahun=? and bulan=? and hari=? and kelas=?");
            pUpd    = conn.prepareStatement ("update stat_per_hari set jum=? where id=?");
            pFlag   = conn.prepareStatement ("update stat_per_jam set is_loaded = 1, old_jum = 0 where is_loaded=0");
            pIns    = conn.prepareStatement ("insert into stat_per_hari(tahun,bulan,hari,kelas,jum,tgl_jam) values (?,?,?,?,?,?)");
            
            ResultSet rsTw = pTw.executeQuery();  
            jumDiproses = 0;
            
            //pindahkan isi query stat_per_jam ke tabel stat_per_hari
            while ( rsTw.next())  {
            	int tahun = rsTw.getInt(1);
            	int bulan = rsTw.getInt(2);
            	int hari = rsTw.getInt(3);
            	int kelas = rsTw.getInt(4);
            	long jumCount = rsTw.getLong(5);
            	long oldJumCount = rsTw.getLong(6);

            	
            	//convert ke tipe jam 
            	Calendar c = Calendar.getInstance();
            	c.set(Calendar.YEAR, tahun);
            	c.set(Calendar.MONTH, bulan-1);  
            	c.set(Calendar.DATE, hari);
            	c.set(Calendar.HOUR_OF_DAY, 0);
            	c.set(Calendar.MINUTE, 0);
            	c.set(Calendar.SECOND, 0);
               	java.sql.Timestamp tglM = new java.sql.Timestamp(c.getTime().getTime()) ;
            	
            	jumDiproses++;
            	//proses disini
            	System.out.println("Tahun:"+tahun+" Bulan:"+bulan+" Hari:"+hari+" kelas "+kelas+ " jumcount:"+jumCount);
            	
            	//cari di tabel, kalau belum ada insert, kalau sudah ada, add.
            	pSdhAda.setInt(1, tahun);
            	pSdhAda.setInt(2, bulan);
            	pSdhAda.setInt(3, hari);
            	pSdhAda.setInt(4, kelas);
            	
            	ResultSet rsSdhAda = pSdhAda.executeQuery();
                if (rsSdhAda.next()) {
                	//sudah ada
                	//kurangi dulu dengan jumlah versi lama (oldcount), baru tambahkan dengan count yang baru
                	long id = rsSdhAda.getLong(1);
                	jumCount = jumCount + rsSdhAda.getLong(2) - oldJumCount;  
                	//update update stat_per_hari set jum = ? where id =?
                	pUpd.setLong(1,jumCount);
                	pUpd.setLong(2,id);
                	pUpd.executeUpdate();
                	System.out.println("update hari");
                } else {
                	//belum ada, insert
                	//"insert into stat_per_hari(tahun,bulan,hari,kelas,jum,tgl_jam)
                	pIns.setInt(1,tahun);
                	pIns.setInt(2,bulan);
                	pIns.setInt(3,hari);
                	pIns.setInt(4,kelas);
                	pIns.setLong(5,jumCount);
                	pIns.setTimestamp(6,tglM);
                	pIns.executeUpdate();
                	System.out.println("insert stat hari");
                }
            } //end while
            //tandai sudah diload
            pFlag.executeUpdate();
            conn.commit();
            System.out.println("Commit, Selesai...");
        } //try
        catch (Exception e) {
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
                	  if (pUpd != null)     {pUpd.close();}
                      if (pIns != null)     {pIns.close(); }
                      if (pSdhAda != null)  {pSdhAda.close(); }
                	  if (pTw != null) 	    {pTw.close();}
                      if (pFlag != null)    {pFlag.close(); }
                      if (conn != null)     {conn.close();}				   
                  } catch (Exception e) {
                      logger.log(Level.SEVERE, null, e);
             }
        }
    }
	
	
	public void prosesJam() {
    	//input: tabel tw_jadi yang sudah mendapatkan kelas (field is_class_defined == 1)
    	//output: tabel tw_jadi.is_loaded terisi 1
    	//        tabel stat_per_jam, hari, bulan, tahun terisi
    	
    	
    	//perlu diperhatikan bahwa saat proses, data baru bisa masuk
    	//jadi perlu dicatat id-nya
    	
		//setelah prosesJam, harus dilakukan dengan segera prosesHari, Bulan dst sebelum method prosesJam dipanggil lagi
		//soalnya field old_jum hanya bisa menyimpan satu siklus update
		
    	//TBD: pemrosesan parsial, agar lebih scalable, saat ini semua tabel diproses sekaligus
		//TBD: cek kebenaran output program untuk update 
		
    	Handler consoleHandler = new ConsoleHandler();
		logger.addHandler(consoleHandler);  
		System.out.println("Load ke warehouse");
        Connection conn=null;      
        PreparedStatement pTw=null;
        PreparedStatement pFlag = null;
        PreparedStatement pMaxId = null;
        PreparedStatement pSdhAda = null;
        PreparedStatement pUpd = null;
        PreparedStatement pIns = null;
        
        
        
        try {
        	//proses jam 
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://"+dbName+"?user="+userName+"&password="+password);
            
            conn.setAutoCommit(false);   //harus atomic
            
            //harus dicatat id-nya karena bisa saja saat diproses ada data lain yang masuk
            pMaxId = conn.prepareStatement("select max(id_internal) from tw_jadi where is_loaded=0 and is_class_defined=1");
            
            //
            pTw     = conn.prepareStatement ("SELECT YEAR(created_at_wib), MONTH(created_at_wib), DAY( created_at_wib ), HOUR( created_at_wib ), kelas, COUNT(*) FROM tw_jadi WHERE is_loaded =0 AND is_class_defined=1 AND id_internal <= ? GROUP BY YEAR(created_at_wib), MONTH(created_at_wib), DAY( created_at_wib ), HOUR( created_at_wib ), kelas");  
            pSdhAda = conn.prepareStatement ("select id,jum from stat_per_jam where tahun =? and bulan =? and hari =? and jam=? and kelas=?");
            
            pUpd    = conn.prepareStatement ("update stat_per_jam set jum = ?, old_jum=?, is_loaded=0 where id =?"); 
            pIns    = conn.prepareStatement ("insert into stat_per_jam(tahun,bulan,hari,jam,kelas,jum,tgl_jam) values (?,?,?,?,?,?,?)");
            pFlag   = conn.prepareStatement ("update tw_jadi set is_loaded = 1 where is_class_defined=1 and is_loaded=0 and id_internal <= ?");

            
            ResultSet rsMaxId = pMaxId.executeQuery();
            int maxId=-99; 
            if (rsMaxId.next()) {
            	maxId = rsMaxId.getInt(1);
            }
            System.out.println("maxID="+maxId);
            pTw.setInt(1,maxId);  
            ResultSet rsTw = pTw.executeQuery();  
            jumDiproses = 0;
            
            //pindahkan isi query agregat ke tabel stat_per_jam
            while ( rsTw.next())  {
            	int tahun = rsTw.getInt(1);
            	int bulan = rsTw.getInt(2);
            	int hari = rsTw.getInt(3);
            	int jam = rsTw.getInt(4);
            	
            	//convert ke tipe jam 
            	
            	Calendar c = Calendar.getInstance();
            	
            	c.set(Calendar.YEAR, tahun);
            	c.set(Calendar.MONTH, bulan-1);  
            	c.set(Calendar.DATE, hari);
            	c.set(Calendar.HOUR_OF_DAY, jam);
            	c.set(Calendar.MINUTE, 0);
            	c.set(Calendar.SECOND, 0);
               	java.sql.Timestamp tglM = new java.sql.Timestamp(c.getTime().getTime()) ;
            	
            	int kelas = rsTw.getInt(5);
            	long jumCount = rsTw.getLong(6);
            	
            	jumDiproses++;
            	//proses disini
            	System.out.println("Tahun:"+tahun+" Bulan:"+bulan+" Hari:"+hari+ " jam:"+jam+" kelas "+kelas+ " jumcount:"+jumCount);
            	
            	//cari di tabel, kalau belum ada insert, kalau sudah ada, add.
            	//ada cara yg lebih efisien ?
            	//tahun =? and bulan =? and hari =? and jam=? and kelas=?
            	pSdhAda.setInt(1, tahun);
            	pSdhAda.setInt(2, bulan);
            	pSdhAda.setInt(3, hari);
            	pSdhAda.setInt(4, jam);
            	pSdhAda.setInt(5, kelas);
            	
            	ResultSet rsSdhAda = pSdhAda.executeQuery();
                if (rsSdhAda.next()) {
                	//sudah ada, ditambah saja countnya (asumsi tidak ada data yg didelete)
                	long id = rsSdhAda.getLong(1);
                	long oldJumCount = jumCount;
                	jumCount = jumCount + rsSdhAda.getLong(2);  //tambah
                	//update update stat_per_jam set jum = ?, old_jum=?, is_loaded=0 where id =?
                	//tandai agar nanti diproses oleh proses hari
                	//kenapa ada oldcount? karena untuk mendapatkan jumlah hari, harus di sum, tanpa oldcount, tidak mungkin bisa menambah jumlah yg benar.
                	pUpd.setLong(1,jumCount);
                	pUpd.setLong(2,oldJumCount); 
                	pUpd.setLong(3,id);
                	pUpd.executeUpdate();
                	System.out.println("update");
                } else {
                	//belum ada, insert
                	//insert into stat_per_jam(tahun,bulan,hari,jam,kelas,jum,tgl_jam) values (?,?,?,?,?,?)
                	pIns.setInt(1,tahun);
                	pIns.setInt(2,bulan);
                	pIns.setInt(3,hari);
                	pIns.setInt(4,jam);
                	pIns.setInt(5,kelas);
                	pIns.setLong(6,jumCount);
                	pIns.setTimestamp(7,tglM);
                	pIns.executeUpdate();
                	System.out.println("insert");
                }
            } //end while
            
            //tandai sudah diload
            pFlag.setLong(1,maxId);
            pFlag.executeUpdate();
            conn.commit();
            System.out.println("Commit, Selesai...");
        } //try
        catch (Exception e) {
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
                	  if (pUpd != null)     {pUpd.close();}
                      if (pIns != null)     {pIns.close(); }
                	  if (pMaxId != null)   {pMaxId.close();}
                      if (pSdhAda != null)  {pSdhAda.close(); }
                	  if (pTw != null) 	    {pTw.close();}
                      if (pFlag != null)    {pFlag.close(); }
                      if (conn != null)     {conn.close();}				   
                  } catch (Exception e) {
                      logger.log(Level.SEVERE, null, e);
             }
        }
    }
    
    
    public static void main(String[] args) {
    	PrepareWarehouse pw = new PrepareWarehouse();
    	pw.dbName = "localhost/indosat1";
		pw.userName = "yudi3";
		pw.password = "rahasia";
    	//pw.prosesJam();
		pw.prosesHari();
    }
    	
 /*
  * 
  * 

NANTI PERLU DIREFRESH LAGI DENGAN STRUKTUR TERBARU!!


delete from stat_per_jam;
update tw_jadi set is_loaded=0


query untuk mendapat count per jam per kelas

SELECT YEAR( created_at_wib ) , MONTH( created_at_wib ) , DAY( created_at_wib ) AS 
DAY , HOUR( created_at_wib ) AS HOUR , kelas, COUNT( * ) AS Count
FROM tw_jadi
GROUP BY YEAR( created_at_wib ) , MONTH( created_at_wib ) , DAY( created_at_wib ) , HOUR( created_at_wib ) , class


create table stat_per_jam (
   id int not null AUTO_INCREMENT primary key,
   tahun int not null,
   bulan int not null,
   hari  int not null,
   jam   int not null,
   kelas int not null,
   jum   int not null,
   keywords varchar(200),
   KEY `tahun` (`tahun`),
   KEY `bulan` (`bulan`),
   KEY `hari` (`hari`),
   KEY `jam` (`jam`),
   KEY `kelas` (`kelas`)   
);

create table stat_per_hari (
   id int not null primary key,
   tahun int not null,
   bulan int not null,
   hari  int not null,
   jam   int not null,
   class int not null,
   jum   int not null,
   KEY `from_user_id` (`from_user_id`),
)

create table stat_per_minggu (
   id int not null primary key,
   tahun int not null,
   bulan int not null,
   hari  int not null,
   jam   int not null,
   class int not null,
   jum   int not null,
   KEY `from_user_id` (`from_user_id`),
)

create table stat_per_bulan (
   id int not null primary key,
   tahun int not null,
   bulan int not null,
   hari  int not null,
   jam   int not null,
   class int not null,
   jum   int not null,
   KEY `from_user_id` (`from_user_id`),
)

create table stat_per_tahun (
   id int not null primary key,
   tahun int not null,
   KEY `from_user_id` (`from_user_id`),
)




*/
    	  
    
    
    
}
