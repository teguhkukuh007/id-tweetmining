package edu.upi.cs.tweetmining;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.Authenticator;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.MatchResult;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class LatLongToRawLocationDB {
	/*
	 *     tujuannya mencari lokasi dari setiap tweet
	 * 	   menggunakan google map API (https://developers.google.com/maps/documentation/geocoding/)
	 *     warning: hanya 2500 req per hari.    
	 *     
	 *     supaya efisien, dibuat sistem cache, select distinct, lalu disimpan lokasinya di tabel cache 
	 *     nanti kalau ada data baru lihat table cache tersebut dulu 
	 *     
	 *     prosesnya: panggil method IsiCache
	 *     
	 *     
	 *     alter table tw_jadi add is_reversegeo int default 0
	 *     alter table tw_jadi add index is_reversegeo(is_reversegeo)
	 *     
	      create table lotlang_cache (
	          id_internal bigint (20) not null auto_increment,
	          lat  double not null,
	          longi double not null,
	          lokasi_mentah text not null,
	          primary key (id_internal),
	          key lat (lat),
	          key longi(longi)
	      ) DEFAULT CHARSET=utf8;

	      
	      
	 *     
	 *     input: tabel tw_jadi
	 *     output: tabel tw_lokasi_mentah terisi
	 *     
	 *     
	 */
	   
	private static final Logger logger = Logger.getLogger("Crawler");
	   public String dbName;           //  format: localhost/mydbname
	   public String userName;
	   public String password;
	   public String query;
	   public int jumDiproses;
	   public int jumTdkDiproses;
	   
	   String proxyHost="";
	   String proxyPort="";
	   String userNameProxy="";
	   String passwordProxy="";
	   
	   
	   public LatLongToRawLocationDB() {
		    Handler consoleHandler = new ConsoleHandler();
	        consoleHandler.setLevel(Level.WARNING);
	        logger.setLevel(Level.FINER);
	        logger.addHandler(consoleHandler);               
	        logger.fine("mulai");
	   }
	   
	   public void parseLokasiMentah() {
	   /*
	    *     berd. field lokasi mentah, mengisi field negara,kode_pos,prop,kabu_kota,alamat
	    *      
	    * 
	    */
	        Connection conn=null;      
	        PreparedStatement pSel=null;    
		    //PreparedStatement pIns=null;
	        try {
	            Class.forName("com.mysql.jdbc.Driver");
	            conn = DriverManager.getConnection("jdbc:mysql://"+dbName+"?user="+userName+"&password="+password);
	            //pake limit dulu untuk testing
	            pSel      = conn.prepareStatement("select lokasi_mentah from lotlang_cache");  
	            ResultSet rs = pSel.executeQuery();
	            while (rs.next())  {
	            	System.out.println("---");
	            	String lokasi = rs.getString(1);
	            	ObjectMapper mapper = new ObjectMapper();
	            	JsonNode root = mapper.readValue(lokasi, JsonNode.class);
	            	JsonNode res  = root.get("results"); //array
	            	JsonNode firstAddressComponent    = res.get(0); //elemen pertama, yang paling lengkap 
	            	JsonNode addressComponent         = firstAddressComponent.get("address_components"); //elemen pertama, yang paling lengkap 
	            	for (JsonNode addrNode : addressComponent) {
	            		String longName = addrNode.get("long_name").getTextValue();
	            		JsonNode typeNode = addrNode.get("types");
	            		for (JsonNode t: typeNode) {
	            			if (t.asText().equals("administrative_area_level_1")) {
	            				System.out.println("Propinsi="+longName);
	            			} else  if (t.asText().equals("administrative_area_level_2")) {
	            				System.out.println("Kabu/Kota="+longName);
	            			} else  if (t.asText().equals("country")) {
	            				System.out.println("Negara="+longName);
	            			} else  if (t.asText().equals("postal_code")) {
	            				System.out.println("Kode Pos="+longName);
	            			} else  if (t.asText().equals("locality")) {
	            				System.out.println("KabuKota?="+longName);   //kalau kabu/kota kosong, gunakan ini.
	            			//} else  if (t.asText().equals("sublocality")) {
	            		    //	System.out.println("sublocality?="+longName);
	            			}
	            			else {
	            				//System.out.println(t.asText()+"???="+longName);
	            			}
	            		}
	            	}
	            }	
	            rs.close();
	        } catch (Exception e) {
	            logger.log(Level.SEVERE, null, e);
	        }
	        finally  {
	          try  {
	                    if (pSel != null) {pSel.close();}                
	                    if (conn != null) {conn.close();}	
	                } catch (Exception e) {
	                    logger.log(Level.SEVERE, null, e);
	           }
	        }
		   
		    
		   
	   }
	   
	   
	   public  void isiCache() {
		   
	   /*   
	    *   ambil data distinct lat,long  dari tabel tweet
	    *   belum ada:masuk ke tabel cache 
	    */
	        System.out.println("Ambil tw dan request lokasi berd lat-lang");
	        
	        if (!proxyHost.equals(""))
	        {
		        System.setProperty("http.proxyHost",proxyHost) ;  
		        System.setProperty("http.proxyPort",proxyPort) ;  	        
		        Authenticator.setDefault(new ProxyAuth(userNameProxy,passwordProxy));  
	        }
	        URL u;
	        InputStream is;
	        Connection conn=null;      
	        PreparedStatement pSel=null;    
	        PreparedStatement pSelCache=null;     
	        PreparedStatement pIns=null;
	        try {
	            Class.forName("com.mysql.jdbc.Driver");
	            conn = DriverManager.getConnection("jdbc:mysql://"+dbName+"?user="+userName+"&password="+password);
	            //pake limit dulu untuk testing
	            pSel      = conn.prepareStatement("select distinct geo_lat,geo_long from tw_jadi where geo_type='Point' and (geo_lat<>0) and (geo_long<>0) and is_reversegeo=0");  
	            pSelCache = conn.prepareStatement("select geo_lat,geo_long from lotlang_cache where geo_lat=? and geo_long=?");  
	            pIns      = conn.prepareStatement("insert into lotlang_cache(geo_lat,geo_long,lokasi_mentah) values (?,?,?) ");
	            ResultSet rs = pSel.executeQuery();
	            ResultSet rs2;
	            jumDiproses = 0;
	            jumTdkDiproses = 0;
	            double lat;
	            double longi;
	            while (rs.next())  {	            	
	            	lat = rs.getDouble(1);
	            	longi = rs.getDouble(2);
	            	System.out.println(lat+","+longi);
	            	
	            	pSelCache.setDouble(1,lat);
	            	pSelCache.setDouble(2,longi);
	            	
		            rs2 = pSelCache.executeQuery();
		            if (!rs2.next()) {
		            	//belum ada? cari di Google dan masukkan
		            	jumDiproses++;
		            	//ambil data dari Google
		            	System.out.println("ambil data dari google, sleep dulu agar tidak terlalu cepat");
		            	Thread.sleep(10*1000); //sleep 10 detik
			            String strUrl = "http://maps.googleapis.com/maps/api/geocode/json?latlng="+lat+","+longi+"&sensor=false"; 
			            
			            u = new URL(strUrl); 
			            is = u.openStream();           
			            
			            char[] buffer = new char[1024 * 16]; 
			            StringBuilder sbOut = new StringBuilder();
			            Reader in = new InputStreamReader(is, "UTF-8");
			            int bytesRead;
			            while ((bytesRead = in.read(buffer, 0, buffer.length)) != -1) {
			            	sbOut.append(buffer, 0, bytesRead);
			            }
			            is.close();
			            in.close();
			            String strOut = sbOut.toString().trim();
			            //cek apakah diakhiri dengan karakter "}", kalau TIDAK artinya JSON tidak valid, harus diskip
			            if (strOut.length()>0) {
			               if (strOut.charAt(strOut.length()-1)!='}') {  //bukan JSON
			             	  System.out.println("-------------------------->ada kesalahan, format tidak sesuai JSON, abort! <-------------------------");             	  
			             	  System.out.println(strOut.charAt(strOut.length()-1));
			               } else {
			                  pIns.setDouble(1,lat);              
			                  pIns.setDouble(2,longi);
			                  pIns.setString(3,strOut);
			                  pIns.executeUpdate();
			               }
			            } else {
			         	   //kosong, error juga
			            }
		            } else {
		            	jumTdkDiproses++;
		            }
	            }
	            rs.close();
	        } catch (Exception e) {
	            logger.log(Level.SEVERE, null, e);
	        }
	        finally  {
	          try  {
	                    if (pSel != null) {pSel.close();}                
	                    if (conn != null) {conn.close();}	
	                    
	                    if (pSelCache != null) {pSelCache.close();} 
	                    if (pSelCache != pIns) {pSelCache.close();}
	                } catch (Exception e) {
	                    logger.log(Level.SEVERE, null, e);
	           }
	        }
	    }
	    
	    public static void main(String[] args) throws InterruptedException {
	        
	    	
	    	//testing    	
	    	LatLongToRawLocationDB ll = new LatLongToRawLocationDB();
//	    	tw.proxyHost ="cache.itb.ac.id";
//	    	tw.proxyPort ="8080";
//	    	tw.userNameProxy ="yudi.wibisono";
//	    	tw.passwordProxy ="*********";
	    	ll.dbName = "localhost/masterchef2";
	    	ll.userName = "yudi3";
	    	ll.password = "rahasia";
	    	//ll.isiCache();
	    	
	    	ll.parseLokasiMentah();
	    	
//	    	for (int i=0; i<=100 ; i++) {
//	            System.out.println("Proses ke---------------------------------->"+i);
//	            tw.process();
//	            //sleep menit
//	            if (isError) {
//	                System.out.println("Terjadi kesalahan, distop");
//	                System.exit(1);
//	            } else {
//	                int jumTidurInMin = 5;
//	                System.out.println("Selesai, tidur dulu "+ jumTidurInMin+ " menit, setelah menyelesaikan iterasi ke "+i);          
//	                Thread.sleep(jumTidurInMin * 60000);
//	            }
//	        }
	    	System.out.println("Selesai semua!");
	    }
	}
