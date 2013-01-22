/*
 *  Copyright (C) 2012 yudi wibisono (yudi1975@gmail.com/yudi@upi.edu)
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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Scanner;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.MatchResult;
import java.sql.*;

import java.net.Authenticator;
import java.net.URL;
import java.net.URLConnection;

/**
 *  data mentah dikumpulkan di database, tidak diproses
 *  mensimulasikan light-node, yang hanya berfungsi untuk mengumpulkan dan diproses kemudian
 *  
 *  
 * Contoh menjalankan di command line, masuk ke direktori bin lalu jalankan java
 * G:\LibTweetMining2\bin>java -classpath .;../libs/mysql-connector-java-5.0.8-bin.jar edu.upi.cs.tweetmining.TwCrawler
 *  
 * setelah selesai, lanjutkan dengan ProsesTwMentahDB 
 *  
 struktur tabel: yg wajib ada adalah content dan status
 
 CREATE TABLE IF NOT EXISTS `tw_mentah` (
  `id_internal` bigint(20) NOT NULL AUTO_INCREMENT,
  `time_stamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `content` text NOT NULL,
  `status` smallint(6) NOT NULL COMMENT '0: belum diproses',
  PRIMARY KEY (`id_internal`),
  KEY `status` (`status`)
) DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `crawl_log_gagal` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `time_stamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `log_str` text NOT NULL, 
  PRIMARY KEY (`id`)
) DEFAULT CHARSET=utf8;


TBD, 
- bisa dibuat lebih efisien untuk pemanggilan berulang kali? (walaupun masalah utama proses crawl dibatasi oleh limitasi API twitter)

- tambah parameter, supaya lebih gampang dijalankan 
  
  -q query
  -db databasename
  -u username
  -p password
  -proxyhost proxyhost
  -proxyport proxyport
  -proxyuser usernameproxy
  -proxypass proxypassword
  -createtable

 */

public class TwCrawler {
   private static final Logger logger = Logger.getLogger("Crawler");
   static boolean isError = false;  
   public String dbName;           //  format: localhost/mydbname
   public String userName;
   public String password;
   public String tableName;
   public String query;
   private String nextQuery="";      //untuk page selanjutnya
   
   String proxyHost="";
   String proxyPort="";
   String userNameProxy="";
   String passwordProxy="";
   
   
   public  void process() {
        Handler consoleHandler = new ConsoleHandler();
        consoleHandler.setLevel(Level.WARNING);
        logger.setLevel(Level.FINER);
        logger.addHandler(consoleHandler);               
        logger.fine("mulai");
        //log.log(Level.FINE,"msg","mulai");
        System.out.println("Ambil tweet mentah dan masuk ke DB");
        
        if (!proxyHost.equals(""))
        {
	        System.setProperty("http.proxyHost",proxyHost) ;  
	        System.setProperty("http.proxyPort",proxyPort) ;  	        
	        Authenticator.setDefault(new ProxyAuth(userNameProxy,passwordProxy));  
        }
        URL u;
        InputStream is;
        Connection conn=null;      
        PreparedStatement pUsr=null;     
        PreparedStatement pErr=null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://"+dbName+"?user="+userName+"&password="+password);
            String q;
            
            if (nextQuery.equals("")) {
            	q = query; 
            } else  {
            	q = nextQuery; //gunakan refersh_url, sehingga tidak ada duplikasi tweet
            }
            
            String strUrl = "http://search.twitter.com/search.json?"+q; 
            System.out.println("mulai mengambil, query:"+q);
            pUsr = conn.prepareStatement  ("insert into "+ tableName+"(content,status) values (?,0)");
            pErr = conn.prepareStatement  ("insert into crawl_log_gagal (log_str) values (?)");
            u = new URL(strUrl); 
            
            URLConnection con = u.openConnection();
            con.setConnectTimeout(30*1000); 
            con.setReadTimeout(30*1000);

            is = con.getInputStream();
            System.out.print("_selesaiopenstream_");  //debug
            
            char[] buffer = new char[1024 * 16]; 
            StringBuilder sbOut = new StringBuilder();
            Reader in = new InputStreamReader(is, "UTF-8");
            int bytesRead;
            while ((bytesRead = in.read(buffer, 0, buffer.length)) != -1) {
            	sbOut.append(buffer, 0, bytesRead);
            }
            
            is.close();
            in.close();
            
            String strOut = sbOut.toString();
            System.out.print("__stringdidapat__");  //debug
            
            //cek apakah diakhiri dengan karakter "}", kalau TIDAK artinya JSON tidak valid, harus diskip
            if (strOut.length()>0) {
               if (strOut.charAt(strOut.length()-1)!='}') {  //bukan JSON
             	  System.out.println("-------------------------->ada kesalahan, format tidak sesuai JSON, ambil ulang <-------------------------");             	  
             	  //nextQuery tidak berubah
             	  //data tidak disimpan
             	  //terjadi karena koneksi terputus ditengah-tengah 
               } else {
            	   //JSON yang valid, ambil next query
            	   nextQuery = "";
                   Scanner sc = new Scanner(strOut);
                   sc.findInLine("\"refresh_url\":\"([^\"]*)\"");  //ambil referesh_url   
                   MatchResult result = sc.match();
          	        if (result.groupCount()>0)  {
          	        	nextQuery = result.group(1);
          	        	//System.out.println("next query (refresh)="+nextQuery);
          	        }
          	        sc.close(); 
                    //isi tabel
          	        System.out.print("__isitabel__");  //debug
                    pUsr.setString(1,strOut);  //status diset 0: belum di crawl untuk diambil friends            
                    pUsr.executeUpdate();
               }
            } else {
         	   //kosong, error juga
            }
            //pUsr.close();    
            //conn.close();
            System.out.println("__selsai__");  //debug
        } catch (Exception e) {
            logger.log(Level.SEVERE, null, e);
            //simpan ke database errornya, tujuannya lebih ke arah mencatat waktu yang 'hilang'. biasanya karena koneksi down
            try {
				pErr.setString(1,e.toString());
				pErr.executeUpdate();
			} catch (SQLException e1) {
				logger.log(Level.SEVERE, null, e1);
				isError = true;
			}
        }
        finally  {
          try  {
                    if (pUsr != null) {pUsr.close();}                
                    if (conn != null) {conn.close();}	
                    if (pErr != null) {pErr.close();} 
                } catch (Exception e) {
                    logger.log(Level.SEVERE, null, e);
                    isError = true; //fatal banget, DB mungkin bermasalah, stop cralwer
           }
        }
    }
    
    public static void main(String[] args) throws InterruptedException {
        
    	
    	//testing    	
    	TwCrawler tw = new TwCrawler();
//    	tw.proxyHost ="cache.itb.ac.id";
//    	tw.proxyPort ="8080";
//    	tw.userNameProxy ="yudi.wibisono";
//    	tw.passwordProxy ="***";
//    	tw.dbName = "localhost/indosattelkomsel";
//    	tw.userName = "yudi3";
//    	tw.password = "rahasia";
//    	tw.query="q=indosat%20OR%20telkomsel&include_entities=true&result_type=recent";
    	
    	tw.dbName = "localhost/obama";
    	tw.userName = "yudi3";
    	tw.password = "rahasia";
    	tw.query="q=obama&include_entities=true&result_type=recent";
    	
    	tw.tableName = "tw_mentah";
    	
    	
    	for (int i=0; i<=10000 ; i++) {
            System.out.println("Proses ke---------------------------------->"+i);
            tw.process();
            //sleep menit
            if (isError) {
                System.out.println("Terjadi kesalahan, distop");
                System.exit(1);
            } else {
                //int jumTidurInMin = 5;
            	System.out.println("Selesai, tidur dulu "); 
            	Thread.sleep(10000);
                //System.out.println("Selesai, tidur dulu "+ jumTidurInMin+ " menit, setelah menyelesaikan iterasi ke "+i);          
                //Thread.sleep(jumTidurInMin * 60000);
            }
        }
    	System.out.println("Selesai semua!");
    }
}


