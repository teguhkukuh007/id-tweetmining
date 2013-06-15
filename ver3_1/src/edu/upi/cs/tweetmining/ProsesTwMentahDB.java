/*
 *  Copyright (C) 2012-2013 yudi wibisono (yudi1975@gmail.com/yudi@upi.edu)
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


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;


import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Locale;
import java.util.Map;

/**
 * @author Yudi Wibisono (yudi@upi.edu)
 * 
 * - memproses twitter mentah yang sudah dikumpulkan di database ke dalam field-field yang lebih terstuktur
 * - struktur tabelnya ada di file ini (paling bawah sekali, dibawah code atau di web di: http://pastebin.com/VfvxNFxu) 
 * - dupilikasi tweet (id sama) juga dideteksi
 * 
 *  parameter:
 
      	  harus ada:
      	  
          -q query   	  
    	  -db databasename
    	  -u username
    	  -p password
    	  -delay delay_dalam_detik. Menyatakan delay antar proses. Ini disebakan crawler tidak berjalan terus menerus
    	   sehingga proses ini juga tidak perlu dijalankan terus menerus
    	  
 * 
 * Contoh commandline: G:\LibTweetMining2\bin>java -classpath .;../libs/mysql-connector-java-5.0.8-bin.jar;../libs/jackson-all-1.9.9.jar edu.upi.cs.tweetmining.ProsesTwMentahDB -db localhost/obamarumor -u yudi3 -p rahasia -delay 10
 * 
 * Gunakan class TwCrawler untuk mengumpulkan tweet mentah
 * 
 * 
 * 
 * 
 * 
 */
public class ProsesTwMentahDB{
	static boolean isError = false;  //error yang fatal
	public String dbName;
    public String userName;
    public String password;
    public String tabelTweetJadi;
    public String tabelUserMention;
    public String tabelURL;
    public String tabelMedia;
    public String tabelHashTag;
    public String tabelTweetMentah;
    
            
    private final  Logger logger = Logger.getLogger("Proses tweet mentah ");
    private int jumMasuk=0;
    private int jumDuplikasi=0;
    public int jumDiproses=0;
    
    
    
    //convert created_at twitter yang string menjadi date, ditambah +7 untuk WIB
    public static java.util.Date getTwitterDate(String date) throws ParseException {    	  
    	  java.util.Date dt;    	  
    	  //PERLU DICEK ULANG !! SimpleDateFormat sepertinya buggy, atau saya yg belum ngerti ya :-/
    	  //Thu Dec 23 18:26:07 +0000 2010
    	  //final String TWITTER="EEE MMM dd HH:mm:ss ZZZZZ yyyy";
    	  //Sat, 11 Aug 2012 21:29:00 +0000
    	  //apakah ada kemungkinan berbeda di setiap device tergantung setting locale??   	  
    	  final String TWITTER="EEE, dd MMM yyyy HH:mm:ss ZZZZZ"; 
    	  SimpleDateFormat sf = new SimpleDateFormat(TWITTER,Locale.ENGLISH);;
    	  sf.setLenient(true);
    	  dt = sf.parse(date);
    	  //System.out.println("tgl="+dt);
    	  Calendar cal = Calendar.getInstance(); // creates calendar
    	  cal.setTime(dt);                       // sets calendar time/date
    	  //cal.add(Calendar.HOUR,7);            // tadinya tambah 7  jam untuk jadi WIB, ternyat sudah otomatis     	  
    	  return cal.getTime();
   }
    
   private static void printHashMap(HashMap<String,String> hm) {
   //debug	   
         for (Map.Entry <String,String>entry : hm.entrySet()) {
            String key  = entry.getKey(); 
            String val  = entry.getValue();
            System.out.println(key + "->> " + val);
        }
   }
    
    
    private static void setValLong(String valStr, PreparedStatement pr, int pos) throws SQLException {
            if ((valStr!=null) && (!valStr.equals("null")) ) {
                    long tempLong = Long.valueOf(valStr); 
                    pr.setLong(pos,tempLong);
                } else {
                    pr.setNull(pos,java.sql.Types.BIGINT);
            }
             
    }
    
    
    private void saveTweet(long idTwMentah,HashMap<String,String> hmRoot, HashMap<String,String> hmRes,
                                   ArrayList<HashMap<String,String>> arrMen,ArrayList<HashMap<String,String>> arrUrl,
                                   ArrayList<HashMap<String,String>> arrHt, ArrayList<HashMap<String,String>> arrMedia) {
        
        try  {
            ResultSet rs = null;           
            //cek apakah ID sudah ada di databgase
            Long idTw = Long.valueOf(hmRes.get("id"));
            pIsTweet.setLong(1,idTw);
            rs = pIsTweet.executeQuery();
            if (rs.next()) {
               //sudah ada, batalkan
               jumDuplikasi++;
               return; 
            } else {jumMasuk++;}
            
            String texttw;
            texttw=hmRes.get("text");
            if ((texttw.length()) >= 300 ) {
            	//terlalu panjang (misal mengandung karakter spt &gt terlalu banyak, skip saja
            	//System.out.println("tweet terlalu panjang, skip");
            	return;
            }
            
            long tempLong; 
            pInsertTw.setString(1, hmRes.get("text"));            
            pInsertTw.setLong(2, idTw);            
            pInsertTw.setLong(3, idTwMentah);
            pInsertTw.setString(4,hmRes.get("created_at"));
            
            //isi tanggal versi wib
            java.util.Date tgl = getTwitterDate(hmRes.get("created_at"));           	
           	java.sql.Timestamp tglM = new java.sql.Timestamp(tgl.getTime()) ;
            
            pInsertTw.setString(5,hmRes.get("from_user"));
            tempLong = Long.valueOf(hmRes.get("from_user_id"));
            pInsertTw.setLong(6,tempLong);
            pInsertTw.setString(7,hmRes.get("from_user_name"));
            pInsertTw.setString(8,hmRes.get("profile_image_url"));
            pInsertTw.setString(9,hmRes.get("profile_image_url_https"));
            pInsertTw.setString(10,hmRes.get("source"));
            
            pInsertTw.setString(11,hmRes.get("location"));
            pInsertTw.setString(12,hmRes.get("iso_language_code"));
            pInsertTw.setString(13,hmRes.get("to_user"));
            
            setValLong(hmRes.get("to_user_id"),pInsertTw,14);
            pInsertTw.setString(15,hmRes.get("to_user_name"));
            
            setValLong(hmRes.get("in_reply_to_status_id"),pInsertTw,16);
            pInsertTw.setString(17,hmRes.get("geo_lat"));
            pInsertTw.setString(18,hmRes.get("geo_long"));
            pInsertTw.setString(19,hmRes.get("type"));
            pInsertTw.setTimestamp(20,tglM);//waktu wib
            
            ResultSet generatedKeys = null;
            int affectedRows = pInsertTw.executeUpdate();
            if (affectedRows == 0) {
                throw new SQLException("failed, no rows affected.");
            }
            
            //update tabel detil
            generatedKeys = pInsertTw.getGeneratedKeys();
            if (generatedKeys.next()) {
                long idTwJadi = generatedKeys.getLong(1);
                //System.out.println("key------------------>"+idTwJadi);
                //masuken ke tabel mention,hashtag,url
                //mention
                //1 id_internal_tw_jadi,2 screen_name,3 name,4 user_id
                for (HashMap<String,String> hm: arrMen) {
                    //System.out.println("========");
                    //printHashMap(hm);
                    pInsertMen.setLong(1,idTwJadi);
                    pInsertMen.setString(2,hm.get("screen_name"));
                    pInsertMen.setString(3,hm.get("name"));
                    setValLong(hm.get("id"),pInsertMen,4);                    
                    pInsertMen.addBatch();
                }
                pInsertMen.executeBatch();
                
                for (HashMap<String,String> hmUrl: arrUrl) {
                    //printHashMap(hmUrl);
                    pInsertUrl.setLong(1,idTwJadi);
                    pInsertUrl.setString(2,hmUrl.get("url"));
                    pInsertUrl.setString(3,hmUrl.get("expanded_url"));
                    pInsertUrl.setString(4,hmUrl.get("display_url"));
                    pInsertUrl.addBatch();
                }    
                pInsertUrl.executeBatch();
                //pInsertHt  = conn.prepareStatement("insert into hashtag_banjir(id_internal_tw_jadi,text)"
                for (HashMap<String,String> hmHt: arrHt) {
                    pInsertHt.setLong(1,idTwJadi);
                    pInsertHt.setString(2,hmHt.get("text"));
                    pInsertHt.addBatch();
                }   
                pInsertHt.executeBatch();
                
                //pInsertMedia = conn.prepareStatement("insert into media_banjir
                //(1 id_internal_tw_jadi,2 id,3 media_url,4 media_url_https,5 url,6 display_url,7 expanded_url,8 type"
                //                      + "values (?,?,?,?,?,?,?,?,?)");
                for (HashMap<String,String> hmMedia: arrMedia) {
                    pInsertMedia.setLong(1,idTwJadi);                    
                    setValLong(hmMedia.get("id"),pInsertMedia,2);                     
                    pInsertMedia.setString(3,hmMedia.get("media_url"));
                    pInsertMedia.setString(4,hmMedia.get("media_url_https"));
                    pInsertMedia.setString(5,hmMedia.get("url"));
                    pInsertMedia.setString(6,hmMedia.get("display_url"));
                    pInsertMedia.setString(7,hmMedia.get("expanded_url"));
                    pInsertMedia.setString(8,hmMedia.get("type"));
                    //System.out.println(pInsertMedia.toString());
                    pInsertMedia.addBatch();
                }   
                pInsertMedia.executeBatch();
            } else {
                throw new SQLException("Creating user failed, no generated key obtained.");
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, null, e);
            System.exit(-1);
        }    
    }
    
    private PreparedStatement pInsertTw;
    private PreparedStatement pInsertMen;
    private PreparedStatement pInsertUrl;
    private PreparedStatement pInsertHt;
    private PreparedStatement pInsertMedia;
    private PreparedStatement pIsTweet;
    private PreparedStatement pFlagTwMentah;
    
    public  void process() {
    	
       jumDuplikasi=0;
       jumMasuk=0;
       jumDiproses=0;
    	
       Connection conn=null;       
       PreparedStatement pTw = null;
       try {
           Class.forName("com.mysql.jdbc.Driver");
           
           String strCon = "jdbc:mysql://"+dbName+"?user="+userName+"&password="+password;
           //System.out.println(strCon);
           conn = DriverManager.getConnection(strCon);
           conn.setAutoCommit(false);
           
           pTw  =  conn.prepareStatement ("select id_internal,content from "+ tabelTweetMentah +" where status = 0 limit 2000");   
           
           pInsertTw = 
                   conn.prepareStatement("insert into "+ tabelTweetJadi + " (text,id,id_internal_tw_mentah,created_at,from_user,from_user_id,from_user_name,profile_image_url,profile_image_url_https,source,location,iso_language_code,to_user,to_user_id,to_user_name,in_reply_to_status_id,geo_lat,geo_long,geo_type,created_at_wib)"
                                         + "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ",Statement.RETURN_GENERATED_KEYS);
           
           pInsertMen =
                        conn.prepareStatement("insert into " + tabelUserMention+ " (id_internal_tw_jadi,screen_name,name,user_id)"
                                      + "values (?,?,?,?) ");
           
           pInsertUrl = conn.prepareStatement("insert into "+ tabelURL +"(id_internal_tw_jadi,url,expanded_url,display_url)"
                                      + "values (?,?,?,?) ");
                   
           pInsertHt  = conn.prepareStatement("insert into "+ tabelHashTag +"(id_internal_tw_jadi,text)"
                                      + "values (?,?) ");
           
           pInsertMedia = conn.prepareStatement("insert into "+tabelMedia+"(id_internal_tw_jadi,id,media_url,media_url_https,url,display_url,expanded_url,type)"
                                      + "values (?,?,?,?,?,?,?,?)");
           
           pIsTweet   = conn.prepareStatement("select id_internal  from "+ tabelTweetJadi +" where id = ?");
           
           pFlagTwMentah = conn.prepareStatement("update "+ tabelTweetMentah +" set status=? where id_internal =  ?"); 
           
           ResultSet rsTw = pTw.executeQuery();
           JsonFactory f = new JsonFactory();
           
           while ( rsTw.next())  {
               long idTwMentah = rsTw.getLong(1);
               System.out.println("================>Proses ID (tw_mentah) ke: "+idTwMentah);
               String strTw= rsTw.getString(2);
               
            //cek apakah diakhiri dengan karakter "}", kalau TIDAK artinya JSON tidak valid, harus diskip
            //disable dulu untuk memastikan dari TwCralwer datanya valid
            //TBD: HARUSNYA DIHANDLE DI CRAWLER agar dicrawl ulang, agar tidak kehilangan satu batch tweet
               
             if (strTw.length()>0) {
                  if ((strTw.charAt(strTw.length()-1)!='}') || (strTw.charAt(strTw.length()-2)!='"')) {  //bukan JSON
                	  System.out.println("ada kesalahan, format tidak sesuai JSON, skip");
                	  pFlagTwMentah.setLong(1,99);  //error
                	  pFlagTwMentah.setLong(2,idTwMentah);   
                	  pFlagTwMentah.executeUpdate();  
                	  continue;
                  }
               } else {
            	   //kosong, error juga, skip
            	   continue;
             }
               
               
               //System.out.println(strTw);
               //proses
               JsonParser jp = f.createJsonParser(strTw);
               if (jp.nextToken() != JsonToken.START_OBJECT) {
                   //throw new IOException("Expected data to start with an Object");
                   pFlagTwMentah.setLong(1,99);  //error
                   pFlagTwMentah.setLong(2,idTwMentah);   
                   pFlagTwMentah.executeUpdate();  
                   continue;  
               } 
               HashMap<String,String> hmKeyValRoot = new HashMap<String,String>();    
               HashMap<String,String> hmKeyValRes  = new HashMap<String,String>(); //result, kumpulan tweet ada disini 
               
               //parsing 
               String valText="";
               while (jp.nextToken() != JsonToken.END_OBJECT) {
                   String fieldName = jp.getCurrentName();              
                   if (fieldName.equals("results")) {
                   
                       jp.nextToken(); //start_arr
                       //System.out.println("masuk results");
                       while (jp.nextToken() != JsonToken.END_ARRAY) {        //end array result (satu result terdiri dari array objek tweet)
//                           System.out.println("Proses Tweet==========================================>");
//                           System.out.println("idTwMentah:"+idTwMentah);
                           ArrayList<HashMap<String,String>> arrMen = new ArrayList<HashMap<String,String>>();
                           ArrayList<HashMap<String,String>> arrUrl = new ArrayList<HashMap<String,String>>();
                           ArrayList<HashMap<String,String>> arrHt  = new ArrayList<HashMap<String,String>>();
                           ArrayList<HashMap<String,String>> arrMedia = new ArrayList<HashMap<String,String>>();
                           
                           while (jp.nextToken() != JsonToken.END_OBJECT) {   //object setiap tweet di dalam result
                               String fieldNameRes = jp.getCurrentName();
                               if (fieldNameRes.equals("entities")) {
                                   //System.out.println("masuk entities");
                                   jp.nextToken(); //start_obj
                                   while (jp.nextToken() != JsonToken.END_OBJECT) {  //END OF ENTITIES
                                           String fieldNameEnt = jp.getCurrentName();                                       
                                           if (fieldNameEnt.equals("media"))  {
                                               //System.out.println("masuk media");
                                               jp.nextToken(); //start array
                                               while (jp.nextToken() != JsonToken.END_ARRAY) {
                                                    //System.out.println("med#");
                                                    HashMap<String,String> hmKeyValMedia = new HashMap<String,String>();
                                                    while (jp.nextToken() != JsonToken.END_OBJECT) {
                                                         String fieldNameMed = jp.getCurrentName();
                                                         if (fieldNameMed.equals("indices")) {
                                                            while (jp.nextToken() != JsonToken.END_ARRAY) { } // skip dulu
                                                         } else
                                                         if (fieldNameMed.equals("sizes")) {
                                                             //System.out.println("masuk sizes");
                                                             boolean stop = false;
                                                             int cc=0;
                                                             StringBuilder sb = new StringBuilder();
                                                             while (!stop) {
                                                                 JsonToken jt =  jp.nextToken();
                                                                 if (jt==JsonToken.START_OBJECT) {
                                                                     cc++;
                                                                 } else if (jt==JsonToken.END_OBJECT) {
                                                                     cc--;
                                                                     if (cc==0) {stop = true;}
                                                                 } else {
                                                                     valText = jp.getText();
                                                                     sb.append(valText);
                                                                     sb.append(";");
                                                                 }
                                                             }
                                                             hmKeyValMedia.put("sizes",sb.toString());
                                                             //System.out.println("Media--> Sizes ="+sb.toString());                                                             
                                                         }
                                                         else {
                                                             jp.nextToken();// Let's move to value
                                                             valText = jp.getText();
                                                             //System.out.println("Media-->"+fieldNameMed+"="+valText);
                                                             hmKeyValMedia.put(fieldNameMed,valText);
                                                         }
                                                     }
                                                     arrMedia.add(hmKeyValMedia);
                                                }   
                                           } else 
                                           if  (fieldNameEnt.equals("hashtags")) {
                                               jp.nextToken(); //start array 
                                               while (jp.nextToken() != JsonToken.END_ARRAY) {
                                                           
                                                   HashMap<String,String> hmKeyValHashtag = new HashMap<String,String>();  
                                                   while (jp.nextToken() != JsonToken.END_OBJECT) {
                                                         String fieldNameHt = jp.getCurrentName();
                                                         if (fieldNameHt.equals("indices")) {
                                                            while (jp.nextToken() != JsonToken.END_ARRAY) { } // skip dulu
                                                         } else {
                                                             jp.nextToken();// Let's move to value
                                                             valText = jp.getText();
                                                             //System.out.println("hashtag-->"+fieldNameHt+"="+valText);
                                                             hmKeyValHashtag.put(fieldNameHt,valText);
                                                         }
                                                    }
                                                    arrHt.add(hmKeyValHashtag);
                                                 }  
                                           } else
                                           if  (fieldNameEnt.equals("urls")) {
                                                jp.nextToken(); //start array 
                                                while (jp.nextToken() != JsonToken.END_ARRAY) {
                                                    HashMap<String,String> hmKeyValUrl     = new HashMap<String,String>(); 
                                                    while (jp.nextToken() != JsonToken.END_OBJECT) {
                                                         String fieldNameUrl = jp.getCurrentName();
                                                         if (fieldNameUrl.equals("indices")) {
                                                            while (jp.nextToken() != JsonToken.END_ARRAY) { } // sekip dulu
                                                         } else {
                                                             jp.nextToken();// Let's move to value
                                                             valText = jp.getText();
                                                             //System.out.println("url-->"+fieldNameUrl+"="+valText);
                                                             hmKeyValUrl.put(fieldNameUrl,valText);
                                                         }
                                                     }
                                                     arrUrl.add(hmKeyValUrl);
                                                 }  
                                           }
                                           else 
                                           if (fieldNameEnt.equals("user_mentions")) { 
                                               jp.nextToken(); //start array
                                               //System.out.println("masuk user mention");
                                               while (jp.nextToken() != JsonToken.END_ARRAY) {  // END ARRAY USER MENTION
                                                   HashMap<String,String> hmKeyValMen = new HashMap<String,String>(); //mention 
                                                   while (jp.nextToken() != JsonToken.END_OBJECT) { 
                                                        String fieldNameMen = jp.getCurrentName();
                                                        if (fieldNameMen.equals("indices")) {
                                                            while (jp.nextToken() != JsonToken.END_ARRAY) { } // sekip dulu
                                                        } else {
                                                            jp.nextToken();// Let's move to value
                                                            valText = jp.getText();
                                                            //System.out.println("men-->"+fieldNameMen+"="+valText);
                                                            hmKeyValMen.put(fieldNameMen,valText);
                                                        }
                                                    }
                                                    arrMen.add(hmKeyValMen);
                                               } //end array user mention
                                           } //mention
                                           else  { //di dalam enties, selain user mention,url, hashtag
                                               jp.nextToken();// Let's move to value
                                               valText = jp.getText();
                                               //System.out.println("entities->"+fieldName+"="+valText);
                                               //tidak ada yg lain
                                               
                                           }
                                    } //end of entities
                                    } else { //di dalam resulsts selain user entities
                                    if (fieldNameRes.equals("geo")) {
                                       //System.out.println("masuk geo");
                                       jp.nextToken(); //start_obj
                                       String tempStr = jp.getText();
                                       if (!tempStr.equals("null")) {
                                           while (jp.nextToken() != JsonToken.END_OBJECT) {
                                              String fieldNameGeo = jp.getCurrentName();
                                              if (fieldNameGeo.equals("coordinates")) {
                                                  jp.nextToken(); //start_arr
                                                  jp.nextToken(); 
                                                  String geoLat = jp.getText();
                                                  jp.nextToken();
                                                  String geoLong = jp.getText();
                                                  jp.nextToken();
                                                 // System.out.println("geo-->koordinat geo:"+geoLat+","+geoLong);
                                                  hmKeyValRes.put("geo_lat",geoLat);
                                                  hmKeyValRes.put("geo_long",geoLong);
                                              } else {
                                                    jp.nextToken();// Let's move to value
                                                    valText = jp.getText();
                                                   // System.out.println("geo-->"+fieldNameGeo+"="+valText);
                                                    hmKeyValRes.put(fieldNameGeo,valText);
                                              }  
                                           } //while
                                       }
                                    } else
                                    if (fieldNameRes.equals("metadata")) {
                                               //System.out.println("masuk metadat");
                                               jp.nextToken(); //start_obj
                                               while (jp.nextToken() != JsonToken.END_OBJECT) {} //skip metadata
                                    } else  {
                                               jp.nextToken();
                                               valText = jp.getText(); 
                                               //System.out.println("res->"+fieldNameRes+"="+valText);
                                               hmKeyValRes.put(fieldNameRes,valText);
                                    }
                                    }  //else
                          } //end object result
                          //selesai satu tweet, simpan
                           saveTweet(idTwMentah,hmKeyValRoot, hmKeyValRes,arrMen,arrUrl,arrHt,arrMedia); 
                           
                       } //end array result
                       
                       //flag table untuk menandakan sudah diproses
                       pFlagTwMentah.setLong(1,1);
                       pFlagTwMentah.setLong(2,idTwMentah);   
                       pFlagTwMentah.executeUpdate();
                   } else //bukan results
                   {
                       //masukan pasangan field val
                       jp.nextToken();// Let's move to value
                       valText = jp.getText();
                       //System.out.println("root->"+fieldName+"="+valText);
                       hmKeyValRoot.put(fieldName,valText);
                   }    
               }  //end OBJ ROOT
               jp.close();
           } //while ( rsTw.next())
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
               isError = true;
           }
       }   
       finally {
           try {
               pInsertMen.close();
               pInsertUrl.close();
               pInsertHt.close();
               pInsertTw.close();
               pInsertMedia.close();
               pIsTweet.close();
               pFlagTwMentah.close();
               pTw.close();
               conn.commit();
               conn.setAutoCommit(true);
               conn.close();
           } catch (Exception e) {
               logger.log(Level.SEVERE, null, e);
               isError = true;
           }    
       }
    }
    
    
    public static void main(String[] args) {
    	
    	//proses param 
    	String strDb=""; 
    	String strDbUser="";
    	String strDbPass="";
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
	    		if (arrDetPar[0].equals("u")) {
	    			strLast="u";
	    			strDbUser = arrDetPar[1]; 	    			
	    		} else 
	    		if (arrDetPar[0].equals("p")) {
	    			strLast="p";
	    			strDbPass = arrDetPar[1];
	    		} else
			    if (arrDetPar[0].equals("delay")) {
			    		strLast="delay";
			    		strDelay = arrDetPar[1]; 	    			
				}
		    	else 
	    		{
	    			//parameter tidak dikenal
	    			System.out.println("Error parameter tdk dikenal : "+par);
	    		}
    		}
    		catch (Exception e) {
    			System.out.println("parameter salah pada bagian:" +strLast+" Gunakan parameter dengan format:");
    			System.out.println("-db databasename -u dbusername -p dbpassword -delay delay_antar_query_dlm_detik");
    			System.out.println("Contoh:");
    			System.out.println("G:\\LibTweetMining2\\bin>java -classpath .;../libs/mysql-connector-java-5.0.8-bin.jar;../libs/jackson-all-1.9.9.jar edu.upi.cs.tweetmining.ProsesTwMentahDB -db localhost/obamarumor -u yudi3 -p rahasia -delay 10");
    			System.exit(1);
    		}
    	}
    	
    	if (strDb.equals("") || strDbUser.equals("") || strDbPass.equals("")) {
    		System.out.println("parameter tidak lengkap db, dbuser dan dpass harus ada! Gunakan parameter dengan format:");
    		System.out.println("-db databasename -u dbusername -p dbpassword -delay delay_antar_query_dlm_detik");
			System.out.println("Contoh:");
			System.out.println("G:\\LibTweetMining2\\bin>java -classpath .;../libs/mysql-connector-java-5.0.8-bin.jar;../libs/jackson-all-1.9.9.jar edu.upi.cs.tweetmining.ProsesTwMentahDB -db localhost/obamarumor -u yudi3 -p rahasia -delay 10");
    		System.exit(1);
    	}
    	
    	if (strDelay.equals("")) {
    		strDelay = "60";}
    	
    	System.out.println("db-->"+strDb);
    	System.out.println("dbuser-->"+strDbUser);
    	System.out.println("dbpass-->"+strDbPass);
    	System.out.println("delay-->"+strDelay);
    	System.out.println("Struktur DB yang harus disiapkan dapat dilihat di: http://pastebin.com/VfvxNFxu ");
    	
    	ProsesTwMentahDB ptm = new ProsesTwMentahDB();
    	
    	ptm.dbName = strDb;
    	ptm.userName = strDbUser;
    	ptm.password = strDbPass;
    	
    	ptm.tabelTweetJadi = "tw_jadi";
    	ptm.tabelUserMention ="user_mention";
    	ptm.tabelURL = "url";
    	ptm.tabelMedia = "media";
    	ptm.tabelHashTag = "hashtag";
    	ptm.tabelTweetMentah = "tw_mentah";
    	
    	int delay = Integer.parseInt(strDelay); //delay antar query dalam detik
    	
    	int jumEventDupTinggi = 0; //jumlah kejadian dimana duplikasi melebihi 75%
    	
    	for (int i=0;;i++) {
            System.out.println("Proses ke---------------------------------->"+i);
            ptm.process();
            
            System.out.println("Jum masuk="+ptm.jumMasuk);
            System.out.println("Jum duplikasi="+ptm.jumDuplikasi);
            double rasio = 100 * (double) ptm.jumMasuk/(ptm.jumMasuk+ptm.jumDuplikasi);
            System.out.println("Rasio data yg masuk (pct)="+rasio+"%");  //makin tinggi artinya kurang cepat/kurang banyak mengambil, makin kecil berarti delay terlalu pendek
            
            if (rasio>75) {
            	jumEventDupTinggi++; 
            }
            double jumEventDupRasio = 100 * (double) jumEventDupTinggi/(i+1);
            System.out.println("persentase jumlah kejadian duplikasi >75%:    "+jumEventDupRasio);
            System.out.println("");
            
            if (isError) {
                System.out.println("Terjadi kesalahan fatal (database), distop");
                System.exit(1);
            } else {
                //int jumTidurInMin = 5;
            	System.out.println("Selesai, tidur dulu "+delay+" detik"); 
            	try {
					Thread.sleep(delay*1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					System.out.println("thread sleep error");
					e.printStackTrace();
				} 
            }
        }
    	//System.out.println("Selesai semua!");
	
//    	ptm.dbName = "localhost/indosattelkomsel";
//    	ptm.userName = "yudi3";
//    	ptm.password = "rahasia";

//    	ptm.dbName = "localhost/masterchef2";
//    	ptm.userName = "yudi3";
//    	ptm.password = "rahasia";

    	//ptm.process();
    	
        
    }
    
    
    /*
     * 
     * 
     *
     *
    
--membersihkan    
update tw_mentah set status=0;
delete from tw_jadi;    

--    
drop  hashtag;
drop tw_jadi;
drop url;
drop user_mention;

     * 
//untuk membersihkan
  
delete from hashtag;
delete from tw_jadi;
delete from url;
delete from user_mention;





CREATE TABLE IF NOT EXISTS `hashtag` (
  `id_internal` bigint(20) NOT NULL AUTO_INCREMENT,
  `id_internal_tw_jadi` bigint(20) NOT NULL,
  `text` varchar(150) NOT NULL,
  PRIMARY KEY (`id_internal`),
  KEY `id_interntal_tw` (`id_internal_tw_jadi`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Data exporting was unselected.



CREATE TABLE IF NOT EXISTS `keywords_stat_per_jam` (
  `id_internal` bigint(20) NOT NULL AUTO_INCREMENT,
  `id_stat_per_jam` bigint(20) NOT NULL DEFAULT '0',
  `keywords` tinytext NOT NULL,
  PRIMARY KEY (`id_internal`),
  KEY `id_stat_per_jam` (`id_stat_per_jam`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=COMPACT COMMENT='keywords yang merepresentasikan tweet dalam jam terentu. Harusnya diisi oleh algoritma yang mencari tweet yang paling relevan dan penting';

-- Data exporting was unselected.



CREATE TABLE IF NOT EXISTS `media` (
  `id_internal` bigint(20) NOT NULL AUTO_INCREMENT,
  `id` bigint(20) NOT NULL,
  `media_url` varchar(300) NOT NULL,
  `media_url_https` varchar(300) NOT NULL,
  `url` varchar(400) NOT NULL,
  `display_url` varchar(100) DEFAULT NULL,
  `expanded_url` varchar(600) NOT NULL,
  `type` varchar(50) NOT NULL,
  `id_internal_tw_jadi` bigint(20) NOT NULL,
  PRIMARY KEY (`id_internal`),
  KEY `id_internal_tw` (`id_internal_tw_jadi`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Data exporting was unselected.



CREATE TABLE IF NOT EXISTS `stat_per_hari` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `tgl_jam` datetime NOT NULL,
  `is_loaded` int(11) NOT NULL DEFAULT '0',
  `tahun` int(11) NOT NULL,
  `bulan` int(11) NOT NULL,
  `hari` int(11) NOT NULL,
  `kelas` int(11) NOT NULL,
  `jum` int(11) NOT NULL,
  `keywords` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `tahun` (`tahun`),
  KEY `bulan` (`bulan`),
  KEY `hari` (`hari`),
  KEY `kelas` (`kelas`),
  KEY `TGL_WAKTU` (`tgl_jam`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ROW_FORMAT=COMPACT;

-- Data exporting was unselected.



CREATE TABLE IF NOT EXISTS `stat_per_jam` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `tgl_jam` datetime NOT NULL,
  `is_loaded` int(11) NOT NULL DEFAULT '0',
  `is_loaded_rep_tw` int(11) NOT NULL DEFAULT '0',
  `is_loaded_rep_keywords` int(10) DEFAULT '0',
  `tahun` int(11) NOT NULL,
  `bulan` int(11) NOT NULL,
  `hari` int(11) NOT NULL,
  `jam` int(11) NOT NULL,
  `kelas` int(11) NOT NULL,
  `jum` int(11) NOT NULL,
  `old_jum` int(11) NOT NULL DEFAULT '0',
  `keywords` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `tahun` (`tahun`),
  KEY `bulan` (`bulan`),
  KEY `hari` (`hari`),
  KEY `jam` (`jam`),
  KEY `kelas` (`kelas`),
  KEY `TGL_WAKTU` (`tgl_jam`),
  KEY `is_loaded` (`is_loaded`),
  KEY `is_loaded_rep_tw` (`is_loaded_rep_tw`),
  KEY `is_loaded_rep_keywords` (`is_loaded_rep_keywords`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Data exporting was unselected.



CREATE TABLE IF NOT EXISTS `tw_jadi` (
  `id_internal` bigint(20) NOT NULL AUTO_INCREMENT,
  `id_internal_tw_mentah` bigint(20) NOT NULL,
  `id` bigint(20) NOT NULL,
  `is_class_defined` int(11) NOT NULL DEFAULT '0',
  `kelas` int(11) NOT NULL DEFAULT '-1' COMMENT '0: nonopini, 1: negatif, 2:positif',
  `created_at` varchar(50) NOT NULL,
  `created_at_wib` datetime NOT NULL COMMENT 'versi datetime, dengan gmt+7',
  `is_loaded` int(11) NOT NULL DEFAULT '0' COMMENT 'sudah diload dari database',
  `from_user` varchar(50) NOT NULL,
  `from_user_id` bigint(20) NOT NULL,
  `from_user_name` varchar(50) NOT NULL,
  `location` varchar(50) DEFAULT NULL,
  `keywords` varchar(200) DEFAULT NULL,
  `bobot` double DEFAULT NULL,
  `iso_language_code` char(10) DEFAULT NULL,
  `profile_image_url` varchar(400) NOT NULL,
  `profile_image_url_https` varchar(400) NOT NULL,
  `source` varchar(200) NOT NULL,
  `text` varchar(300) NOT NULL,
  `to_user` varchar(100) DEFAULT NULL,
  `to_user_id` bigint(20) DEFAULT NULL,
  `to_user_name` varchar(100) DEFAULT NULL,
  `in_reply_to_status_id` bigint(20) DEFAULT NULL,
  `geo_coordinate` varchar(100) DEFAULT NULL,
  `geo_type` varchar(50) DEFAULT NULL,
  ` time_stamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `geo_lat` varchar(30) DEFAULT NULL,
  `geo_long` varchar(30) DEFAULT NULL,
  `text_prepro` varchar(200) DEFAULT NULL,
  `is_prepro` int(11) NOT NULL DEFAULT '0',
  `is_duplicate` int(11) DEFAULT '0',
  `is_duplicate_checked` int(11) DEFAULT '0',
  PRIMARY KEY (`id_internal`),
  UNIQUE KEY `id` (`id`,`from_user_id`),
  KEY `from_user_id` (`from_user_id`),
  KEY `is_class_defined` (`is_class_defined`,`kelas`),
  KEY `is_prepro` (`is_prepro`),
  KEY `created_at_wib` (`created_at_wib`),
  KEY `is_loaded` (`is_loaded`),
  KEY `text_prepro` (`text_prepro`),
  KEY `is_duplicate` (`is_duplicate`),
  KEY `is_duplicate_checked` (`is_duplicate_checked`),
  KEY `id_internal_tw_mentah` (`id_internal_tw_mentah`),
  KEY `bobot` (`bobot`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT;

-- Data exporting was unselected.


-- Dumping structure for table indosat1.tw_jadi_flag_duplicate
CREATE TABLE IF NOT EXISTS `tw_jadi_flag_duplicate` (
  `id_internal` bigint(20) NOT NULL AUTO_INCREMENT,
  `id_internal_tw_mentah` bigint(20) NOT NULL,
  `id` bigint(20) NOT NULL,
  `is_class_defined` int(11) NOT NULL DEFAULT '0',
  `kelas` int(11) NOT NULL DEFAULT '-1' COMMENT '0: nonopini, 1: negatif, 2:positif',
  `created_at` varchar(50) NOT NULL,
  `created_at_wib` datetime NOT NULL COMMENT 'versi datetime, dengan gmt+7',
  `is_loaded` int(11) NOT NULL DEFAULT '0' COMMENT 'sudah diload dari database',
  `from_user` varchar(50) NOT NULL,
  `from_user_id` bigint(20) NOT NULL,
  `from_user_name` varchar(50) NOT NULL,
  `location` varchar(50) DEFAULT NULL,
  `iso_language_code` char(10) DEFAULT NULL,
  `profile_image_url` varchar(400) NOT NULL,
  `profile_image_url_https` varchar(400) NOT NULL,
  `source` varchar(200) NOT NULL,
  `text` varchar(200) NOT NULL,
  `to_user` varchar(100) DEFAULT NULL,
  `to_user_id` bigint(20) DEFAULT NULL,
  `to_user_name` varchar(100) DEFAULT NULL,
  `in_reply_to_status_id` bigint(20) DEFAULT NULL,
  `geo_coordinate` varchar(100) DEFAULT NULL,
  `geo_type` varchar(50) DEFAULT NULL,
  ` time_stamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `geo_lat` varchar(30) DEFAULT NULL,
  `geo_long` varchar(30) DEFAULT NULL,
  `text_prepro` varchar(200) DEFAULT NULL,
  `is_prepro` int(11) NOT NULL DEFAULT '0',
  `is_duplicate` int(11) DEFAULT '0',
  `is_duplicate_checked` int(11) DEFAULT '0',
  PRIMARY KEY (`id_internal`),
  UNIQUE KEY `id` (`id`,`from_user_id`),
  KEY `from_user_id` (`from_user_id`),
  KEY `is_class_defined` (`is_class_defined`,`kelas`),
  KEY `is_prepro` (`is_prepro`),
  KEY `created_at_wib` (`created_at_wib`),
  KEY `is_loaded` (`is_loaded`),
  KEY `text_prepro` (`text_prepro`),
  KEY `is_duplicate` (`is_duplicate`),
  KEY `is_duplicate_checked` (`is_duplicate_checked`),
  KEY `id_internal_tw_mentah` (`id_internal_tw_mentah`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Data exporting was unselected.


CREATE TABLE IF NOT EXISTS `tw_stat_per_jam` (
  `id_internal` bigint(20) NOT NULL AUTO_INCREMENT,
  `id_stat_per_jam` bigint(20) NOT NULL DEFAULT '0',
  `id_tw_jadi` bigint(20) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id_internal`),
  KEY `id_stat_per_jam` (`id_stat_per_jam`),
  KEY `id_tw_jadi` (`id_tw_jadi`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT='tweet yang merepresentasikan tweet dalam jam terentu. Harusnya diisi oleh algoritma yang mencari tweet yang paling relevan dan penting';

-- Data exporting was unselected.



CREATE TABLE IF NOT EXISTS `url` (
  `id_internal` bigint(20) NOT NULL AUTO_INCREMENT,
  `id_internal_tw_jadi` bigint(20) NOT NULL,
  `url` varchar(600) NOT NULL,
  `expanded_url` varchar(600) DEFAULT NULL,
  `display_url` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`id_internal`),
  KEY `id_interntal_tw` (`id_internal_tw_jadi`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Data exporting was unselected.


CREATE TABLE IF NOT EXISTS `user_mention` (
  `id_internal` bigint(20) NOT NULL AUTO_INCREMENT,
  `id_internal_tw_jadi` bigint(20) NOT NULL,
  `screen_name` varchar(50) NOT NULL,
  `name` varchar(75) NOT NULL,
  `user_id` bigint(20) NOT NULL,
  PRIMARY KEY (`id_internal`),
  KEY `id_interntal_tw` (`id_internal_tw_jadi`),
  KEY `user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



-- copy-psate sampai disini
     
 */
}
