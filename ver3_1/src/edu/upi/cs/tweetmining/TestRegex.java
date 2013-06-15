package edu.upi.cs.tweetmining;

import java.util.Scanner;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestRegex {
	public static void main(String[] args)  {
		
		String msg = "gilaa 232342322222 sdg!!! @@@@@  @caaaaacacacaaaa #hahahahaaaa lkjas  giilaa!!!  helo???    11111111a   asldfkj las ssss ladkjsf xxxdfsd saat asdfcccccccccccccccc";
		msg = msg.replaceAll("([!?\\w])\\1{1,}", "$1");  //buang #xxxx hashtag
        System.out.println(msg);
		
		
//		Pattern pat = 
//		Pattern.compile("[\\s]*");
//		
//		Matcher matcher = 
//		pat.matcher("@gilaa sdg!!! lkjas     11111111a   asldfkj las ssss ladkjsf xxxdfsd saat asdfcccccccccccccccc");
//
//	    boolean found = false;
//	    while (matcher.find()) {
//	       System.out.println(matcher.group());	                
//	       found = true;
//	    }
//	    if(!found){
//	                System.out.println("No match found.%n");
//	    }
		
		
//		 String tw ="gilaaa betul!!!";		
//		//String tw2 ="\"next_page\":\"hello\"";
//		 
//		
//		 Scanner s = new Scanner(tw);
//	     s.findInLine("([a-zA-Z! ]*)\2");  //ambil next page	     
//	     MatchResult result = s.match();
//	     for (int i=1; i<=result.groupCount(); i++) {
//	         System.out.println(result.group(i));
//	     }
//	     System.out.println("selesai");
	     
//	     s.findInLine("\"refresh_url\":\"([^\"]*)\"");  //ambil referesh_url   
//	     result = s.match();
//	     for (int i=1; i<=result.groupCount(); i++) {
//	         System.out.println(result.group(i));
//	     }
	     
	    // s.close(); 
		
		
//		String tw ="{\"completed_in\":0.111,\"max_id\":234258277326786560,\"max_id_str\":\"234258277326786560\",\"next_page\":\"?page=2&max_id=234258277326786560&q=jokowi&rpp=50&include_entities=1&result_type=recent\",\"page\":1,\"query\":\"jokowi\",\"refresh_url\":\"?since_id=234258277326786560&q=jokowi&result_type=recent&include_entities=1\"";
//		
//		String tw2 ="\"next_page\":\"hello\"";
//		
//		
//		 Scanner s = new Scanner(tw);
//	     s.findInLine("\"next_page\":\"([^\"]*)\"");  //ambil next page	     
//	     MatchResult result = s.match();
//	     for (int i=1; i<=result.groupCount(); i++) {
//	         System.out.println(result.group(i));
//	     }
//	     
//	     s.findInLine("\"refresh_url\":\"([^\"]*)\"");  //ambil referesh_url   
//	     result = s.match();
//	     for (int i=1; i<=result.groupCount(); i++) {
//	         System.out.println(result.group(i));
//	     }
//	     
//	     s.close(); 
		
		
		
		//Pattern pat = Pattern.compile("next_page\":\"(\"\\w+\")");
//		Pattern pat = Pattern.compile("next_page(\\w+)");
//	   	Matcher m = pat.matcher(tw);
//		
//	   	boolean found = false;
//        while (m.find()) {
//            System.out.println(m.group());
//            found = true;
//        }
//        if(!found){
//            System.out.println("No match found");
//        }
//	   	
	}
}
