/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.upi.cs.tweetmining;

import java.io.InputStream;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.util.Scanner;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author user
 */
public class TestProxy {
    private static final Logger logger = Logger.getLogger("yw");
    
    public static void main(String[] args) {
        Handler consoleHandler = new ConsoleHandler();
        consoleHandler.setLevel(Level.FINER);
        logger.setLevel(Level.FINER);
        logger.addHandler(consoleHandler);               
        logger.fine("mulai");
        System.out.println("Testing Proxy");
	URL u;
	InputStream is;
        try  {
            System.setProperty("http.proxyHost","cache.itb.ac.id") ;
            System.setProperty("http.proxyPort", "8080") ;
            Authenticator.setDefault(new ProxyAuth("xxx", "xxx"));
            u = new URL("http://www.yuliadi.com"); 
            is = u.openStream();
            Scanner sc = new Scanner(is);
            while (sc.hasNext()) {
                System.out.println(sc.next());
            }
        } 
        catch (Exception e) {
             logger.log(Level.SEVERE, null, e);
        }    
    }
}
