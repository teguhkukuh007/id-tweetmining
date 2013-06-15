/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.upi.cs.tweetmining;

import java.net.Authenticator;
import java.net.PasswordAuthentication;

/**
 *
 * @author user
 */
public class ProxyAuth extends Authenticator {
    private String user, password;

    public ProxyAuth(String user, String password) {
        this.user = user;
        this.password = password;
    }

    protected PasswordAuthentication getPasswordAuthentication() {
        return new PasswordAuthentication(user, password.toCharArray());
    }
}

