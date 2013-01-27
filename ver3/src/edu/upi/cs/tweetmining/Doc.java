/*
 *  Copyright (C) 2010 yudi wibisono (yudi1975@gmail.com)
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

import java.util.HashMap;
import java.util.Scanner;
import java.util.logging.Logger;

/**
 *
 * @author yudi wibisono (yudi@upi.edu)
 */
public class Doc {
    protected int numTerm=0;
    protected Cluster oldIdCluster=null;
    protected Cluster idCluster=null;
    public int id=-1;
    public String text;                  //original teks
    public HashMap<String,Double> vector = new HashMap<String,Double>();  //term dengan weightnya
    Logger log = Logger.getLogger("edu.cs.upi.doc");
    
    /**
     *   Memperoses bobot term dalam bentuk 
     *   contoh input:
     *   di=2.1972245773362196;ayo=5.123963979403259;cinta=5.198497031265826;
     */
    public void addTermsInLine(String termLine) {
                String[] str;
                Scanner sc = new Scanner(termLine);
                sc.useDelimiter(";");
                try {
	                while (sc.hasNext()) {
	                        String item = sc.next(); //pasangan term=val
	                        str=item.split("=");
	                        
	                        if (str.length==2) {
//		                        System.out.println("str="+str[0]);
//		                        System.out.println("str="+str[1]);
		                        this.addTerm(str[0], Double.parseDouble(str[1]));
	                        } else {
	                        	System.out.println("ERROR------------->"+item);
	                        }
	                }
                } catch (Exception e) {
                	log.severe(e.toString());
                }    
    }

    public void addTerm(String term, Double weight) {
            numTerm++;
            vector.put(term, weight);
    }

   public void print() {
             System.out.println(this.toString());
   }

   public void clearCluster() {
             oldIdCluster = idCluster;
             idCluster =null;
   }
   
   public static void main(String[] args) {
        //testing   
        for (int i=1;i<=10;i++) {
            System.out.println(i);
        }
            
   }


}


