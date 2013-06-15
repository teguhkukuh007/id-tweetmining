/*
 *  Copyright (C) 2010 yudi wibisono (yudi1975@gmail.com/cs.upi.edu)
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

import java.util.ArrayList;

/**
 * representasi cluster hasil dari clustering
 * @author yudi wibisono (yudi@upi.edu)
 */


public abstract class Cluster {
   public long idCluster;
   public ArrayList<Doc> alDoc = new ArrayList<Doc>();  //
   public boolean flag=false;                           //untuk berbagai keperluan, menandai kalau sudah diproses etc
   /*
    *    tambah isi cluster c ke dalam this
    */
   public void mergeCluster(Cluster c) {
	   for (Doc d:c.alDoc) {
		   this.addDoc(d);
	   }
   }
   
   
   /**
         * add doc to cluster
         * @param d
   */
   public void addDoc(Doc d) {
            d.idCluster = this;
            alDoc.add(d);
   }

   //clear member
   public void clear() {
            for (Doc d:alDoc) {
                d.clearCluster();
            }
            alDoc.clear();
   }

   public void print() {
            System.out.println(this.toString());
   }

   public abstract double innerQualityScore(); 
   
   public abstract String getLabel();

   @Override
   public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("\nIDCluster:").append(idCluster).append("\n");
            sb.append("Label"+"\n");
            sb.append(this.getLabel());
            sb.append("\n");
            for (Doc d : alDoc) {
                sb.append(d.text).append("\n");
                //sb.append(d.toString()).append("\n");  //debug,
            }
            return sb.toString();
    }

//    public String printWithRowNum() {
//            StringBuilder sb = new  StringBuilder();
//            sb.append("\nIDCluster:").append(idCluster).append("\n");
//            sb.append("Label"+"\n");
//            sb.append(this.getLabel());
//            sb.append("\n");
//            sb.append("<start>\n");
//            for (Doc d : alDoc) {
//                sb.append(d.rowNum).append("|").append(d.text).append("\n");
//                //sb.append(d.toString()).append("\n");  //debug,
//            }
//            sb.append("<end>\n");
//            return (sb.toString());
//    }
    


    public Cluster(long idCluster) {
       this.idCluster = idCluster;
    }
    
    
    public Cluster() {
    }
}
