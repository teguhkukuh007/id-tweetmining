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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

/**
 *
 * @author yudi wibisono  (yudi@upi.edu)
 */


/**
     *  representasi tweet (bisa juga untuk dokumen lain sebenarnya)
     *  
    */


public class DocKMeans extends Doc{
         private double sumSqrWeight=0;       //sum(sqrweight)
         private double sqrtSumSqrWeight=0;   //sqr(sumSqrWeight) valid setelah calcSqrtSumSqrWeight dipanggil

         public boolean  isJunk() {           //doc tidak layak di cluster
             return (numTerm<2);
         }

         public double getSqrtSqrWeight() {
             return sqrtSumSqrWeight;
         }

         public void calcSqrtSumSqrWeight() {
             sqrtSumSqrWeight = Math.sqrt(sumSqrWeight);
         }

        @Override
        public void addTerm(String term, Double weight) {
            super.addTerm(term,weight);
            sumSqrWeight += (weight * weight);
         }

         //hanya mencetak 10 term terbaik saja
         public String bestToString() {

             StringBuilder sb = new StringBuilder();
             //sorting vector disort dulu
             ArrayList <TermStat> alTS = new ArrayList <TermStat>();
             for (Map.Entry<String,Double> entry : vector.entrySet()) {
                 alTS.add(new TermStat(entry.getKey(),entry.getValue()));
             }
             Collections.sort(alTS, new TermStatComparable());
             //endsort
             double val; int cc=0;
             for (TermStat ts : alTS) {
                 cc++;
                 if (cc>10) {break;}
                 val = ts.getTotVal();
                 //sb.append(ts.term).append("=").append(String.format("%4.2f", val)).append(";"); //dengan weightnya-nya
                 sb.append(ts.term).append(";"); //weigthnya tidak dprint
             }
             sb.append("\n");
             return sb.toString();
         }

         @Override
         public String toString() {
             StringBuilder sb = new StringBuilder();

             //untuk debug, totWeight dan avgWeigth
             double totWeight =0;
             double avgWeight =0;
             double cc=0;

             //sorting vector disort dulu
             ArrayList <TermStat> alTS = new ArrayList <TermStat>();
             for (Map.Entry<String,Double> entry : vector.entrySet()) {
                 cc++;
                 alTS.add(new TermStat(entry.getKey(),entry.getValue()));
                 totWeight += entry.getValue();
             }
             avgWeight = totWeight/cc;
             Collections.sort(alTS, new TermStatComparable());
             //endsort

             //print term dengan weightnya
             double val; cc=0;
             for (TermStat ts : alTS) {
                 cc++;

                 val = ts.getTotVal();
                 sb.append(ts.term).append("=").append(String.format("%4.2f", val)).append(";");
                 if ((cc % 5)==0) {  //setiap lima baris, cr supaya bisa dibaca
                     sb.append("\n");
                 }
             }
             sb.append("\n");

             sb.append(String.format(" avg Weight: %4.2f \n",avgWeight));
             return sb.toString();
         }

         double similar(DocKMeans otherDoc) {
             //kesamaan antara dua doc
             //rumus kedekatan dua tweets   (sum (x[i] x y[i]) / (  sqr(sum(x[i]^2)) x sqr(sum(y[i]^2))  )  --> cosine similiarity
             //1 paling dekat, 0 paling jauh
        	 
        	 double simVal =0;
             double pembagi =0;
             this.calcSqrtSumSqrWeight();
             otherDoc.calcSqrtSumSqrWeight();

             double sumXY=0;
             for (Map.Entry<String,Double> thisEntry : this.vector.entrySet())  {   //loop untuk semua term di this
                 Double val =  otherDoc.vector.get(thisEntry.getKey());  //cari term yg sama di D
                 if (val!=null) {  //ada, dikali
                     sumXY += (val * thisEntry.getValue());
                 }
             }

             pembagi = (this.sqrtSumSqrWeight * otherDoc.sqrtSumSqrWeight);  //mencegah NaN
             if (pembagi!=0) {
                 simVal = sumXY / pembagi;
             } else {
                simVal = 0; //error salah satu doc semua bobotnya nol, dianggap berjauhan, perlu smoothing?
             }

             return simVal;
         }

         //berdasarkan totVal
        public class TermStatComparable implements Comparator<TermStat>{
            @Override
            public int compare(TermStat o1, TermStat o2) {
                    return (o1.getTotVal()>o2.getTotVal() ? -1 : (o1.getTotVal()==o2.getTotVal() ? 0 : 1));
            }
        }

     }






