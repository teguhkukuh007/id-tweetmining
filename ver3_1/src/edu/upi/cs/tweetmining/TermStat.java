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


/**
 * Digunakan untuk menghitung nilai rata-rata term, biasanya dikombinasikan dengan hashmap
 * (penggunaan lihat TFIDF dan KMeans)
 *
 * @author Yudi Wibisono (yudi@upi.edu)
 */



 public class TermStat  {
         public String term;
         private  int freq;
         private double totVal; // total value
         private double avg;
        

         public TermStat(String term,double totVal) {
             freq=1;
             avg = 0;
             this.term = term;
             this.totVal = totVal;
         }

         public void incFreq() {
             freq++;
         }

         public  void addVal(double val) {
             totVal += val;
             incFreq();
         }

         public void calcAvg() {
             avg = totVal / freq;
         }

         /**
          * panggil dulu calcAvg!
          * @return
          */
         public double getAvg() {
             return avg;
         }

         public int getFreq() {
             return freq;
         }

         public double getTotVal() {
             return totVal;
         }
     } // class termStat


