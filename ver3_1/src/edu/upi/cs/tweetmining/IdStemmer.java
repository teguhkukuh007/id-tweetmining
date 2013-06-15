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
 *
 * Indonesian Stemmer
 *
 * Based on Fadillah Z Tala algorithm
 *
 * @author Yudi Wibisono (yudi@upi.edu)
 * @version July 2006
 *
 */

public class IdStemmer{

	private static boolean isKe,isPeng,isDi,isMeng,isTer,isBer;

	public static void main(String [] Args)
	{
		int i;
		System.out.println("Testing idStemmer");
		String[] arrTes  = {"manakah","usahalah","matipun","sepedamu","sepedanya","sepedaku",
		                    "mengukur","menyapu","menduga","menuduh","memusat","membaca","merusak","pengukur",
		                    "penyapu","penduga","penuduh","pemusat","pembaca","diukur","tersapu","kekasih","tarikkan",
		                    "mengambilkan","makanan","perjanjian","tandai","mendapati","pantai","berlari","belajar",
		                    "bekerja","perjelas","pelajar","pekerja"};

		//IdStemmer ids = new IdStemmer();


		for (i=0;i<=arrTes.length-1;i++)
		{
		   System.out.println(arrTes[i]+"-->"+IdStemmer.stem(arrTes[i]));
		}
	}

	private static boolean isVokal(char cc)
	{
		return  (cc =='a'||cc =='i'||cc=='u'||cc=='e'||cc =='o');
	}


	public static String removeSuffix(String str)
	{
		String Ostr = str;

		if (str.length() > 6)
        {
	        if (str.endsWith("kan") &&  !isKe && !isPeng)
			{
				Ostr =  str.substring(0,str.length()-3);
			}
			else if (str.endsWith("an") &&  !isDi && !isMeng  && !isTer)
			{
				Ostr =  str.substring(0,str.length()-2);
			}
			else if (str.endsWith("i") &&  !isBer && !isKe  && !isPeng)
			{
			    if (!str.endsWith("si"))
			      {Ostr =  str.substring(0,str.length()-1);}
			}
		}
		return Ostr;
	}

	public static String removeSecondOrder(String str)
	{
        String Ostr = str;

        if (str.length() > 6)
        {
	        if (str.startsWith("ber"))
	        {
	        	isBer = true;
	        	Ostr =  str.substring(3,str.length());
	        }
	        else if (str.startsWith("per"))
	        {
	        	Ostr =  str.substring(3,str.length());
	        }
	        else if (str.startsWith("bel") || str.startsWith("pel") )
	        {

	        	if (str.substring(3,7).equals("ajar"))
	        	{
	        		Ostr = str.substring(3,str.length());
	        	}
	        }
	        else if (str.startsWith("be"))
	        {
	        	char cc = str.charAt(2);
		        if (!isVokal(cc))  //konsonan
		        { //beterbangan  bekerja
		        	if (str.substring(3,5).equals("er"))
		        	{
		        		Ostr = str.substring(2,str.length());
		        	}
		        }
	        }
			else if (str.startsWith("pe"))
			{
				Ostr = str.substring(2,str.length());
			}
		}

		return Ostr;


	}


	public static String stem(String str)
	{
	      isKe = false;
	      isPeng = false;
	      isDi = false;
	      isMeng = false;
	      isTer = false;
	      isBer = false;

	      String oStr = str;
	      oStr.toLowerCase();
	      if (oStr.length() > 6)
	      {
	          //Remove Particle   kah,lah,pun
	          //Remove Possesive Pronoun  mu,ku,nya

	          if  ( oStr.endsWith("nya")||oStr.endsWith("pun")||oStr.endsWith("lah")||oStr.endsWith("kah") )
	          {
	          	 //System.out.println("debug"+oStr);
	          	 oStr = oStr.substring(0, str.length() - 3);
	          }
	          else if (oStr.endsWith("mu")||oStr.endsWith("ku"))
	          {
	          	 oStr = oStr.substring(0, str.length() - 2);
	          }
	      }

	      if (oStr.length() > 6 )
	      {

	          //remove first order of derivational prefixes

	          boolean isRemoveFirstOrder = false;

	          if (oStr.startsWith("meng"))
	          {
	          	 isMeng = true;
	          	 oStr = oStr.substring(4,str.length());
	          	 isRemoveFirstOrder = true;
	          }
	          else if (oStr.startsWith("peng"))
	          {
	          	 isPeng = true;
	          	 oStr = oStr.substring(4,str.length());
	          	 isRemoveFirstOrder = true;
	          }
	          else if ( oStr.startsWith("meny") || oStr.startsWith("peny") )
	          {
	          	 char cc = oStr.charAt(4);
	          	 if (isVokal(cc))
	          	 {
	          	 	oStr = 's' + oStr.substring(4,str.length());
	          	 	isRemoveFirstOrder = true;
	          	 }
	          }
	          else if (oStr.startsWith("men") || oStr.startsWith("pen") )
	          {

	          	char cc = oStr.charAt(3);  //yw:additional rule, menuduh --> tuduh
	          	if (isVokal(cc))
	          	{
	          	   	oStr = 't' + oStr.substring(3,str.length());
	          	}
	          	else
	          	{
	          		oStr = oStr.substring(3,str.length());
	          	}

	          	isRemoveFirstOrder = true;
	          }
	          else if (oStr.startsWith("mem") || oStr.startsWith("pem"))
	          {
	          	 char cc = oStr.charAt(3);
	          	 isRemoveFirstOrder = true;
	          	 if (isVokal(cc))
	          	 {
	          	 	oStr = 'p' + oStr.substring(3,str.length());  // memilah --> pilah
	          	 }
	          	 else
	          	 {  oStr = oStr.substring(3,str.length()); } //membaca --> baca
	          }
	          else if (oStr.startsWith("me"))
	          {
	          	 isRemoveFirstOrder = true;
	          	 oStr = oStr.substring(2,str.length());
	          }
	          else if (oStr.startsWith("di"))
	          {
	          	 isDi = true;
	          	 isRemoveFirstOrder = true;
	          	 oStr = oStr.substring(2,str.length());
	          }
	          else if (oStr.startsWith("ter"))
	          {
	          	 isTer = true;
	          	 isRemoveFirstOrder = true;
	          	 oStr = oStr.substring(3,str.length());
	          }
	          else if (oStr.startsWith("ke"))
	          {
	          	 isKe = true;
	          	 isRemoveFirstOrder = true;
	          	 oStr = oStr.substring(2,str.length());
	          }


	          if (isRemoveFirstOrder)
	          {
	          	  oStr = removeSuffix(oStr);
	          	  oStr = removeSecondOrder(oStr);
	          }
	          else
	          {
	          	  oStr = removeSecondOrder(oStr);
	          	  oStr = removeSuffix(oStr);
	          }


	      }
		  return oStr;
	}
}


