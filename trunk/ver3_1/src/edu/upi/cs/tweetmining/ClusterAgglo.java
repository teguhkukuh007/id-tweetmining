package edu.upi.cs.tweetmining;

import java.util.ArrayList;

public class ClusterAgglo extends ClusterKMeans {
	
	public ClusterAgglo parent ;            
	public ArrayList<ClusterAgglo> child = new ArrayList<ClusterAgglo>();
	public long level; 
	public long maxLevel;
	
	@Override
	public double innerQualityScore() {
	    //semakin mendekati 1 semakin bagus
		
		//kohesi digabung dengan level
		//semakin dekat ke 1, kohesi semakin bagus
		//level semakin dekat ke 1, semakin bagus 
		//IS: maxlevel terisi
		
		double wc = 0.6;  //bobot kohesi
		double wl = 0.4;  //bobot level
		double wcc = wc*getCohesion(); 
		double wll = wl*(1-( (double) level/maxLevel));  // <-- hati2 int/int = int, harud dicasting
		double iq =  ( wcc + wll) /2;
//		System.out.println("maxlevel==="+maxLevel);
//		System.out.println("k="+cohesion());
//		System.out.println("l="+level);
//		System.out.println("wcc="+wcc);
//		System.out.println("wll="+wll);
//		System.out.println("iq="+iq);
		return iq;
	}
	

	public void addChild(ClusterAgglo c) {
		child.add(c);
	}
	
	
	public ClusterAgglo(long l) {
		super(l);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) { 
		// TODO Auto-generated method stub

	}

}
