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
		
		double wc = 0.5;  //bobot
		double wl = 0.5;
		return (  (wc*cohesion()) + (wl*(1-(level/maxLevel))))/2;
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
