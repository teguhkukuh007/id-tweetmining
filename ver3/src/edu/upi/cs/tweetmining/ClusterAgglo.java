package edu.upi.cs.tweetmining;

import java.util.ArrayList;

public class ClusterAgglo extends ClusterKMeans {
	
	public ClusterAgglo parent ;            
	public ArrayList<ClusterAgglo> child = new ArrayList<ClusterAgglo>();
	public long level; 
	
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
