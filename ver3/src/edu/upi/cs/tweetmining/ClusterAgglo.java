package edu.upi.cs.tweetmining;

import java.util.ArrayList;

public class ClusterAgglo extends ClusterKMeans {
	
	public ClusterAgglo parent ;            
	public ArrayList<ClusterAgglo> child = new ArrayList<ClusterAgglo>();
	
	public void addChild(ClusterAgglo c) {
		child.add(c);
	}
	
	
	public ClusterAgglo(int idCluster) {
		super(idCluster);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
