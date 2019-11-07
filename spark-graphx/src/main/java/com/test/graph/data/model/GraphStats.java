package com.test.graph.data.model;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

public class GraphStats {
	
	private static final String fileName = "graph_stats.csv";

	private long vertexCount;
	private long edgeCount;
	private int maxDegree;
	private int minDegree;
	private double avgDegree;
	private double density;
	private long triangleCount;

	public GraphStats() {
	}
	
	public GraphStats(int vertexCount, int edgeCount, int maxDegree, int minDegree, int avgDegree, 
			double density) {
		this.vertexCount = vertexCount;
		this.edgeCount = edgeCount;
		this.maxDegree = maxDegree;
		this.minDegree = minDegree;
		this.avgDegree = avgDegree;
		this.density = density;
	}

	public long getVertexCount() {
		return vertexCount;
	}

	public void setVertexCount(long vertexCount) {
		this.vertexCount = vertexCount;
	}

	public long getEdgeCount() {
		return edgeCount;
	}

	public void setEdgeCount(long edgeCount) {
		this.edgeCount = edgeCount;
	}

	public int getMaxDegree() {
		return maxDegree;
	}

	public void setMaxDegree(int maxDegree) {
		this.maxDegree = maxDegree;
	}

	public int getMinDegree() {
		return minDegree;
	}

	public void setMinDegree(int minDegree) {
		this.minDegree = minDegree;
	}

	public double getAvgDegree() {
		return avgDegree;
	}

	public void setAvgDegree(double avgDegree) {
		this.avgDegree = avgDegree;
	}

	@Override
	public String toString() {
		return "GraphStats \n"
				+ "-----------------------------------\n"
				+ " [vertexCount=" + vertexCount + ", edgeCount=" + edgeCount  
				+ ", density=" + density +
				", maxDegree=" + maxDegree
				+ ", minDegree=" + minDegree + ", avgDegree=" + avgDegree 
				+ ", triangleCount=" + triangleCount
				+ "]"
				+ "\n-----------------------------------";
	}

	public double getDensity() {
		return density;
	}

	public void setDensity(double density) {
		this.density = density;
	}

	public long getTriangleCount() {
		return triangleCount;
	}

	public void setTriangleCount(long triangleCount) {
		this.triangleCount = triangleCount;
	}
	
	public void saveStats(String outputdir) {
		File outDir = new File(outputdir);
		if(!outDir.exists()) {
			outDir.mkdir();
		}
		try {
			PrintStream ps = new PrintStream(new FileOutputStream(outputdir + "/" + fileName));
			ps.println("Stat, Value");
			ps.println("Vertices, "+ vertexCount);
			ps.println("Edges, "+ edgeCount);
			ps.println("MaxDegree, " + maxDegree);
			ps.println("MinDegree, " + minDegree);
			ps.println("MeanDegree, " + avgDegree);
			ps.println("TriangleCount, " + triangleCount);
			ps.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
