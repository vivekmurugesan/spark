package com.test.graph.operations;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import com.test.graph.data.model.GraphStats;

import scala.Tuple2;

public class GraphOperations implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String inputFile = "/Users/apple/DataSets/socfb-A-anon/socfb-A-anon.mtx";
	private String outputDir;
	
	private static final int partition_count = 256;
	

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Graph Processing of operations");
		
		JavaSparkContext sc = new JavaSparkContext(conf);

		GraphOperations cc = new GraphOperations(args[0], args[1]);
		
		cc.computeConnectedComponents(sc);
		
	}

	public GraphOperations(String inputFile, String outputDir) {
		this.inputFile = inputFile;
		this.outputDir = outputDir;
	}
	
	public void computeConnectedComponents(JavaSparkContext sc) {
		JavaPairRDD<Integer, Integer> edgesRdd = 
				readFile(sc);
		
		
		  GraphStats stats = computeStats(edgesRdd, sc);
		  
		  System.out.println(".. Graph stats..."); System.out.println(stats);
		 
		
		/* System.out.println(".. Computing connected components.."); */

		/* computeConnectedComponents(edgesRdd, sc); */
		
		/*
		 * JavaPairRDD<Integer, Integer> degCountRdd = computeDegCount(edgesRdd);
		 * degCountRdd.map(x -> x._1 + "," + x._2).saveAsTextFile(outputDir+"/degress");
		 */
	}
	
	public GraphStats computeStats(JavaPairRDD<Integer, Integer> edgesRdd,
			JavaSparkContext sc) {
		GraphStats stats = new GraphStats();
		stats.setEdgeCount(edgesRdd.count());
		stats.setVertexCount(computeVertexCount(edgesRdd));
		
		JavaPairRDD<Integer, Integer> degCountRdd = computeDegCount(edgesRdd);
		
		stats.setMinDegree(degCountRdd.values().min(new SerializableComparator()));
		stats.setMaxDegree(degCountRdd.values().max(new SerializableComparator()));
		
		stats.setAvgDegree(computeMeanDeg(degCountRdd));
		
		// For undirected graphs..
		double vertexCount = stats.getVertexCount() * 1.0;
		double edgeCount = stats.getEdgeCount();
		double density = edgeCount / (vertexCount*(vertexCount-1)/2.0);
		
		stats.setDensity(density);
		
		System.out.println(".. Graph stats...(without triangle count)..");
		System.out.println(stats);
		
		stats.saveStats(outputDir);
		
		/*
		 * long triangleCount = computeTriangleCount(edgesRdd, sc);
		 * 
		 * stats.setTriangleCount(triangleCount);
		 */
		
		return stats;
	}
	
	private double computeMeanDeg(JavaPairRDD<Integer, Integer> degCountRdd) {
		long sum = degCountRdd.values().fold(0, (a,b) -> a+b);
		long count = degCountRdd.count();
		
		double mean = (sum*1.0)/count;
		
		return mean;
	}
	
	/**
	 * Count vertices that appear on both the sides of the edges, 
	 * after taking the distinct set from them.
	 * @param edgesRdd
	 * @return
	 */
	public long computeVertexCount(JavaPairRDD<Integer, Integer> edgesRdd) {
		
		long vertexCount = edgesRdd.keys().union(edgesRdd.values()).distinct().count();
		
		return vertexCount;
	}
	
	/**
	 * 1. Compute the degree for each vertex based on which side of the 
	 * edge they appear.
	 * 2. Given a vertex present on both the sides of the edges across
	 * different edges, the degrees computed has to be summed up to 
	 * get the final count.
	 * For directed graphs, this code has to be modified a bit to compute
	 * in/out degrees separately.
	 * @param edgesRdd
	 * @return
	 */
	public JavaPairRDD<Integer, Integer> computeDegCount(JavaPairRDD<Integer, Integer> edgesRdd){
		JavaPairRDD<Integer, Integer> v1DegCount = edgesRdd.mapToPair(x -> new Tuple2<>(x._1,1))
													.reduceByKey((a,b) -> a+b);
		JavaPairRDD<Integer, Integer> v2DegCount = edgesRdd.mapToPair(x -> new Tuple2<>(x._2, 1))
													.reduceByKey((a,b) -> a+b);
		
		JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<Integer>>> joined =
				v1DegCount.fullOuterJoin(v2DegCount);
		
		JavaPairRDD<Integer, Integer> degCountRdd = joined.mapToPair(x -> {
			int vertexId = x._1;
			Optional<Integer> a = x._2._1;
			Optional<Integer> b = x._2._2;
			
			int degCount = 1;
			if(a.isPresent() && b.isPresent())
				degCount = a.get() + b.get();
			else if(a.isPresent())
				degCount = a.get();
			else if(b.isPresent())
				degCount = b.get();
			
			return new Tuple2<>(vertexId, degCount);
		});
		
		System.out.println(".. Vertex count.. from degrees rdd::" + degCountRdd.count());
		
		return degCountRdd;
	}
	
	/**
	 * Reads the file format .mat and creates rdd containing different edges.
	 * This format is eventually a csv file containing an entry for each edge,
	 * with some additional meta-data.
	 * @param sc
	 * @return
	 */
	public JavaPairRDD<Integer, Integer> readFile(JavaSparkContext sc){
		JavaRDD<String> textRdd = sc.textFile(inputFile, partition_count);
		
		JavaRDD<String> filtered =
		textRdd.filter(x -> {
			boolean result = true;
			if(!x.startsWith("%")) {
				if(x.split(" ").length > 2)
					result = false;
			}else
				result = false;
			return result;	
		});
		
		JavaPairRDD<Integer, Integer> edgesRdd = filtered.mapToPair(x ->{
			String[] tokens = x.split(" ");
			int v1 = Integer.parseInt(tokens[0]);
			int v2 = Integer.parseInt(tokens[1]);
			
			return new Tuple2<>(v1, v2);
		});
		
		return edgesRdd.cache();
	}
	
	/**
	 * Triangle is a combination of 3 vertices, where all of them are connected 
	 * with each other. 
	 * Idea here is to process each edge at a time in a distributed manner. 
	 * Look at the directly connected vertices of both the vertices of the edge 
	 * and create an intersection among both the sets. These are eventually the
	 * set of vertices that are connected to both the vertices, hence forming
	 * a triangle.
	 * 1. Creates an adjacency list representation of the graph based on the 
	 * edges rdd <vertex_id, Set<directly_connected>>
	 * 2. Join with both <key, val> of each edge rdd and create a representation
	 * in the form,
	 * 		<<vertex1_id+"__"+vertex2_id>,  <<v1_dir_connected>, <v2_dir_connected>>
	 * 3. Remove both v1 and v2 from the intersection set.
	 * 4. Size of the resulting set from step 3 is the number of triangles that
	 * can be formed from the given edge.
	 * 
	 * This mechanism may not be the efficient. But the computations on this 
	 * algorithm can be distributed. That is, each edge can be processed without
	 * having access to data about any other edge in the graph.
	 * @param edgesRdd
	 * @param sc
	 * @return
	 */
	public long computeTriangleCount(JavaPairRDD<Integer, Integer> edgesRdd, 
			JavaSparkContext sc) {
		
		JavaPairRDD<Integer, Set<Integer>> adjList = computeAdjacencyList(edgesRdd);
		
		System.out.println("... Number of entries in consolidated "
				+ "directly connected map::" + adjList.count());
		
		/*
		 * Map<Integer, Set<Integer>> dirConnectedMap = consolidated.collectAsMap();
		 * final Map<Integer, Set<Integer>> dirConnectedMapB =
		 * sc.broadcast(dirConnectedMap).getValue();
		 */
		
		JavaPairRDD<Integer, Tuple2<Integer, Set<Integer>>> joinedWithV1 =
 			edgesRdd.join(adjList);
		JavaPairRDD<Integer, Tuple2<Tuple2<Integer, Set<Integer>>, Set<Integer>>> joinedWithV2 =
				joinedWithV1.mapToPair(x -> new Tuple2<>(x._2._1, new Tuple2<>(x._1, x._2._2)))
				.join(adjList);
		JavaPairRDD<String, Tuple2<Set<Integer>, Set<Integer>>> joinedMerged =
				joinedWithV2.mapToPair(x -> {
					String edgeKey = x._2._1._1 +"__" + x._1;
					
					Set<Integer> v1Set = x._2._1._2;
					Set<Integer> v2Set = x._2._2;
					
					return new Tuple2<>(edgeKey, new Tuple2<>(v1Set, v2Set));
				});
		
		JavaPairRDD<String, Integer> triangleCountRdd = 
				joinedMerged.mapToPair(x -> {
			String[] tokens = x._1.split("__");
			int v1 = Integer.parseInt(tokens[0]);
			int v2 = Integer.parseInt(tokens[1]);
			
			Set<Integer> smaller = new HashSet<>();
			if(x._2._1.size() < x._2._2.size()) {
				smaller.addAll(x._2._1);
				smaller.retainAll(x._2._2);
			}else {
				smaller.addAll(x._2._2);
				smaller.retainAll(x._2._1);
			}
			
					/*
					 * Set<Integer> v1Connected = new HashSet<>(); v1Connected.addAll(x._2._1);
					 * v1Connected.retainAll(x._2._2);
					 */
			smaller.remove(v1);
			smaller.remove(v2);
			
			// nodes that are connected to both the nodes will form triangles
			int numTriangles = smaller.size();
			
			return new Tuple2<>(x._1, numTriangles);
			
		});
		
		long totalCount = triangleCountRdd.values().fold(0, (a,b) -> a+b);
		
		return totalCount;
	}
	
	/**
	 * 1. Consider each edge as <v1, v2> representing 2 sides of the edges.
	 * 2. Compute the directly connected set of vertices for each v1 by grouping
	 * based on key.
	 * 3. Similarly compute the directly connected set of vertices for each v2 
	 * by reversing the rdd and then grouping based on the key.
	 * 4. Perform a full outer join on the resulting rrds from step 2 and 3.
	 * 5. For each vertex perform an union among the 2 sets of directly connected
	 * vertices obtained from v1 and v2. As any given vertex can appear as 
	 * either v1 or v2 on different edges.
	 * @param edgesRdd
	 * @return
	 */
	public JavaPairRDD<Integer, Set<Integer>> computeAdjacencyList(JavaPairRDD<Integer, Integer> edgesRdd) {
		JavaPairRDD<Integer, Iterable<Integer>> directlyConnected = 
				edgesRdd.groupByKey();
		JavaPairRDD<Integer, Iterable<Integer>> directlyConnected2 =
			edgesRdd.mapToPair(x -> new Tuple2<>(x._2, x._1)).groupByKey();
		
		JavaPairRDD<Integer, Tuple2<Optional<Iterable<Integer>>, Optional<Iterable<Integer>>>> joined =
			directlyConnected.fullOuterJoin(directlyConnected2);
		
		JavaPairRDD<Integer, Set<Integer>> consolidated = 
				joined.mapToPair(x -> {
			int vertexId = x._1;
			
			Set<Integer> connected = new HashSet<>();
			if(x._2._1.isPresent())
				x._2._1.get().forEach(a -> connected.add(a));
			
			if(x._2._2.isPresent())
				x._2._2.get().forEach(a -> connected.add(a));
			
			return new Tuple2<>(vertexId, connected);
		}).cache();
		
		return consolidated;
	}
	
	/**
	 * The idea is to,
	 * 1. Start with a vertex and discover all the connected vertices by traversing through
	 * the edges.
	 * 2. Remove the entire subgraph discovered through the traversal.
	 * 3. Repeat the process by identifying another start vertex in the remaining graph.
	 * 4. Repeat the process until the graph becomes empty.
	 * The strategy followed is to start with a vertex that has maximum degree. 
	 * It is a typical greedy approach by starting with a best possible vertex 
	 * based on the known information.
	 * @param edgesRdd
	 * @param sc
	 */
	public void computeConnectedComponents(JavaPairRDD<Integer, Integer> edgesRdd, JavaSparkContext sc) {
		
		JavaPairRDD<Integer, Set<Integer>> adjList = computeAdjacencyList(edgesRdd);
		
		final Set<Integer> discovered = new HashSet<>();
		
		JavaPairRDD<Integer, Integer> degreesRdd = adjList.mapToPair(x -> new Tuple2<>(x._1, x._2.size()));
		
		Map<Integer, ConnectedComponent> components = new HashMap<>();
		
		long adjListSize = adjList.count();
		
		while(adjListSize > 0) {

			System.out.println(".. Adj list size::" + adjListSize);
			
			int startVertexId = findStartVertex(degreesRdd);

			System.out.println(".. Starting the iteration with .. start vertex::" + startVertexId);
			
			//ConnectedComponent component = iterateFromStartVertex(adjList, componentsRdd, startVertexId, discovered);
			ConnectedComponent component = iterateFromStartVertexDist(edgesRdd, adjList, sc, startVertexId, discovered);
			components.put(component.getComponentId(), component);
			
			System.out.println(".. Component constructed.." + component);

			final Set<Integer> discoveredB = sc.broadcast(discovered).getValue();

			// 1. Remove all the discovery from the component.
			// 2. Continue the discovery with next start vertex..

			adjList = adjList.filter(x -> !discoveredB.contains(x._1));
			degreesRdd = degreesRdd.filter(x -> !discoveredB.contains(x._1));
			
			adjListSize = adjList.count();
		}
		
		System.out.println(".. Total number connected components discovered: " + components.size());
		
		for(int id : components.keySet()) {
			ConnectedComponent comp = components.get(id);
			System.out.println(comp);
		}
	}
	
	/**
	 * This is a version of the implementation where there is no parallel processing of 
	 * discovering and processing all the nodes of the current layer at once.
	 * Refer to: iterateFromStartVertexDist for the distributed implementation.
	 * @param adjList
	 * @param componentsRdd
	 * @param startVertexId
	 * @param discovered
	 * @return
	 */
	public ConnectedComponent iterateFromStartVertex(JavaPairRDD<Integer, Set<Integer>> adjList, 
			JavaPairRDD<Integer, Tuple2<Set<Integer>, Optional<Integer>>> componentsRdd,
			int startVertexId, Set<Integer> discovered) {
		
		if(discovered.contains(startVertexId)) // Just to ensure..
			return null;
		
		Set<Integer> members = new HashSet<>();
		
		int memberCount = 0;
		LinkedList<Integer> que = new LinkedList<>();
		que.add(startVertexId);
		
		int iterCount = 0;
		
		while(!que.isEmpty()) {

			System.out.printf("Running iter count: %d for start vertex: %d\n", iterCount++, startVertexId);
			
			int u = que.removeFirst();
			discovered.add(u);
			members.add(u);
			memberCount++;
			
			Set<Integer> neighbours = componentsRdd.filter(x -> (x._1 == u)).values().collect().get(0)._1;
			//int newDiscovery = 0;
			for(int v : neighbours) {
				if(!discovered.contains(v)) {
					que.add(v);
					//newDiscovery++;
				}
			}
			/*
			 * if(newDiscovery == 0) continue;
			 */
		}
		
		ConnectedComponent component = new ConnectedComponent(startVertexId, members, memberCount);
		
		return component;
		
	}
	
	/**
	 * It is an approach of discovering layer by layer. 
	 * Slightly modified version of a BFS, to fit into a distributed computation.
	 * 1. At every layer find out all the directly connected vertices. By,
	 * 		a. Filtering based on the new nodes discovered from the current layer
	 * 		on the edges rdd.
	 * 		b. Identifying all the distinct set of directly connected vertices 
	 * 2. Identify the new nodes discovered by removing the set found earlier.
	 * 3. Make this set as to explore and traverse further.
	 * 4. Add this set to the set of members discovered already.
	 * 5. Stop iterating, the moment no more new nodes getting discovered.
	 * @param edgesRdd
	 * @param adjList
	 * @param sc
	 * @param startVertexId
	 * @param discovered
	 * @return
	 */
	public ConnectedComponent iterateFromStartVertexDist(JavaPairRDD<Integer, Integer> edgesRdd,
			JavaPairRDD<Integer, Set<Integer>> adjList, 
			JavaSparkContext sc,
			int startVertexId, Set<Integer> discovered) {
		
		if(discovered.contains(startVertexId)) // Just to ensure..
			return null;
		
		Set<Integer> members = new HashSet<>();
		
		int memberCount = 0;
		LinkedList<Integer> que = new LinkedList<>();
		que.add(startVertexId);
		
		members.add(startVertexId);
	
		final Set<Integer> neighbors = adjList.filter(x -> (x._1 == startVertexId)).values().collect().get(0);
		System.out.println("Initial neighbors count::" + neighbors.size());
		int iterCount = 0;
		
		do {
			
			int prevCount = members.size();
			members.addAll(neighbors);
			if(members.size()<=prevCount) {
				System.out.println(".. 2. No new nodes discovered.. breaking..");
				break;
			}
			
			System.out.printf("Running iter count: %d for start vertex: %d with: %d nodes discovered so far\n", iterCount++, startVertexId, members.size());
			
			final Set<Integer> neighborsB = sc.broadcast(neighbors).getValue();
			
			System.out.println(".. neighborsB size:" + neighborsB.size());

			List<Integer> discovery = 
					edgesRdd.filter(x -> neighborsB.contains(x._1)).values()./*.subtract(neighborRdd). */distinct().collect();
			System.out.println(".. Count of the nodes discovered.." + discovered.size());
			//newDiscovery.removeAll(members); // Removing all that have already been discovered.
			/*
			 * if(newDiscovery.isEmpty()) {
			 * System.out.println(".. No new nodes discovered.. breaking.."); break; }
			 */
			//System.out.println(".. Count of new nodes discovered.. after removing dups.." + newDiscovery.size());
			// This set becomes the new neigbours
			
			List<Integer> newDiscovery = new ArrayList<>();
			for(int nodeId : discovery) {
				if(!members.contains(nodeId))
					newDiscovery.add(nodeId);
			}
			
			if(newDiscovery.size() <= 0) {
				System.out.println(".. 1. No new nodes discovered.. breaking..");
				break;
			}
			
			neighbors.clear();
			neighbors.addAll(newDiscovery);
		}while(!neighbors.isEmpty());
		
		System.out.println(".. Completed the discovery with start vertex::" + startVertexId);
				
		ConnectedComponent component = new ConnectedComponent(startVertexId, members, memberCount);
		
		return component;
		
	}
	
	public boolean allNodesDiscovered(JavaPairRDD<Integer, Tuple2<Set<Integer>, Optional<Integer>>> componentsRdd) {
		long undiscoveredCount = componentsRdd.filter(x -> (!x._2._2.isPresent())).count();
		return undiscoveredCount <= 0;
	}
	
	/**
	 * This method looks at the degree count of each vertex and returns the one with
	 * highest degree as the start vertex for discovering connected component.
	 * @param degreesRdd
	 * @return
	 */
	public int findStartVertex(JavaPairRDD<Integer, Integer> degreesRdd) {
		final int maxDegree = degreesRdd.values().max(new SerializableComparator());
		
		System.out.println("Max degree::" + maxDegree);
		
		Map<Integer, Integer> maxDegreesVertex = degreesRdd.filter(x -> (x._2 == maxDegree)).collectAsMap();
		
		System.out.println("Max degrees vertex::" + maxDegreesVertex);
		
		return maxDegreesVertex.keySet().iterator().next();
	}
	
	
	class SerializableComparator implements Comparator<Integer>, Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public int compare(Integer o1, Integer o2) {
			return o1.compareTo(o2);
		}
	}
}
