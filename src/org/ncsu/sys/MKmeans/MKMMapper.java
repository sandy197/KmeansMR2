package org.ncsu.sys.MKmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.ncsu.sys.MKmeans.MKMTypes.Values;
import org.ncsu.sys.MKmeans.MKMTypes.VectorType;

public class MKMMapper extends Mapper<Key, Values, IntWritable, PartialCentroid> {
	
	private static final boolean DEBUG = true;
	private int dimension;
	private int k;
	private int R1;
	private boolean isCbuilt, isVbuilt;
	private List<Value> centroids, vectors;
	
	public void setup (Context context) {
		init(context);
		Configuration conf = context.getConfiguration();
		//read centroids
		//Change this section for Phadoop version
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			Path path = new Path(conf.get("KM.inputCenterPath"));
			Path filePath = fs.makeQualified(path);
			centroids = MKMUtils.getCentroidsFromFile(filePath, false);
			if(centroids == null){
				throw new IOException("No centroids fetched from the file");
			}
			isCbuilt = true;
			if(DEBUG) System.out.println("************Centroids Read form file*************");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void init(Context context) {
		Configuration conf = context.getConfiguration();
		dimension = conf.getInt("KM.dimension", 2);
		k = conf.getInt("KM.k", 6);
		R1 = conf.getInt("KM.R1", 6);
		centroids = new ArrayList<Value>();
		vectors = new ArrayList<Value>();
		isCbuilt = isVbuilt = false;
	}

	public void map(Key key, Values values, Context context)
			throws IOException, InterruptedException {
		PartialCentroid[] partialCentroids = null;
		if(key.getType() == org.ncsu.sys.MKmeans.MKMTypes.VectorType.CENTROID && !isCbuilt){
			buildCentroids(values, centroids);
			isCbuilt = true;
		}
		else{
			//use build and set here
			buildCentroids(values, vectors);
			isVbuilt = true;
			//buildVectors(values, vectors);
		}
		if(isCbuilt && isVbuilt){
			try{
				if(DEBUG) System.out.println("Classifying " + vectors.size() + " vectors among " + centroids.size() + " clusters" );
				System.out.println("$$VectorCount:"+"\t"+vectors.size());
				long start =System.nanoTime();
				partialCentroids = (PartialCentroid[]) classify(vectors, centroids);
				for(PartialCentroid pcent : partialCentroids){
					IntWritable newKey = new IntWritable(pcent.getCentroidIdx());
					context.write(newKey, pcent);
					if(DEBUG) printMapOutput(newKey, pcent);
				}
				long end =System.nanoTime();
				System.out.println("$$ClassifyTime:"+"\t" + (end-start));
			}
			catch(Exception ex){
				ex.printStackTrace();
			}
		}
		
	}
	
	private void printMapOutput(IntWritable newKey, PartialCentroid pcent) {
		StringBuilder sb = new StringBuilder();
		sb.append("##### Map output: (" + newKey.get() + ") (" 
					+ pcent.getDimension() + "," + pcent.getCentroidIdx() + "," + pcent.getCount() + "\n");
		for(int coord : pcent.getCoordinates()){
			sb.append(coord + ",");
		}
		sb.append(") ");
		System.out.println(sb.toString());
	}

	private void buildCentroids(Values values, List<Value> centroidsLoc) {
		for(Value val : values.getValues()){
			if(DEBUG) System.out.println("Adding value :" + val);
			Value valCopy = VectorFactory.getInstance(VectorType.REGULAR);
			valCopy.copy(val);
			centroidsLoc.add(valCopy);
		}
		
	}
	
	private PartialCentroid[] classify(List<Value> vectors2, List<Value> centroids2) throws Exception {
		PartialCentroid[] partialCentroids = new PartialCentroid[centroids2.size()];
		
		Hashtable<Integer, Value> pCentMapping = new Hashtable<Integer, Value>(); 
		for(Value pcent : centroids2){
			pCentMapping.put(pcent.getCentroidIdx(), pcent);
		}
			
		for(Value point : vectors2){
			int idx = getNearestCentroidIndex(point, centroids2);
			if(partialCentroids[idx] == null){
				partialCentroids[idx] = (PartialCentroid)VectorFactory.getInstance(VectorType.PARTIALCENTROID, point.getDimension());
				pCentMapping.remove(idx);
			}
			partialCentroids[idx].addVector(point);
			if(partialCentroids[idx].getCentroidIdx() == MKMTypes.UNDEF_VAL){
				partialCentroids[idx].setCentroidIdx(idx);
			}
			else {
				if(partialCentroids[idx].getCentroidIdx() != idx)
					if(DEBUG) throw new Exception("Fatal: Inconsistent cluster, multiple centroids problem!");
			}
		}
		if(pCentMapping.keySet()!= null && !pCentMapping.keySet().isEmpty()){
			for(Integer key : pCentMapping.keySet()){
				partialCentroids[key] = (PartialCentroid)VectorFactory.getInstance(VectorType.PARTIALCENTROID);
				partialCentroids[key].copy(pCentMapping.get(key));
			}
		}
		return partialCentroids;
	}
	
	private int getNearestCentroidIndex(Value point, List<Value> centroids2) {
		int nearestCidx = -1;
		int shortestDistance = Integer.MAX_VALUE;
		for(Value centroid : centroids2){
			int distance = MKMUtils.getDistance(point, centroid);
			if(distance < shortestDistance){
				nearestCidx  = centroid.getCentroidIdx();
				shortestDistance = distance;
			}
		}
		return nearestCidx;
	}
}
