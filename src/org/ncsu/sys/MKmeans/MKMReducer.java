package org.ncsu.sys.MKmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Reducer;
import org.ncsu.sys.MKmeans.MKMTypes.VectorType;

public class MKMReducer extends Reducer<IntWritable, PartialCentroid, Key, Value> {

	private int mapTaskCount;
	private List<SequenceFile.Writer> writers;
	
	
	public void setup (Context context) {
		Configuration conf = context.getConfiguration();
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			init(context);
			//initialize the list of sequence file writers
			Path path = new Path(conf.get("KM.inputCenterPath"));
			writers = new ArrayList<SequenceFile.Writer>();
			writers.add(SequenceFile.createWriter(fs, conf, path,
				      Key.class, Value.class,
				      SequenceFile.CompressionType.NONE));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void init(Context context) {
		Configuration conf = context.getConfiguration();
		mapTaskCount = conf.getInt("KM.mapTaskCount", 4);
//		dimension = conf.getInt("KM.dimension", 2);
//		k = conf.getInt("KM.k", 6);
//		R1 = conf.getInt("KM.R1", 6);
//		centroids = new ArrayList<Value>();
//		vectors = new ArrayList<Value>();
//		isCbuilt = isVbuilt = false;
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
		//close writers
		if(writers != null){
			for(SequenceFile.Writer writer : writers){
				writer.close();
			}
		}
	}

	public void reduce(IntWritable _key, Iterable<PartialCentroid> values, Context context)
			throws IOException, InterruptedException {
		// process values
		Value newCentroid;
		PartialCentroid newpCentroid = null;
		for (PartialCentroid val : values) {
			if(newpCentroid == null){
				newpCentroid = new PartialCentroid(val.getDimension());
				newpCentroid.copy(val);
			}
			else
			{
				newpCentroid.addVector(val);
			}
//				context.write(new Key(1, VectorType.CENTROID), newCentroid);
		}
		try {
			newCentroid = computeNewCentroid(newpCentroid);
			//TODO: iterate through all the file and write each of the newCentroids computed
			for(SequenceFile.Writer writer : writers){
				writer.append(new Key(1, VectorType.CENTROID), newCentroid);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private Value computeNewCentroid(PartialCentroid pCent) throws Exception {
		if(pCent == null){
			throw new Exception("partialcentroid can not be null");
		}
		if(pCent.getCount() == 0)
			return null;
		else {
			Value newCentroid = VectorFactory.getInstance(VectorType.CENTROID, pCent.getDimension());
			int[] coords = pCent.getCoordinates();
			int[] newCoords = newCentroid.getCoordinates();
			for(int i = 0; i < coords.length; i++){
				newCoords[i] = coords[i]/pCent.getCount();
			}
			newCentroid.setCentroidIdx(pCent.getCentroidIdx());
			return newCentroid;
		}
			
	}

}
