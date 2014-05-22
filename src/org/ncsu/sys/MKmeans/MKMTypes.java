package org.ncsu.sys.MKmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class MKMTypes {
	
	public static int UNDEF_VAL = -1;
	
	public static enum VectorType{
		REGULAR(0), CENTROID(1), PARTIALCENTROID(2);
		
		private int typeVal;
		private VectorType(int typeVal){
			this.typeVal = typeVal;
		}
		
		public int getTypeVal(){
			return this.typeVal;
		}

		public static VectorType getType(int readInt) {
			switch(readInt){
			case 0:
				return REGULAR;
			case 1:
				return CENTROID;
			case 2:
				return PARTIALCENTROID;
			default:
				return REGULAR;
			}
		}
	}
	
	public static class Values implements Writable{
		private int valCount;
		private Value[] values;
		
		public Values(){
			valCount = 0;
		}
		
		public Values(int count){
			this.valCount = count;
			values = new Value[count];
		}
		
		public int getValCount() {
			return valCount;
		}
		
		public void setValCount(int valCount) {
			this.valCount = valCount;
		}
		
		public Value[] getValues() {
			return values;
		}
		public void setValues(Value[] values) {
			this.values = values;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			valCount = in.readInt();
			values = new Value[valCount];
			for(int i = 0; i < valCount; i++){
				Value val = new Value();
				val.readFields(in);
				values[i] = val;
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(valCount);
			for(int i =0; i < valCount; i++){
				values[i].write(out);
			}
		}
		
	}
}
