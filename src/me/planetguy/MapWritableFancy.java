package me.planetguy;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public final class MapWritableFancy extends MapWritable {
	public String toString(){
		StringBuilder result=new StringBuilder();
		for(Writable text: keySet()){
			result.append(text);
			result.append('=');
			DoubleWritable dw=(DoubleWritable) get(text);
			result.append(dw.get());
			result.append('\t');
		}
		return result.toString();
	}
}