package mapreduce_maven;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.Writable;

public final class MaxWritable implements Writable  {

	double value;
	String name;
	public MaxWritable() {}

	public MaxWritable(double value, String name) {
		this.value = value;
		this.name = name;
	}

	

	public void write(DataOutput out) throws IOException {
		out.writeDouble(value);
		out.writeUTF(name);
	}

	public void readFields(DataInput in) throws IOException {
		value = in.readDouble();
		name = in.readUTF();
	}


}
