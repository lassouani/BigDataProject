import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Point2DWritable implements Writable {

	public Double x = new Double(0);
	public Double y = new Double(0);
	

	public Point2DWritable() {
	}
	
	public Point2DWritable(Double x, Double y){		
		this.x = x;
		this.y = y;
	}

	public void readFields(DataInput arg0) throws IOException {
		this.x = arg0.readDouble();
		this.y = arg0.readDouble();
	}

	public void write(DataOutput arg0) throws IOException {
		arg0.writeDouble(this.x);
		arg0.writeDouble(this.y);
	}

}
