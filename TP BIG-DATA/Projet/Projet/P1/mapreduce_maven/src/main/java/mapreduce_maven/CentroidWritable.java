package mapreduce_maven;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public final class CentroidWritable implements Writable , Cloneable {

	long count;
	List<Double> sumCoords;
	public CentroidWritable() {}

	public CentroidWritable(List<Double> coords) {
		sumCoords = coords;
		count = 1;
	}

	public CentroidWritable(int dim) {
		List<Double> coords = new ArrayList<Double>();
		for(int i=0;i<dim;i++) {
			coords.add(0.0);
		}
		sumCoords = coords;
		count = 0;
	}

	public CentroidWritable clone() {
		try {
			return (CentroidWritable)super.clone();
		}
		catch (Exception e) {
			System.err.println(e.getStackTrace());
			System.exit(-1);
		}
		return null;
	}

	void merge(CentroidWritable coords) {
		count += coords.count;
		for(int i=0;i<coords.sumCoords.size();i++){
			sumCoords.set(i, coords.sumCoords.get(i)+sumCoords.get(i));
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(sumCoords.size());
		for(int i=0;i<sumCoords.size();i++) {
			out.writeDouble(sumCoords.get(i));
		}
		out.writeLong(count);
	}

	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		List<Double> coords = new ArrayList<Double>();
		for(int i=0;i<size;i++) {
			coords.add(in.readDouble());
		}
		sumCoords = coords;
		count = in.readLong();
	}

	public String toString() {
		StringBuilder tmp = new StringBuilder();
		/*tmp.append("Nombre de points: ");
		tmp.append(count);
		tmp.append(", Coordonnées du nouveau centroid");*/
		
		List<Double> newCoordsCentroid = new ArrayList<Double>();;
		for(int i=0;i<sumCoords.size();i++) {
			newCoordsCentroid.add((double)sumCoords.get(i)/(double)count);
		}
		MultiDemPoint newCentroid = new MultiDemPoint(newCoordsCentroid);
		tmp.append(newCentroid.toString());
		tmp.append("\r\n");
		return tmp.toString();
	}
}
