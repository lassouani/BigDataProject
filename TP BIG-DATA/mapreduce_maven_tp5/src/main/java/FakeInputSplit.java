import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class FakeInputSplit extends InputSplit implements Writable {

	@Override
	public long getLength() throws IOException, InterruptedException {
		// 0 ou un autre
		return 1;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		// tableau vide de string
		String tab[] = {};
		return tab;
	}

	public void readFields(DataInput arg0) throws IOException {
		// on fait rien
	}

	public void write(DataOutput arg0) throws IOException {
		// on fait rien
	}

}
