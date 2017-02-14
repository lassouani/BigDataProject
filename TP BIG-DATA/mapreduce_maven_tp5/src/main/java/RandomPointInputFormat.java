import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RandomPointInputFormat extends
		InputFormat<LongWritable, Point2DWritable> {

	@Override
	public RecordReader<LongWritable, Point2DWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new RandomPointReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException,
			InterruptedException {
		ArrayList<InputSplit> list = new ArrayList<InputSplit>();
		int end = arg0.getConfiguration().getInt("nbMapper", 0);
		for (int i = 0; i < end; i++) {
			list.add(new FakeInputSplit());
		}
		return list;
	}
}
