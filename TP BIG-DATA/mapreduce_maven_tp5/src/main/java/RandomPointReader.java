import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RandomPointReader extends
		RecordReader<LongWritable, Point2DWritable> {
	private LongWritable lw = new LongWritable(0);
	private int number_of_points;
	private double x;
	private double y;
	private Random rdm = new Random();;

	@Override
	public void close() throws IOException {
		// on ne fait rien
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return lw;
	}

	@Override
	public Point2DWritable getCurrentValue() throws IOException,
			InterruptedException {
		return new Point2DWritable(this.x, this.y);
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// on ne fait rien
		return 0;
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {	
		this.number_of_points = Integer.parseInt(arg1.getConfiguration().get(
				"nbPointsPerSplit"));
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		this.x = rdm.nextInt(2);
		this.y = rdm.nextInt(2);
		
		lw.set(lw.get() + 1);
		this.number_of_points--;
		return number_of_points > 0;
	}

}
