import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class Lab1Ex2 {
	public static void main ( String [] args ) throws Exception {
		// The system configuration
		Configuration conf = new Configuration();

		// Get an instance of the Filesystem
		FileSystem fs = FileSystem.get(conf);
		
		// Set and build path to file to read from
		String path_name = "/cpre419/bigdata";

		Path path = new Path(path_name);
		
		// The Output Data Stream to write into
		FSDataInputStream file = fs.open(path);

		// Read 1000 byte block from file of [1000000000, 1000000999]
		byte[] buf = new byte[1000];
		file.readFully(1000000000, buf, 0, 1000);
		
		// Calculate XOR checksum
		byte xor = 0;
		for(int i = 0; i < buf.length; i++)
			xor ^= buf[i];
		
		System.out.println("Checksum: " + xor);
		
		// Close the file and the file system instance
		file.close();
		fs.close();
	}
}
