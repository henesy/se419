import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
//import 

public class Lab1Ex2 {
 public static void main ( String [] args ) throws Exception {
	 // The system configuration
	 Configuration conf = new Configuration();
	 // Get an instance of the Filesystem
	 FileSystem fs = FileSystem.get(conf);

	 String path_name = "/cpre419/bigdata";

	 Path path = new Path(path_name);

	 // The Output Data Stream to write into
	 FSDataInputStream file = fs.open(path);

	// seek to first offset
	//file.seek(;

	byte[] buf = new byte[1000];
	file.readFully(1000000000, buf, 0, 1000);

	// calculte checksum

	byte xor = 0;
	for(int i = 0; i < buf.length; i++)
		xor ^= buf[i];

	System.out.println("Checksum: " + xor);

	 // Close the file and the file system instance
	 file.close();
	 fs.close();
	}
}
