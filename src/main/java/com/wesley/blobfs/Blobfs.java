package com.wesley.blobfs;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Blobfs {
	private static Logger logger = LoggerFactory.getLogger("BfsPath.class");
	@SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception{
		final BfsFuseOptions opts = parseOptions(args);
	    if (opts == null) {
	      System.exit(1);
	    }
//		BlobBasics blobbasic = new BlobBasics();
		BfsFuseServiceImpl blobfs = new BfsFuseServiceImpl();
		final List<String> fuseOpts = opts.getFuseOpts();
		// Force direct_io in FUSE: writes and reads bypass the kernel page
	    // cache and go directly to blobfs. This avoids extra memory copies
	    // in the write path.
	    fuseOpts.add("-odirect_io");
        try {
//			blobbasic.runSamples();
			blobfs.mount(Paths.get(opts.getMountPoint()), true, opts.isDebug(),
			          fuseOpts.toArray(new String[0]));
		} finally {
			blobfs.umount();
        }

	}
	 /**
	 * Parses CLI options.
	 *
	 * @param args CLI args
	 * @return BlobFs configuration options
	 */
	 private static BfsFuseOptions parseOptions(String[] args) {
	    final Options opts = new Options();
	    final Option mntPoint = Option.builder("m")
	        .hasArg()
	        .required(false)
	        .longOpt("mount-point")
	        .desc("Desired local mount point for BlobFs.")
	        .build();

	    final Option blobPrefix = Option.builder("b")
	        .hasArg()
	        .required(false)
	        .longOpt("blob-prefix")
	        .desc("The prefix of the blobs that will be used as the mounted BlobFS root "
	            + "(e.g., /container1/blob1/; defaults to /)")
	        .build();

	    final Option help = Option.builder("h")
	        .required(false)
	        .longOpt("help")
	        .desc("Print this help")
	        .build();

	    final Option fuseOption = Option.builder("o")
	        .valueSeparator(',')
	        .required(false)
	        .hasArgs()
	        .desc("FUSE mount options")
	        .build();

	    opts.addOption(mntPoint);
	    opts.addOption(blobPrefix);
	    opts.addOption(help);
	    opts.addOption(fuseOption);

	    final CommandLineParser parser = new DefaultParser();
	    try {
	      CommandLine cli = parser.parse(opts, args);

	      if (cli.hasOption("h")) {
	        final HelpFormatter fmt = new HelpFormatter();
	        fmt.printHelp(Blobfs.class.getName(), opts);
	        return null;
	      }

	      String mntPointValue = cli.getOptionValue("m");
	      String blobPrefixValue = cli.getOptionValue("b");

	      List<String> fuseOpts = new ArrayList<>();
	      boolean noUserMaxWrite = true;
	      if (cli.hasOption("o")) {
	        String[] fopts = cli.getOptionValues("o");
	        // keep the -o
	        for (final String fopt: fopts) {
	          fuseOpts.add("-o" + fopt);
	          if (noUserMaxWrite && fopt.startsWith("max_write")) {
	            noUserMaxWrite = false;
	          }
	        }
	      }
	      // check if the user has specified his own max_write, otherwise get it
	      // from conf
	      if (noUserMaxWrite) {
	           final long maxWrite = Constants.DEFAULT_MAXWRITE_BYTES;
	           fuseOpts.add(String.format("-omax_write=%d", maxWrite));
	      }

	      if (mntPointValue == null) {
			   mntPointValue = Constants.DEFAULT_MOUNT_POINT;
			   logger.info("Mounting on default {}", mntPointValue);
	      }

	      if (blobPrefixValue == null) {
	    	  blobPrefixValue = Constants.DEFAULT_BLOB_PREFIX;
	    	  logger.info("Using default blobfs root {}", blobPrefixValue);
	      }

	      final boolean fuseDebug = Constants.BFS_DEBUG_ENABLED;
	      return new BfsFuseOptions(mntPointValue, blobPrefixValue, fuseDebug, fuseOpts);
	      
	    } catch (ParseException e) {
	      System.err.println("Error while parsing CLI: " + e.getMessage());
	      final HelpFormatter fmt = new HelpFormatter();
	      fmt.printHelp(Blobfs.class.getName(), opts);
	      return null;
	    }
	  }
}
