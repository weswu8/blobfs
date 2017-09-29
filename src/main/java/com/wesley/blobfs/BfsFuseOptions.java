package com.wesley.blobfs;

import java.util.List;

public class BfsFuseOptions {
	  private final String mMountPoint;
	  private final String mBlobPrefix;
	  private final boolean mDebug;
	  private final List<String> mFuseOpts;

	  public BfsFuseOptions(String mountPoint, String blobPrefix, boolean debug, List<String> fuseOpts) {
	    mMountPoint = mountPoint;
	    mBlobPrefix = blobPrefix;
	    mDebug = debug;
	    mFuseOpts = fuseOpts;
	  }

	  /**
	   * @return The path to where the BlobFS should be mounted
	   */
	  public String getMountPoint() {
	    return mMountPoint;
	  }

	  /**
	   * @return The prefix of the blobs that will be used as the mounted BlobFS root
	   * (e.g. /container1/blob1/)
	   */
	  public String getBlobPrefix() {
	    return mBlobPrefix;
	  }

	  /**
	   * @return extra options to pass to the FUSE mount command
	   */
	  public List<String> getFuseOpts() {
	    return mFuseOpts;
	  }

	  /**
	   * @return whether the file system should be mounted in debug mode
	   */
	  public boolean isDebug() {
	    return mDebug;
	  }
}
