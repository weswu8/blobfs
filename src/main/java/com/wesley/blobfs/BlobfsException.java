package com.wesley.blobfs;

import java.io.IOException;

public class BlobfsException extends IOException {
	
	  private static final long serialVersionUID = 1L;
	
	  public BlobfsException(String message) {
	    super(message);
	  }
	
	  public BlobfsException(String message, Throwable cause) {
	    super(message, cause);
	  }
	
	  public BlobfsException(Throwable t) {
	    super(t);
	  }
}
