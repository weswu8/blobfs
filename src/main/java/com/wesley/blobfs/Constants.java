package com.wesley.blobfs;

public final class Constants {
	
	/* the configuration file */
	public static final String	CONFIGURATION_FILE = "./blobfs.conf";
	
	/* custom configurable options */
	public static final String	STORAGE_CONNECTION_STRING = Configurations.getProperty("Storage_Connection_String");
	public static final String	DEFAULT_CHARSET = Configurations.getProperty("charset");
	//public static final String	DEFAULT_TIMEZONE = Configurations.getProperty("timezone");
	public static final String  DEFAULT_BLOB_PREFIX = Configurations.getProperty("blob_prefix");
	public static final String  DEFAULT_MOUNT_POINT = Configurations.getProperty("mount_point");
	public static final int 	DEFAULT_BFS_UID = Integer.parseInt(Configurations.getProperty("uid"));
	public static final int 	DEFAULT_BFS_GID = Integer.parseInt(Configurations.getProperty("gid"));
	public static final boolean  BFS_DEBUG_ENABLED = Boolean.parseBoolean(Configurations.getProperty("debug_enabled"));
	public static final String  DEFAULT_BFS_LOG_ENVIROMENT = Configurations.getProperty("log_environment");
	public static final boolean  BFS_CLUSTER_ENABLED = Boolean.parseBoolean(Configurations.getProperty("cluster_enabled"));
	public static final String  SERVICE_BUS_CONNECTION_STRING = Configurations.getProperty("service_bus_connection_string");
	public static final String 	SERVICE_BUS_TOPIC = Configurations.getProperty("service_bus_topic");	
	public static final String 	SERVICE_BUS_SUBSCRIPTION = Configurations.getProperty("service_bus_subscription");
	public static final boolean AUTO_CHANGE_BLOCK_BLOB_TO_APPEND_BLOB = ("true".equals(Configurations.getProperty("auto_change_block_blob_to_append_blob").toLowerCase())) ? true : false;
	public static final int 	BLOB_BUFFERED_INS_DOWNLOAD_SIZE = Integer.parseInt(Configurations.getProperty("read_buffer_size"));
	public static final int 	BLOB_BUFFERED_OUTS_BUFFER_SIZE = Integer.parseInt(Configurations.getProperty("write_buffer_size"));
	public static final int 	BLOB_BUFFERED_OUTS_BLOCKBLOB_CHUNK_SIZE = Integer.parseInt(Configurations.getProperty("block_blob_upload_chunk_size"));

	/* non custom configurable options */
	public static final String	PATH_DELIMITER = "/";
	public static final String	VIRTUAL_DIRECTORY_NODE_NAME = "$.$$";
	public static final String  BLOBFS_TEMP_FILE_PREFIX = "bfs";
	public static final String	GET_UID_ON_UNIX_CMD = "id -u";
	public static final String	GET_GID_ON_UNIX_CMD = "id -g";
	public static final String  BLOB_META_DATA_LEASE_ID_KEY = "leaseID";
	public static final String 	BLOB_META_DATA_COMMITED_BLOBKS_KEY = "commitedBlocks";
	public static final String 	BLOB_META_DATA_ISlINK_KEY = "isLink";
	public static final String  BLOB_META_DATE_FORMAT = "E, dd-MMM-yy HH:mm:ss.SSS";
	public static final String  BLOB_META_DATE_UID_KEY = "uid";
	public static final String  BLOB_META_DATE_GID_KEY = "gid";
	public static final long  	DEFAULT_MAXWRITE_BYTES = 128 * 1024;//default 128K

	public static final int 	BFS_FILES_CACHE_INIT_CAPACITY = 1024; //1024
	public static final int 	BFS_FILES_CACHE_MAX_CAPACITY = 65535; //opened file
	public static final int 	BFS_FILES_CACHE_EXPIRE_TIME = 7 * 24 * 60 * 60 * 1000; //7 days
	
	public static final int		DEFAULT_BLOB_OPRATION_RETRY_TIMES = 3;
	public static final int		DEFAULT_THREAD_SLEEP_MILLS = 100;
	public static final int		DEFAULT_BFC_THREAD_SLEEP_MILLS = 2 * 100; // 200 mill seconds
	public static final int		DEFAULT_OFM_THREAD_SLEEP_MILLS = 10 * 1000; // 20 seconds
	public static final int 	CONCURRENT_REQUEST_COUNT = 4;
	public static final int 	SINGLE_BLOB_PUT_THRESHOLD = 16 * 1024 * 1024; //16MB
	public static final int 	STREAM_WRITE_SIZE = 4 * 1024 * 1024; //4MB	
	public static final int 	COMMAND_EXECUTION_BUFFER_SIZE = 4 * 1024;
	public static final int 	DEFAULT_BFS_CACHE_CAPACITY = 1 * 1024;
	public static final int 	DEFAULT_BFS_CACHE_EXPIRE_TIME = 5 * 1000; //5 seconds
	public static final int 	BLOB_BUFFER_INS_CACHE_INIT_CAPACITY = 4; //8
	public static final int 	BLOB_BUFFER_INS_MAX_CAPACITY = 8; //8
	public static final int 	BLOB_BUFFER_INS_EXPIRE_TIME = 5 * 60 * 1000; //5 minutes
	public static final int 	OPENED_FILE_MANAGER_INIT_CAPACITY = 1024; //1024
	public static final int 	OPENED_FILE_MANAGER_MAX_CAPACITY = 65535; //opened file
	public static final int 	OPENED_FILE_MANAGER_EXPIRE_TIME = 7 * 24 * 60 * 60 * 1000; //7 days

	/* set the retry policy */
	public static final int 	UPLOAD_BACKOFF_INTERVAL = 1 * 1000; // 1s
	public static final int 	UPLOAD_RETRY_ATTEMPTS = 15;
	
	public static final int 	APPENDBLOB_SPLIT_CHUNK_SIZE = 4 * 1000 * 1000; // default is 4MB
	public static final int 	UPLOAD_MULTIPARTS_SPLIT_CHUNK_SIZE = 4 * 1024 * 1024; // default is 4MB
	public static final int 	DOWNLOAD_MULTIPARTS_SPLIT_CHUNK_SIZE = 4 * 1024 * 1024; // default is 4MB
	public static final int 	BLOB_INS_DOWNLOAD_SIZE = 4 * 1024 * 1024; // default is 4MB
	public static final int 	BLOB_INS_CENTRAL_BUFFER_SIZE = 4 * 1024 * 1024; // default is 4MB
	public static final int		BLOB_INPUTSTREAM_MAXIMUM_READ_SIZE = 4 * 1024 * 1024; // default is 4MB
	public static final int 	BLOB_BUFFERED_OUTS_APPENDBLOB_CHUNK_SIZE = 4 * 1024 * 1024; // default is 4MB
	public static final int 	BLOB_BLOCK_NUMBER_LIMIT = 50000; // default is 50000
	public static final long 	APPENDBLOB_SIZE_LIMIT = 195L * 1024L * 1024L * 1024L; //default is 195GB
	public static final int 	APPENDBLOB_BLOCK_SIZE_LIMIT = 4 * 1024 * 1024; //default is 4MB
	public static final long 	BLOCKBLOB_SIZE_LIMIT = 5L * 1024L * 1024L * 1024L * 1024L; //default is 5TB
	public static final int 	BLOCKBLOB_BLOCK_SIZE_LIMIT = 4 * 1024 * 1024; //default is 4MB
	public static final long 	PAGEBLOB_SIZE_LIMIT = 1L * 1024L * 1024L * 1024L * 1024L; //default is 1TB
	public static final int		PAGEBLOB_MINIMUM_SIZE = 512;	
	public static final int		BLOB_LOCKED_SECONDS = 60;  /* leaseTimeInSeconds should be between 15 and 60 */



}
