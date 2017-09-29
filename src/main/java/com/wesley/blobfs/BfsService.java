package com.wesley.blobfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jnr.constants.platform.OpenFlags;
import com.microsoft.azure.storage.StorageException;

import jnr.ffi.Pointer;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.Timespec;

/**
 * @author weswu
 *
 */
@SuppressWarnings("static-access")
public class BfsService {
	private static Logger logger = LoggerFactory.getLogger("BfsService.class");
	private static BfsService instance;
	private static boolean clusterEnabled = Constants.BFS_CLUSTER_ENABLED;
	public static int bfsUid = BfsFuseServiceImpl.bfsUid;
	public static int bfsGid = BfsFuseServiceImpl.bfsGid;
    /** read-only flag (default value if no other flags set) */
    public static final int RDONLY = OpenFlags.O_RDONLY.intValue();
    /** write-only flag */
    public static final int WRONLY = OpenFlags.O_WRONLY.intValue();
    /** read/write flag */
    public static final int RDWR = OpenFlags.O_RDWR.intValue();
    /** create flag, to specify non-existing file should be created */
    public static final int CREAT = OpenFlags.O_CREAT.intValue();
    /** exclusive access flag, to require locking the target file */
    public static final int EXCL = OpenFlags.O_EXCL.intValue();
    /** truncate flag, to truncate the target file to zero length */
    public static final int TRUNC = OpenFlags.O_TRUNC.intValue();
    /** append flag, to seek to the end of the file */
    public static final int APPEND = OpenFlags.O_APPEND.intValue();
    
    /* Table of opened files with corresponding InputStreams and OutputStreams */
    private final OpenedFilesManager openedFilesManager = OpenedFilesManager.getInstance();
    private long openedFileIds;
    private final BfsFilesCache bfsFilesCache = BfsFilesCache.getInstance();
    
    private BfsService(){
    	openedFileIds = 1L;
    }
    
    public static BfsService getInstance(){
        if(instance == null){
            synchronized (BfsService.class) {
                if(instance == null){
                    instance = new BfsService();
                }
            }
        }
        return instance;
    }   
        
    /**
     * fill the stat struct with blob properties
     * @param path
     * @param stat
     * @return 0, success
     */
	public final int bfsGetFileAttributes(String path, FileStat stat){
		logger.trace("Geting attributes {}", path);
		int result = -ErrorCodes.EFAULT();
    	try {
			/* null or space value */
			if (null == path || path.trim() == "") {
				return -ErrorCodes.ENOENT();
			}
			/* check the cache first */
			if (bfsFilesCache.has(path)) {
				logger.trace("Geting attributes {} from cache", path);
				bfsFilesCache.getFileStat(path, stat);
				return 0;
			}
			BfsPath bfsPath = new BfsPath(path);
			PathProperties pathProperties = bfsPath.getBfsPathProperties();
			if (!bfsPath.fillFileStat(pathProperties, stat)){
				/* invalid  directory*/
				return -ErrorCodes.ENOENT();
			}
			
			/* default use uid and gid of the user running blobfs, can be changed later */		
    		stat.st_uid.set(bfsUid);
			stat.st_gid.set(bfsGid);
			/* put the stat into cache */
			bfsFilesCache.put(path, pathProperties);
			result = 0;
		} catch (Exception ex) {
			logger.error(ex.getMessage());
//			ex.printStackTrace();
		}
    	return result;
	}

    public final int bfsCheckFileAccess(){
    	int result = -ErrorCodes.EFAULT();
    	return 0;
    }
    
    /**
     * read the virtual directory
     * @param path
     * @param buf
     * @param filter
     * @param offset
     * @param fi
     * @return
     */
    public int bfsReaddir(String path, Pointer buf, FuseFillDir filter, @off_t long offset, FuseFileInfo fi) {
		logger.trace("Reading directory {}", path);
    	int result = -ErrorCodes.EFAULT();
    	try {
    		BfsPath bfsPath = new BfsPath(path);
    		BfsPathType bfsPathType = bfsPath.getBfsPathProperties().getBfsPathType();
			if ("ROOT".equals(bfsPathType.toString())) {
	    		/* is the root directory */
				filter.apply(buf, ".", null, 0);
		        filter.apply(buf, "..", null, 0);
				List<String> containerNames = ContainerService.getAllContainersName();
				for (String containerName : containerNames){
					if (filter.apply(buf, containerName, null, 0) == 1) {
						return -ErrorCodes.ENOMEM();
					}
				}
				result = 0;
			} else if ("CONTAINER".equals(bfsPathType.toString()) || "SUBDIR".equals(bfsPathType.toString())) {
				/* is the container or virtual directory */
				BlobReqParams reqParams = new BlobReqParams();
				reqParams.setVirtualDirOptMode(VirtualDirOptMode.HBLOBPROPS);
				reqParams.setContainer(bfsPath.getContainer());
				reqParams.setBlob(bfsPath.getBlob());
				reqParams.setBfsBlobType(BfsBlobType.BLOCKBLOB);
				List<BfsBlobModel> bfsBlobModels = BlobService.getBlobsWithinVirtualDirectory(reqParams);
				filter.apply(buf, ".", null, 0);
		        filter.apply(buf, "..", null, 0);
		        for (BfsBlobModel bfsBlobModel : bfsBlobModels){
		        	//logger.trace("calling filler with name {}", bfsBlobModel.getBlobName());
		        	/* for fuse issue: the dir end with slash will cause the fuse error */
		        	if (bfsBlobModel.getBlobName().endsWith(Constants.VIRTUAL_DIRECTORY_NODE_NAME)){
		        		continue;
		        	}
		        	String cPath = new BfsPath(bfsBlobModel.getBlobName()).getCurrentPath();
					if (filter.apply(buf, cPath, null, 0) == 1) {
						return -ErrorCodes.ENOMEM();
					 }
				}
		    	result = 0;
			} 
		} catch (BlobfsException ex) {
			logger.error(ex.getMessage());
			//ex.printStackTrace();
		} catch (Exception ex) {
			logger.error(ex.getMessage());
			//ex.printStackTrace();
		}
        return result;
    }
    
    /**
     * @param path
     * @param fi
     * @return
     */
    public int bfsFlush(String path, FuseFileInfo fi) {
    	logger.trace("flush {}", path);
    	final long fd = fi.fh.get();
    	OpenedFileModel ofm;
    	synchronized (openedFilesManager) {
    		ofm = (OpenedFileModel) openedFilesManager.get(fd);
    	}
	    if (ofm == null) {
	    	logger.error("Cannot find fd for {} in table", path);
	    	return -ErrorCodes.EBADFD();
	    }
    	if (ofm.getbOut() != null) {
    	    try {
    	    	ofm.getbOut().flush();
    	    } catch (IOException e) {
    	        return -ErrorCodes.EIO();
    	    }
    	}
        return 0;
    }
    /**
     * @param path
     * @param fi
     * @return
     */
    public int bfsRelease(String path, FuseFileInfo fi) {
    	logger.trace("release({})", path);
        final long fd = fi.fh.get();
        OpenedFileModel ofm;
    	synchronized (openedFilesManager) {
    		ofm = (OpenedFileModel) openedFilesManager.get(fd);
    		if (ofm == null) {
    			logger.error("Cannot find fd for {} in table", path);
    			return -ErrorCodes.EBADFD();
    		}
    	}
        try {
        	/* if it is the write operation, we should update the time */
        	if (ofm.getbOut() != null){updateCacheAndNotifyPeers(path, BfsPathType.BLOB);}
          	ofm.close();
        } catch (IOException e) {
        	logger.error("Failed closing {}", path, e);
        }
        /* clean the table */
        openedFilesManager.delete(fd);
        return 0;
      }
    /**
     * @param path
     * @param fi
     * @return
     */
    public int bfsOpen(String path, FuseFileInfo fi) {
    	int result = -ErrorCodes.EFAULT();
    	final int flags = fi.flags.get();
    	final long fd = fi.fh.get();
    	/* already opened */
    	if (fd > 0){
    		return 0;
    	}
    	logger.trace("open {} , 0x{}, fd:{}", path, Integer.toHexString(flags), fd);
    	boolean isAPPEND = (flags & APPEND) != 0;
    	boolean isREAD = (flags & RDONLY) != 0;
    	boolean isWRITE = (flags & WRONLY) != 0;
    	boolean isUPADAT = (flags & RDWR) != 0;
    	if (isWRITE && isUPADAT) { return -ErrorCodes.EINVAL(); }     
        if (!isWRITE) {
            if (isUPADAT) {	isWRITE = true;
            } else { isREAD = true; }
        }
	    try {
	    	BfsPath bfsPath = new BfsPath(path);			
			BlobReqParams insParams = new BlobReqParams();
			insParams.setContainer(bfsPath.getContainer());
			insParams.setBlob(bfsPath.getBlob());
			/* get the blob type */
			BfsBlobType bfsBlobType = BlobService.getBlobType(insParams);
			insParams.setBfsBlobType(bfsBlobType);
			/* check the append flag */
			if (isAPPEND){				
				if ("BLOCKBLOB".equals(bfsBlobType.toString()) && Constants.AUTO_CHANGE_BLOCK_BLOB_TO_APPEND_BLOB){
					if (BlobService.changeBlocBlobToAppendBlob(insParams)){
						/*update the blob type*/
						insParams.setBfsBlobType(BfsBlobType.APPENDBLOB);
					} else {
						logger.error("File can not be changed to O_APPEND mode ({})", path);
						return -ErrorCodes.EIO();
					}
				} else if ("BLOCKBLOB".equals(bfsBlobType.toString()) || "PAGEBLOB".equals(bfsBlobType.toString())) {
				    logger.error("File can not be opened in O_APPEND mode ({})", path);
				    return -ErrorCodes.EACCES();
				}
			}			
	        synchronized (openedFilesManager) {
	        	BlobBufferedIns bbIns = null;
	        	if (isREAD)	{  bbIns =	new BlobBufferedIns(insParams); }
	        	BlobBufferedOus bbOus = null;
	        	if (isWRITE) {	bbOus = new BlobBufferedOus(insParams); }
	            final OpenedFileModel ofe = new OpenedFileModel(bbIns, bbOus);
	            if (isWRITE){ ofe.setLeaseID(bbOus.getLeaseID());}
				ofe.setContainer(bfsPath.getContainer());
				ofe.setBlob(bfsPath.getBlob());
	            if (! openedFilesManager.put(openedFileIds, ofe)){
	            	if (openedFilesManager.count() > Constants.OPENED_FILE_MANAGER_MAX_CAPACITY){
		            	logger.error("Cannot open {}: too many open files", path);
		                return -ErrorCodes.EMFILE();
	            	} else {
	            		return -ErrorCodes.EIO();
	            	}
	            }	           
	            fi.fh.set(openedFileIds);
	            /* increase the file descriptor */
	            openedFileIds += 1;	            
				return 0;
	        }	        
		} catch (IOException | StorageException ex) {
			logger.error(ex.getMessage());
			//ex.printStackTrace();
		}
	    result = 0;
        return result;
    }
    
    /**
     * create a file
     * @param path
     * @param mode
     * @param fi
     * @return
     */
    public int bfsCreate(String path, @mode_t long mode, FuseFileInfo fi) {
    	int result = -ErrorCodes.EFAULT();
    	final int flags = fi.flags.get();
    	logger.trace("create {} , 0x{}", path, Integer.toHexString(flags));
    	boolean isAPPEND = (flags & APPEND) != 0;
    	boolean isREAD = (flags & RDONLY) != 0;
    	boolean isWRITE = (flags & WRONLY) != 0;
    	boolean isUPADAT = (flags & RDWR) != 0;
    	
    	if (isWRITE && isUPADAT) { return -ErrorCodes.EINVAL(); }     
        if (!isWRITE) {
            if (isUPADAT) {	isWRITE = true;
            } else { isREAD = true; }
        }    
    	try {
    		BfsPath bfsPath = new BfsPath(path);
    		BlobReqParams createParams = new BlobReqParams();
    		createParams.setContainer(bfsPath.getContainer());
    		createParams.setBlob(bfsPath.getBlob());
    		/* set blob type */
    		if (isAPPEND) {createParams.setBfsBlobType(BfsBlobType.BLOCKBLOB);}
    		else {createParams.setBfsBlobType(BfsBlobType.BLOCKBLOB);}
    		synchronized (openedFilesManager) {
    			if (BlobService.createBlob(createParams)){
    				/* set the uid and gid for the file */
    				setBlobUidandGid(createParams, bfsGid, bfsGid);
    				BlobBufferedIns bbIns = null;
    	        	if (isREAD)	{  bbIns =	new BlobBufferedIns(createParams); }
    	        	BlobBufferedOus bbOus = null;
    	        	if (isWRITE) {	bbOus = new BlobBufferedOus(createParams); }
    				@SuppressWarnings("resource")
					final OpenedFileModel ofe = new OpenedFileModel(bbIns, bbOus);
    				if (isWRITE) {	ofe.setLeaseID(bbOus.getLeaseID()); }
    				ofe.setContainer(bfsPath.getContainer());
    				ofe.setBlob(bfsPath.getBlob());
    				if (! openedFilesManager.put(openedFileIds, ofe)){
    	            	if (openedFilesManager.count() > Constants.OPENED_FILE_MANAGER_MAX_CAPACITY){
    		            	logger.error("Cannot open {}: too many open files", path);
    		                return -ErrorCodes.EMFILE();
    	            	} else {
    	            		return -ErrorCodes.EIO();
    	            	}
    	            }
    	            fi.fh.set(openedFileIds);
    	            /* increase the file descriptor */
    	            openedFileIds += 1;
    	            
    	            /* delete the cache */
    				if (bfsFilesCache.has(path)) {
    					bfsFilesCache.delete(path);
    					return 0;
    				}
    				
    				result = 0;
    			} else {
    				result = -ErrorCodes.ENOENT();
    			}    		
    	    }
		} catch (BlobfsException | StorageException ex) {
			logger.error(ex.getMessage());
			//ex.printStackTrace();
		}    	
        return result;
    }
    
    /**
     * @param path
     * @param buf
     * @param size
     * @param offset
     * @param fi
     * @return
     */
    public int BfsRead(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
		logger.trace("read file {}, offset = {}, read length = {}", path, offset, size);
		int readResult = 0;
		try {
			final long fd = fi.fh.get();
		    OpenedFileModel ofm;
		    synchronized (openedFilesManager) {
		    	ofm = (OpenedFileModel) openedFilesManager.get(fd);
		    }
		    if (ofm == null) {
		    	logger.error("Cannot find fd for {} in table", path);
		    	return -ErrorCodes.EBADFD();
		    }
		    BlobBufferedIns bbIns = ofm.getbIn();
		    if (bbIns == null) {
		    	logger.error("{} was not open for reading", path);
		    	return -ErrorCodes.EBADFD();
		    }
			/* avoid the out of boundary error */
		    int bytesToRead = (int) Math.min(bbIns.blobSize - offset, size);
			byte[] bytesRead = new byte[bytesToRead];
			synchronized (bbIns) {
				if ((readResult = bbIns.read(bytesRead, (int)offset, bytesToRead)) != -1){
					buf.put(0, bytesRead, 0, readResult);
					logger.trace("read offset {} got {} bytes", offset, readResult);
				} else {
				/*  reach the end of file */	
					readResult = 0;
				}
			}
		} catch (IOException ex) {
			logger.error(ex.getMessage());
			//ex.printStackTrace();
		}
        return readResult;
    }    
    

    public int bfsSetxattr(String path, String name, Pointer value, @size_t long size, int flags){
    	logger.trace("setxattr {}", path);
    	return 0;
    }
    
    /**
     * due to the blob only has the time last modified.
 	*/
    public int bfsUtimens(String path, Timespec[] timespec){
    	logger.trace("utimens {}", path);
    	return 0;
    }
    
    public int bfsMkdir(String path, @mode_t long mode) {
		logger.trace("creates directory {}", path);
    	int result = -ErrorCodes.EFAULT();
    	try {
    		BfsPath bfsPath = new BfsPath(path);
    		/* it is container */
    		if (!bfsPath.getContainer().equals("")  && bfsPath.getBlob().equals("")){
    			if (ContainerService.containerExists(bfsPath.getContainer())) {					
					return -ErrorCodes.ENOENT();
				} else {
					if(ContainerService.createContainer(bfsPath.getContainer(), ContainerPermissionType.PRIVATE)){
						/* set the uid and gid for the dir */
	    				setContainerUidandGid(bfsPath.getContainer(), bfsGid, bfsGid);
						result = 0;
    	 			} else {
    	    			return -ErrorCodes.EIO();
    	    		}   
				}
    		} else if (!bfsPath.getContainer().equals("")  && !bfsPath.getBlob().equals("")){
    			BlobReqParams mkdirParams = new BlobReqParams();
        		mkdirParams.setContainer(bfsPath.getContainer());
        		mkdirParams.setBlob(bfsPath.getBlob());
    			if (BlobService.virtualDirectoryExists(mkdirParams)){
        			return -ErrorCodes.EEXIST();
        		} else {
        			if (BlobService.createVirtualDirectory(mkdirParams)){
        				mkdirParams.setBlob(bfsPath.getBlob()  + Constants.PATH_DELIMITER + Constants.VIRTUAL_DIRECTORY_NODE_NAME);
        				/* set the uid and gid for the file */
        				setBlobUidandGid(mkdirParams, bfsGid, bfsGid);
        				result = 0;
    	 			} else {
    	    			return -ErrorCodes.EIO();
    	    		}    
        		}
    		}
		} catch ( Exception ex) {
			logger.error(ex.getMessage());
			//ex.printStackTrace();
		}    	
        return result;    	
    }
    
    public int bfsRename(String path, String newName) 
    {
    	logger.trace("rename direcotry/file from {} to {}", path, newName);
    	int result = -ErrorCodes.EFAULT();
    	try {
      		List<String> failedFilesList = new ArrayList<String>();
    		BfsPath srcPath = new BfsPath(path);
    		BfsPathType srcPathType = srcPath.getBfsPathProperties().getBfsPathType();
    		BfsPath destPath = new BfsPath(newName);
    		/* set the move params */
    		BlobReqParams rnParams = new BlobReqParams();
    		rnParams.setBlobOptMode(BlobOptMode.MOVE);
    		rnParams.setContainer(srcPath.getContainer());
    		rnParams.setBlob(srcPath.getBlob());
    		rnParams.setDestContainer(destPath.getContainer());
    		rnParams.setDestBlob(destPath.getBlob());
    		if ("CONTAINER".equals(srcPathType.toString()) || "SUBDIR".equals(srcPathType.toString())){
    			if (BlobService.CopyOrmoveVirtualDirectory(failedFilesList, rnParams)){
    				updateCacheAndNotifyPeers(path, BfsPathType.SUBDIR);
     				result = 0;
	 			} else {
	    			return -ErrorCodes.EIO();
	    		}    
    			
    		} else if ("BLOB".equals(srcPathType.toString())) {
    			if (BlobService.CopyOrmoveBlobCrossContainer(rnParams)){
    				updateCacheAndNotifyPeers(path, BfsPathType.BLOB);
     				result = 0;
     			} else {
        			return -ErrorCodes.EIO();
        		}    
    			
    		} else {
    			return -ErrorCodes.ENOTDIR();
    		}   		 
		} catch (BlobfsException ex) {
			logger.error(ex.getMessage());
			//ex.printStackTrace();
		}    	
        return result;    	
    }
    
    public int bfsRmdir(String path) {
    	logger.trace("delete direcotry {}", path);
    	int result = -ErrorCodes.EFAULT();
    	try {
    		BfsPath delDirPath = new BfsPath(path);
    		BfsPathType delPathType = delDirPath.getBfsPathProperties().getBfsPathType();    		
    		if ("CONTAINER".equals(delPathType.toString())){
    			ContainerService.deleteContainer(delDirPath.getContainer());
    			updateCacheAndNotifyPeers(path, BfsPathType.CONTAINER);
    			return 0; 			
    		} else if ("SUBDIR".equals(delPathType.toString())){
    			/* set the directory params */
    			List<String> failedFilesList = new ArrayList<String>();
        		BlobReqParams delDirParams = new BlobReqParams();
        		delDirParams.setContainer(delDirPath.getContainer());
        		delDirParams.setBlob(delDirPath.getBlob());
        		if (BlobService.deleteVirtualDirectory(failedFilesList, delDirParams)){
        			updateCacheAndNotifyPeers(path, BfsPathType.SUBDIR);
        			return 0; 
        		} else {
        			return -ErrorCodes.EIO();
        		}    			
    		} else {
    			return -ErrorCodes.ENOTDIR();
    		}    		 
		} catch (Exception ex) {
			logger.error(ex.getMessage());
			//ex.printStackTrace();
		}    	
        return result;  
     }
    
    public int bfsUnlink(String path) {
    	logger.trace("delete file {}", path);
    	int result = -ErrorCodes.EFAULT();
    	try {
    		BfsPath delFielPath = new BfsPath(path);
    		if ("".equals(path.trim()) || delFielPath.getContainer().equals("")){
    			return -ErrorCodes.ENOENT();
    		}
    		BlobReqParams delFileParams = new BlobReqParams();
			delFileParams.setContainer(delFielPath.getContainer());
			delFileParams.setBlob(delFielPath.getBlob());
    		if(BlobService.deleteBlob(delFileParams)){
    			updateCacheAndNotifyPeers(path, BfsPathType.BLOB);
    			return 0; 
    		} else {
    			return -ErrorCodes.EIO();
    		}    		
		} catch (Exception ex) {
			logger.error(ex.getMessage());
			//ex.printStackTrace();
		}    	
        return result;    	
    }
    
    public int bfsTruncate(String path, long offset) {
    	logger.trace("truncate file {}, offset {}", path, offset);
    	int result = -ErrorCodes.EFAULT();
    	try {
    		BfsPath trctFielPath = new BfsPath(path);
    		if ("".equals(path.trim()) || trctFielPath.getContainer().equals("")){
    			return -ErrorCodes.ENOENT();
    		}
    		BlobReqParams trctFileParams = new BlobReqParams();
    		trctFileParams.setContainer(trctFielPath.getContainer());
    		trctFileParams.setBlob(trctFielPath.getBlob());
    		trctFileParams.setBlobSize(offset);
    		/* set a dummyID, indicate that there is real leaseID in the meta data */
    		trctFileParams.setLeaseID("dummyID");
    		if (BlobService.changeBlobSize(trctFileParams)){
    			updateCacheAndNotifyPeers(path, BfsPathType.BLOB);
    			return 0; 
    		} else {
    			return -ErrorCodes.EIO();
    		}  		 
		} catch (Exception ex) {
			logger.error(ex.getMessage());
			//ex.printStackTrace();
		}    	
        return result;
    }
    
    public int bfsWrite(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    	logger.trace("write file {}, offset {}, size {}", path, offset, size);
	    final int bytesToWrite = (int) size;
    	try {
    		final long fd = fi.fh.get();
		    OpenedFileModel ofm;
		    synchronized (openedFilesManager) {
		    	ofm = (OpenedFileModel) openedFilesManager.get(fd);
		    }
		    if (ofm == null) {
		    	logger.error("Cannot find fd for {} in table", path);
		    	return -ErrorCodes.EBADFD();
		    }
		    BlobBufferedOus bbOus = ofm.getbOut();
		    if (bbOus == null) {
		    	logger.error("{} was not open for writing", path);
		    	return -ErrorCodes.EBADFD();
		    }
		    final byte[] bytesWrite = new byte[bytesToWrite];
		    synchronized (bbOus) {
			    buf.get(0, bytesWrite, 0, bytesToWrite);
			    logger.trace("write offset {} written {} bytes", offset, bytesToWrite);
			    bbOus.write(bytesWrite);
		    }
		} catch (Exception ex) {
			logger.error(ex.getMessage());
			//ex.printStackTrace();
		}    	
        return bytesToWrite;    	
    }

	public int bfsRemovexattr(String path, String name) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int bfsGetxattr(String path, String name, Pointer value, long size) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int bfsLink(String oldpath, String newpath) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int bfsSymlink(String oldpath, String newpath) {
		logger.trace("creates symbolic link from {} to {}", oldpath, newpath);
    	int result = -ErrorCodes.EFAULT();
    	try {
    		BfsPath toPath = new BfsPath(newpath);
    		BfsPathType toPathType = toPath.getBfsPathProperties().getBfsPathType();
    		if ("BLOB".equals(toPathType.toString())){
    			return -ErrorCodes.EEXIST();
    		}
    		/* create the path */
    		BlobReqParams createParams = new BlobReqParams();
    		createParams.setContainer(toPath.getContainer());
    		createParams.setBlob(toPath.getBlob());
    		createParams.setBfsBlobType(BfsBlobType.BLOCKBLOB);
    		createParams.setContent(oldpath.trim());
    		if (BlobService.createBlob(createParams)){
    			BlobService.setBlobMetadata(createParams, Constants.BLOB_META_DATA_ISlINK_KEY, "1");
    			setBlobUidandGid(createParams, bfsUid, bfsGid);
    			result = 0;
 			} else {
    			return -ErrorCodes.EIO();
    		}     		 		 
		} catch (BlobfsException ex) {
			logger.error(ex.getMessage());
			//ex.printStackTrace();
		}    	
        return result;
	}

	public int bfsReadlink(String path, Pointer buf, long size) {
		logger.trace("read symbolic link from {}", path);
    	int result = -ErrorCodes.EFAULT();
    	try {
    		if ("".equals(path) || buf.size() == 0 || size <= 0){
    			return 0;
    		}
    		BfsPath srcPath = new BfsPath(path);
    		
    		/* create the path */
    		BlobReqParams insParams = new BlobReqParams();
    		insParams.setContainer(srcPath.getContainer());
    		insParams.setBlob(srcPath.getBlob());
    		insParams.setBfsBlobType(BfsBlobType.BLOCKBLOB);
    		BlobBufferedIns bbIns = new BlobBufferedIns(insParams);
    		long bytesToRead = bbIns.getBlobSize();
    		if(size <= bytesToRead){
    			bytesToRead = size - 1;
    		}
    		byte[] bytesReaded = new byte[(int) bytesToRead + 1];
    		if (bbIns.read(bytesReaded, 0, (int)bytesToRead) != -1){
    			bytesReaded[(int) bytesToRead] = 0;
    			buf.put(0, bytesReaded, 0, (int) bytesToRead + 1);
			}
    		bbIns.close();
    		result = 0;
    		
		} catch (IOException ex) {
			logger.error(ex.getMessage());
			//ex.printStackTrace();
		}    	
        return result;
	}

	public int bfsChmod(String path, long mode) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public int bfsChown(String path, long uid, long gid) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	
	public int setBlobUidandGid(BlobReqParams reqParams, int uid, int gid){
		try {
			BlobService.setBlobMetadata(reqParams, Constants.BLOB_META_DATE_UID_KEY, Integer.toString(uid));
			BlobService.setBlobMetadata(reqParams, Constants.BLOB_META_DATE_GID_KEY, Integer.toString(gid));
		} catch (BlobfsException ex) {
			logger.error(ex.getMessage());
//			ex.printStackTrace();
		}
		return 0;
	}
	
	public int setContainerUidandGid(String containerName, int uid, int gid){
		try {
			ContainerService.setContainerMetadata(null, containerName, Constants.BLOB_META_DATE_UID_KEY, Integer.toString(uid));
			ContainerService.setContainerMetadata(null, containerName, Constants.BLOB_META_DATE_GID_KEY, Integer.toString(gid));
		} catch (Exception ex) {
			logger.error(ex.getMessage());
//			ex.printStackTrace();
		}
		return 0;
	}

	public int bfsFsync(String path, int isdatasync, FuseFileInfo fi) {
		logger.trace("fsync from {}", path);
		final long fd = fi.fh.get();
    	OpenedFileModel ofm;
    	synchronized (openedFilesManager) {
    		ofm = (OpenedFileModel) openedFilesManager.get(fd);
    	}
	    if (ofm == null) {
	    	logger.error("Cannot find fd for {} in table", path);
	    	return -ErrorCodes.EBADFD();
	    }
    	try {
			ofm.close();
		} catch (Exception e) {
			logger.error("Failed closing {}", path, e);
		}
    	
    	updateCacheAndNotifyPeers(path, BfsPathType.BLOB);
    	
        return 0;
	}
	
	public void updateCacheAndNotifyPeers(String path, BfsPathType bfsPathType){
    	try {
    		if ("ROOT".equals(bfsPathType.toString())){
    			bfsFilesCache.clear();
    		} else if  ("CONTAINER".equals(bfsPathType.toString()) || "SUBDIR".equals(bfsPathType.toString())){
    			for (Entry<String, CachedObject> entry : bfsFilesCache.cacheStore.entrySet()) {
    				if (entry.getKey().startsWith(path)) {
    					bfsFilesCache.delete(entry.getKey());
    				}
    			}
    		} else if ("BLOB".equals(bfsPathType.toString()) || "LINK".equals(bfsPathType.toString())){
    			if (bfsFilesCache.has(path)) {
        			bfsFilesCache.delete(path);
            	}
    		}    		
        	/* notify other peers */
    		if (clusterEnabled){
    			MessageService.sbSendMessages(path);
    		}
		} catch (Exception e) {
			logger.error("Failed sending  {}", path, e);
		}
	}
	
}
