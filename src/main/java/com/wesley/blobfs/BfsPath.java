package com.wesley.blobfs;


import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wesley.blobfs.utils.BfsUtility;

import ru.serce.jnrfuse.struct.FileStat;

@SuppressWarnings("static-access")
public class BfsPath{
	private static Logger logger = LoggerFactory.getLogger("BfsPath.class");
	String bfsPrefix = Constants.DEFAULT_BLOB_PREFIX.trim();
	String path;
	String bfsFullPath;
	String container;
	String virtualDirectoy;
	String blob;
	
	public BfsPath(String path) {
		bfsFullPath = getBfsFullPath(path);	
	}
	
	public final String getBfsFullPath(String path){
		
		if (bfsPrefix.length() >= 1 && !bfsPrefix.endsWith("/")){
			bfsPrefix = bfsPrefix + "/";
		}
		bfsFullPath = (bfsPrefix + path).replaceAll("/+", "/");
		return bfsFullPath;		
	}
	
	/* get the container name for the string :/container/folder/blob.txt */
	public String getContainer () {
		/* avoid the leading empty string */
		// bfsFullPath.replaceFirst("^/", "").split("/");
		String containerName = bfsFullPath.substring(1).split("/")[0];
		return containerName;
	}
	/* get the blob name for the string :/container/folder/blob.txt */
	public String getBlob () {
		String blobName = "";
		if (bfsFullPath.contains("/") && bfsFullPath.indexOf("/") != bfsFullPath.lastIndexOf("/")) {
			/* plus 2, due to substring, and do not need "/" */
			int beginIndex = bfsFullPath.substring(1).indexOf("/") + 2;
			blobName = bfsFullPath.substring(beginIndex);
		} 
		return blobName;
	}
	
	/**
	 * get the file properties
	 * @return
	 */
	public PathProperties getBfsPathProperties(){
		PathProperties pathProperties = new PathProperties();
		pathProperties.setBfsPathType(BfsPathType.INVALID);
		try {
			if (Objects.equals(bfsFullPath, "/")) {
				/*  root directory : /:*/
				pathProperties.setBfsPathType(BfsPathType.ROOT);
			} else if (!getContainer().equals("")  && getBlob().equals("")) {
				/* container: /container */
				if (ContainerService.containerExists(getContainer())) {
					/* TODO: put into cache here */
					ContainerProperties containerProperties = ContainerService.getContainerProperties(getContainer());
					pathProperties.setName(containerProperties.getName());
					pathProperties.setBfsPathType(BfsPathType.CONTAINER);
					pathProperties.setCtime(containerProperties.getCreated());
					pathProperties.setMtime(containerProperties.getLastModified());
				} 
			} else if (!getContainer().equals("")  && !getBlob().equals("")) {
				BlobReqParams checkReq = new BlobReqParams();
				checkReq.setContainer(getContainer());
				checkReq.setBlob(getBlob());
				if (BlobService.blobExists(checkReq)){
					/* blob: /container/folder/file1 */
					/* TODO: put into cache here */
					if (null != BlobService.getBlobMetadata(checkReq, Constants.BLOB_META_DATA_ISlINK_KEY)){
						pathProperties.setBfsPathType(BfsPathType.LINK);
					} else {
						pathProperties.setBfsPathType(BfsPathType.BLOB);
					}
					BlobProperties blobPorperties = BlobService.getBlobProperties(checkReq);
					pathProperties.setName(blobPorperties.getName());
					pathProperties.setBfsBlobType(blobPorperties.getBfsBlobType());
					pathProperties.setCtime(blobPorperties.getCreated());
					pathProperties.setMtime(blobPorperties.getLastModified());
					pathProperties.setSize(blobPorperties.getActualLength());
					
				} else if (BlobService.virtualDirectoryExists(checkReq)){
					/* virtual directory : /container/folder/ */
					/* TODO: put into cache here */
					pathProperties.setBfsPathType(BfsPathType.SUBDIR);
					String blobName = getBlob() + "/" + Constants.VIRTUAL_DIRECTORY_NODE_NAME;
					BlobReqParams vdParams = new BlobReqParams();
					vdParams.setContainer(getContainer());
					vdParams.setBlob(blobName);
					vdParams.setBfsBlobType(BfsBlobType.BLOCKBLOB);
					if (!BlobService.blobExists(vdParams)){
						BlobService.createBlob(vdParams);
						/* set the uid and gid */
//						BlobService.setBlobMetadata(vdParams, Constants.BLOB_META_DATE_UID_KEY, Integer.toString(uid));
//						BlobService.setBlobMetadata(vdParams, Constants.BLOB_META_DATE_GID_KEY, Integer.toString(gid));
					}
					BlobProperties blobPorperties = BlobService.getBlobProperties(vdParams);
					pathProperties.setName(blobPorperties.getName());					
					pathProperties.setCtime(blobPorperties.getCreated());
					pathProperties.setMtime(blobPorperties.getLastModified());					
				} 
			}
		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}
		return pathProperties;
	}
	public final boolean fillFileStat(PathProperties pathProperties, FileStat stat){
		BfsPathType bfsPathType = pathProperties.getBfsPathType();
		if ("ROOT".equals(bfsPathType.toString()) || Objects.equals(path, ".")) {
			/* is the root directory */
			stat.st_mode.set(FileStat.S_IFDIR | 0755);
			stat.st_nlink.set(2);
//			stat.st_uid.set(bfsUid);
//			stat.st_gid.set(bfsGid);
		} else if ("CONTAINER".equals(bfsPathType.toString())){
			/* is the container */
			stat.st_mode.set(FileStat.S_IFDIR | 0755);
			stat.st_nlink.set(2);
			stat.st_ctim.tv_sec.set(pathProperties.getCtime().getTime() / 1000);
			stat.st_ctim.tv_nsec.set((pathProperties.getCtime().getTime() % 1000) / 1000);
			stat.st_mtim.tv_sec.set(pathProperties.getMtime().getTime() / 1000);
		    stat.st_mtim.tv_nsec.set((pathProperties.getMtime().getTime() % 1000) / 1000);				
			stat.st_atim.tv_sec.set(pathProperties.getMtime().getTime() / 1000);	
		} else if ("SUBDIR".equals(bfsPathType.toString())){
			/* is the virtual directory */
			stat.st_mode.set(FileStat.S_IFDIR | 0755);
			stat.st_nlink.set(2);
			stat.st_ctim.tv_sec.set(pathProperties.getCtime().getTime() / 1000);
			stat.st_ctim.tv_nsec.set((pathProperties.getCtime().getTime() % 1000) / 1000);
			stat.st_mtim.tv_sec.set(pathProperties.getMtime().getTime() / 1000);
		    stat.st_mtim.tv_nsec.set((pathProperties.getMtime().getTime() % 1000) / 1000);				
			stat.st_atim.tv_sec.set(pathProperties.getMtime().getTime() / 1000);
		} else if ("LINK".equals(bfsPathType.toString())){
			/* is the virtual directory */				
			stat.st_mode.set(FileStat.S_IFLNK | 0755);
			stat.st_nlink.set(2);
			stat.st_ctim.tv_sec.set(pathProperties.getCtime().getTime() / 1000);
			stat.st_ctim.tv_nsec.set((pathProperties.getCtime().getTime() % 1000) / 1000);
			stat.st_mtim.tv_sec.set(pathProperties.getMtime().getTime() / 1000);
		    stat.st_mtim.tv_nsec.set((pathProperties.getMtime().getTime() % 1000) / 1000);				
			stat.st_atim.tv_sec.set(pathProperties.getMtime().getTime() / 1000);
		} else if ("BLOB".equals(bfsPathType.toString())){
			/* is the blob */
			if ("APPENDBLOB".equals(pathProperties.getBfsBlobType().toString())){
				stat.st_mode.set(FileStat.S_IFREG | 0666);
			} else {
				stat.st_mode.set(FileStat.S_IFREG | 0444);
			}
			stat.st_nlink.set(1);
			stat.st_size.set(pathProperties.getSize());
			stat.st_ctim.tv_sec.set(pathProperties.getCtime().getTime() / 1000);
			stat.st_ctim.tv_nsec.set((pathProperties.getCtime().getTime() % 1000) / 1000);
			stat.st_mtim.tv_sec.set(pathProperties.getMtime().getTime() / 1000);
		    stat.st_mtim.tv_nsec.set((pathProperties.getMtime().getTime() % 1000) / 1000);				
			stat.st_atim.tv_sec.set(pathProperties.getMtime().getTime() / 1000);
		} else {
			return false;
		}
//		stat.st_uid.set(pathProperties.getUid());
//		stat.st_gid.set(pathProperties.getGid());
		return true;
	}
	public String getParent(){
		/* over write the Paht.getParent */
		if (Objects.equals(bfsFullPath, "/")) {
			return null;
		}		
		boolean endsWithSlash = bfsFullPath.endsWith("/");
		int startIndex = endsWithSlash ? bfsFullPath.length() - 2 : bfsFullPath.length() - 1;
		String parentDir = bfsFullPath.substring(0, bfsFullPath.lastIndexOf("/",startIndex));
		return parentDir;
	}
	
	public String getCurrentPath(){
		String file = bfsFullPath.substring(getParent().length() + 1, bfsFullPath.length());
		/* remove last slash */
		file  = BfsUtility.removeLastSlash(file);
		return file;
		
	}
	
}
