package com.wesley.blobfs;


import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wesley.blobfs.utils.BfsUtility;

import jnr.ffi.Pointer;
import jnr.ffi.types.gid_t;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import jnr.ffi.types.uid_t;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.Timespec;

public class BfsFuseServiceImpl extends FuseStubFS implements BfsFuseService{
	private static Logger logger = LoggerFactory.getLogger("BfsFuseServiceImpl.class");
	public static int bfsUid = Constants.DEFAULT_BFS_UID;
	public static int bfsGid = Constants.DEFAULT_BFS_UID;
    public BfsFuseServiceImpl() {
    	getCallerUid();
    	getCallerGid();
    }
    public final void getCallerUid() {
    	if (bfsGid == -1){
			try {
				bfsGid = BfsUtility.getIdOnUnix(Constants.GET_GID_ON_UNIX_CMD);
			} catch (IOException ex) {
				logger.error(ex.getMessage());
			}
    	}
	}

    public final void getCallerGid() {
    	if (bfsUid == -1){
			try {
				bfsUid = BfsUtility.getIdOnUnix(Constants.GET_GID_ON_UNIX_CMD);
			} catch (IOException ex) {
				logger.error(ex.getMessage());
			}
    	}
	}
    
    @Override
    public String getFSName() {
      return "Blobfs";
    }
    
    @Override
    public int getattr(String path, FileStat stat) {
        return BfsService.getInstance().bfsGetFileAttributes(path, stat);
    }

    @Override
    public int readdir(String path, Pointer buf, FuseFillDir filter, @off_t long offset, FuseFileInfo fi) {
         return BfsService.getInstance().bfsReaddir(path, buf, filter, offset, fi);
    }

    @Override
    public int open(String path, FuseFileInfo fi) {
        return BfsService.getInstance().bfsOpen(path, fi);
    }
    
    @Override
    public int release(String path, FuseFileInfo fi) {
        return BfsService.getInstance().bfsRelease(path, fi);
    }

    @Override
    public int read(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    	return BfsService.getInstance().BfsRead(path, buf, size, offset, fi);
    }
    
    @Override
    public int flush(String path, FuseFileInfo fi) {
        return BfsService.getInstance().bfsFlush(path, fi);
    }
    
    @Override
    public int create(String path, @mode_t long mode, FuseFileInfo fi) {
    	return BfsService.getInstance().bfsCreate(path, mode, fi);
    }
    
    @Override
    public int setxattr(String path, String name, Pointer value, @size_t long size, int flags) {
    	return BfsService.getInstance().bfsSetxattr(path, name, value, size, flags);
    }  
    
    @Override
    public int getxattr(String path, String name, Pointer value, @size_t long size){
    	return BfsService.getInstance().bfsGetxattr(path, name, value, size);
    }    
    @Override
    public int removexattr(String path, String name){
    	return BfsService.getInstance().bfsRemovexattr(path, name);
    }
    @Override
    public int utimens(String path, Timespec[] timespec) {
    	return BfsService.getInstance().bfsUtimens(path, timespec);
    }
    
    @Override
    public int mkdir(String path, @mode_t long mode) {
        return BfsService.getInstance().bfsMkdir(path, mode);
    }
    
    @Override
    public int rename(String path, String newName) {
    	return BfsService.getInstance().bfsRename(path, newName);
    }
    
    @Override
    public int access(String path, int mask) {
        return 0;
    }
    
    @Override
    public int rmdir(String path) {
    	return BfsService.getInstance().bfsRmdir(path);
    }
    
    @Override
    public int unlink(String path) {
        return BfsService.getInstance().bfsUnlink(path);
    }
    
    @Override
    public int truncate(String path, long offset) {
    	return BfsService.getInstance().bfsTruncate(path, offset);
    }

    @Override
    public int write(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
    	return BfsService.getInstance().bfsWrite(path, buf, size, offset, fi);
    }
    
    @Override
    public int link(String oldpath, String newpath){
    	return BfsService.getInstance().bfsLink(oldpath, newpath);
    }
    
    @Override
    public int symlink(String oldpath, String newpath){
    	return BfsService.getInstance().bfsSymlink(oldpath, newpath);
    }
    
    @Override
    public int readlink(String path, Pointer buf, @size_t long size){
    	return BfsService.getInstance().bfsReadlink(path, buf, size);
    }
    
    @Override
    public int chmod(String path, @mode_t long mode){
    	return BfsService.getInstance().bfsChmod(path, mode);
    }
    
    @Override
    public int chown(String path, @uid_t long uid, @gid_t long gid){
    	return BfsService.getInstance().bfsChown(path, uid, gid);
    }
    
    @Override
    public int fsync(String path, int isdatasync, FuseFileInfo fi) {
    	return BfsService.getInstance().bfsFsync(path, isdatasync, fi);
    }
    
}
