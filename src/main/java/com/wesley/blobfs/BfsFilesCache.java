package com.wesley.blobfs;

import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.serce.jnrfuse.struct.FileStat;

public class BfsFilesCache extends BfsCacheBase {
	private static BfsFilesCache instance;
	private static ExecutorService cacheAutoCleanupES;
	private static boolean clusterEnabled = Constants.BFS_CLUSTER_ENABLED;
	
	private BfsFilesCache(){
		cacheStore = new ConcurrentHashMap<String, CachedObject>(Constants.BFS_FILES_CACHE_INIT_CAPACITY, 0.9f, 1);
		capacity = Constants.BFS_FILES_CACHE_MAX_CAPACITY;
		expireTime = Constants.BFS_FILES_CACHE_EXPIRE_TIME;
		/* start the lease auto cleanup service */
		if (clusterEnabled){
			startCacheAutoCleanupService();
		}
	}
	
	private final void startCacheAutoCleanupService(){
		/* Make it a daemon */
		cacheAutoCleanupES = Executors.newSingleThreadExecutor(new ThreadFactory(){
		    public Thread newThread(Runnable r) {
		        Thread t = new Thread(r);
		        t.setDaemon(true);
		        return t;
		    }        
		});
		cacheAutoCleanupES.submit(new cacheAutoCleaner());
	}
	
	public final boolean getFileStat(String path, FileStat stat){
		BfsPath bfsPath = new BfsPath(path);
		PathProperties pathProperties = (PathProperties) get(path);
		if (!bfsPath.fillFileStat(pathProperties, stat)){
			return false;
		}		
		return true;
		
	}
	
	@Override
	public void finalize() {
		cacheAutoCleanupES.shutdown();
	}
	
	public static BfsFilesCache getInstance(){
        if(instance == null){
            synchronized (BfsFilesCache.class) {
                if(instance == null){
                    instance = new BfsFilesCache();
                }
            }
        }
        return instance;
    }
	
	private class cacheAutoCleaner implements Runnable{
		private  Logger logger = LoggerFactory.getLogger("cacheAutoCleaner.class");
		@Override
		public void run() {
			try {				
				/* start the auto cleanup process */
				while (true){
					BfsFilesCache bfsFilesCache = BfsFilesCache.getInstance();
					/* Retrieve the msgs from the service bus topic */
					ArrayList<String> msgs = new ArrayList<>();
					msgs = MessageService.sbReceiveMessages();
					for (String msg: msgs){	
						logger.trace("delete {} from cache", msg);
						BfsPath msgPath = new BfsPath(msg);
			    		BfsPathType msgPathType = msgPath.getBfsPathProperties().getBfsPathType();
			    		if ("ROOT".equals(msgPathType.toString())){
			    			bfsFilesCache.clear();
			    		} else if ("CONTAINER".equals(msgPathType.toString()) || "SUBDIR".equals(msgPathType.toString())){
			    			for (Entry<String, CachedObject> entry : bfsFilesCache.cacheStore.entrySet()) {
			    				if (entry.getKey().startsWith(msg)) {
			    					bfsFilesCache.delete(entry.getKey());
			    				}
			    			}
			    		} else if ("BLOB".equals(msgPathType.toString()) || "LINK".equals(msgPathType.toString())){
			    			if (bfsFilesCache.has(msg)) {
			        			bfsFilesCache.delete(msg);
			            	}
			    		}   
					}					
					Thread.sleep(Constants.DEFAULT_BFC_THREAD_SLEEP_MILLS);
				}			
			} catch (Exception ex) {
				logger.error(ex.getMessage());
			}
		}
	
	}

}
