BlobFs
=====
![blobfs demo](doc/blobfs-demo.gif)
BlobFs is a distributed [FUSE](http://fuse.sourceforge.net) based file system backed by [Microsoft azure blob storage service](https://azure.microsoft.com/en-us/services/storage/blobs/). It allows you to mount the containers/blobs in the storage account as a the local folder/driver, no matter it is a Linux system or a Windows system. It support the cluster mode. you can mount the blob container (or part of it) across multiple linux and windows nodes.

## Important Notes:
* Here is the linux/mac version of the blobfs, please find the windows version of the blobfs from [blobfs-win](https://github.com/wesley1975/blobfs-win).
* For the core libraries of the blobfs, you can find it from the [bloblib](https://github.com/wesley1975/bloblib). which is responsible for handling all the underlying azure blob storage operations
* If you are interested in contributing, please contact me via jie1975.wu@gmail.com

## Project Goals
Object storage is one of the most fundamental topic you'll encounter when you decide to start your cloud journey.The main goal of the project is to make azure storage service easy to use for Linux and windows box.

## Key Updates:
base on the lots of feedbacks, in version 0.0.3, I made these major updates:
* Ported the blobfs to windows platform. It became a universal solution. 
* Improve the performance of list/rename/delete operation by enabling the multi-threading way. now it can list/rename/delete the thousands of items within few seconds.
* By requests of many users, I changed the queue from service bus to the Azure Queue storage,This will greatly simplify the configuration.
* Various bugs fixed, It is now more stable. you can use in production environment, but it's at your own risk.

## Features:
* Implemented these fuse functions: getattr, readdir, open, release, read, flush, create, mkdir, rename, rmdir, unlink, truncate, write, symlink, readlink.
* Allow mount multiple containers (or part of them) as the local folder.
* Cluster enabled: It supports mount the same containers/blobs across multiple nodes. Theses files can be shared via these nodes. The caches of these nodes are synchronized via azure queue storage.
* Use blob leases as the distributed locking mechanism across multiple nodes. The blob will be locked exclusively when it is written. 
* File’s attribute is cached for better performance, the cache are synchronized via azure queue storage.
* The contents are pre-cached by chunks when there is read operation. This will eliminate the times of http request and increase the performance greatly. 
* Multi-part uploads are used for the write operation. Data is buffered firstly and then be uploaded if the buffer size exceed the threshold. This also can eliminate the times of http request and increase the performance greatly. 
* You can edit the file content on the fly, especially recommend for the small file, It does not need download, edit and then upload.
* Append mode is supported, you can append the new line to the existing blob directly. this is more friendly for logging operation. And it can change the block blob to append blob automatically.
* Use server-side copy for move, rename operations, more efficient for big files and folders.
* Support the symbol link function

## Architecture and introduction

This is the logical architecture of blobfs:

![blobfs Logical Architecture](doc/blobfs-arch.jpg)
* Blobfs uses the blob leases to safe the write operation in the distributed environment, there is a dedicated thread to renew the lease automatically.
* For each of the node, there is local cache in it’s memory, the cache will store the file attributes. Once the file is changed by the node, the node will send a message to the Azure Queue storage. And then other nodes will receive the message and process it.

## installation
Installation now is very easy. But I strongly recommend to test and verify it in you environment before you use it. it's at your own risk.
### 1. Install fuse
    yum install fuse fuse-devel
### 2. Install blobfs
#### 2.1 get the azure account connection string, refer this [link](https://docs.microsoft.com/en-us/azure/storage/storage-create-storage-account)
#### 2.2 Edit configuration file: 
	Open blobfs.conf
	change the setting of :
    Storage_Connection_String = your-storage-account -connection-string
    blob_prefix = /  (e.g. /container1/folder1/)
    mount_point = /mnt/blobfs (make sure the path exists in you node)
    cluster_enabled = true
the cluster mode is enabled by default, You can also modify other settings if needed

### final. Start the blobfs service
    nohup java -jar blobfs-0.0.3jar
It is highly recommended that you should use [supervisord](http://supervisord.org/) to manage the blobfs services.

## Tips
* the block blob is read only by default. marked with read only flag. e.g. r--r--r--
* the append blob is marked with the read and write flag. e.g rw-rw-rw-


## How to create a append blob
* CLI way

	touch append.log	// this will create a empty block blob.
	
	echo 'new line here' >> append.log  
	
	// this will change the underlying block blob to append blob automatically. 
	
	// you also can issue this command against a existing file, this also works, but the time depends on the size of the file.
		
* Programming way

	FileWriter fw = new FileWriter("/mnt/blobfs/container1/append.log", true)  //java 1.7+
	
	applendFile = open("/mnt/blobfs/container1/append.log",' a+') //python
	
	...

## Performance Test
* simply do a dd testing (single thread mode).  so the performance depends on the machine and network.
![blobfs performance test](doc/blobfs-perf.gif)

## Dependency
* FUSE (Filesystem in Userspace) is an OS mechanism for unix-like OS that lets non-privileged users create their own file systems without editing kernel code.
* [Java Native Runtime (JNR)](https://github.com/jnr/jnr-ffi) is high-performance Java API for binding native libraries and native memory.
* [jnr-fuse](https://github.com/SerCeMan/jnr-fuse) is a great FUSE implementation in Java using Java Native Runtime (JNR).

## Limitation and known issues:
* Due to the overhead of fuse system, the performance will be expected slower than native file system. 
* For the cp command, the blobfs will use read out - then write in to new blob mode. this will spent more time for large files/folders.
* For the page blob, currently, should be, but it is not well tested yet. it may casue file interruption.  

## Supported platforms
* Linux : x86, x64
* MacOS (via osxfuse): x86, x64  (should be, but not tested yet)
* windows: [blobfs-win](https://github.com/wesley1975/blobfs-win)

## Command Line Usage
    blobfs  -h
    -b,--blob-prefix <arg>   The prefix of the blobs that will be used as the
                          mounted BlobFS root (e.g., /container1/blob1/;
                          defaults to /)
    -h,--help                Print this help
    -m,--mount-point <arg>   Desired local mount point for BlobFs.
    -o <arg>                 FUSE mount options

	for more fuse mount options, you can find [here](http://manpages.ubuntu.com/manpages/xenial/man8/mount.fuse.8.html).

## License
	Copyright (C) 2017 Wesley Wu jie1975.wu@gmail.com
	This code is licensed under The General Public License version 3
	
## FeedBack
	Your feedbacks are highly appreciated! :)
