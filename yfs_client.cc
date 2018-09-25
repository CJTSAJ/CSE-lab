// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

yfs_client::yfs_client()
{
    ec = new extent_client();

}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    }
    printf("isfile: %lld is a dir\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 *
 * */

bool
yfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
/*
 * your code goes here.
 * note: get the content of inode ino, and modify its content
 * according to the size (<, =, or >) content length.
 */
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;
    if(ino < 1 || ino > INODE_NUM)
      return IOERR;

    int old_size;
    std::string buf;
    EXT_RPC(ec->get(ino, buf));

    old_size = buf.length();

    if(size == old_size) return r;

    buf.resize(size, '\0');

    EXT_RPC(ec->put(ino, buf));
release:
    return r;
}

/*
 * your code goes here.
 * note: lookup is what you need to check if file exist;
 * after create file or dir,
 * you must remember to modify the parent infomation.
 */
int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    if(!isdir(parent))
      return IOERR;

    std::string temp_name = name;
    std::ostringstream ost;
    std::string buf;
    /*create file*/
    EXT_RPC(ec->create(extent_protocol::T_FILE, ino_out));

    /*write it to parent*/

    ec->get(parent, buf);
    ost.put((unsigned char)temp_name.length());
    ost.write(name, temp_name.length());
    ost.write((char*)&ino_out, sizeof(inum));

    buf.append(ost.str());
    EXT_RPC(ec->put(parent, buf));
release:
    return r;
}

/*
 * your code goes here.
 * note: lookup is what you need to check if directory exist;
 * after create file or dir, you must remember to modify the parent infomation.
 */
int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;



    return r;
}

/*
 * your code goes here.
 * note: lookup file from parent dir according to name;
 * you should design the format of directory content.
 */
int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    if(parent < 1 || parent > INODE_NUM)
      return IOERR;

    std::string filename = std::string(name);
    std::list<dirent> entryList;
    EXT_RPC(readdir(parent, entryList));

    for(std::list<dirent>::iterator it = entryList.begin(); it != entryList.end(); it++){
      if(filename == it->name){
        found = true;
        ino_out = it->inum;
        break;
      }
    }

release:
    return r;
}

/*
 * your code goes here.
 * note: you should parse the dirctory content using your defined format,
 * and push the dirents to the list.
 */
int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    if(dir < 1 && dir > INODE_NUM)
      return IOERR;
    list.clear();

    std::istringstream ist;
    int filename_len;
    char tempC;
    char filename[MAXFILELEN];
    dirent entry;
    inum file_inum;

    /*read the content of directory*/
    std::string dir_content;
    EXT_RPC(ec->get(dir, dir_content));
    ist.str(dir_content);
    //the format of directory content:
    //filenameLen+filename+inum
    while(ist.get(tempC)){
      filename_len = (int)(unsigned char)tempC;

      //read file name
      ist.read(filename, filename_len);
      entry.name = std::string(filename, filename_len);

      //read inum
      ist.read((char*)&file_inum, sizeof(inum));
      entry.inum = file_inum;

      list.push_back(entry);
    }
release:
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */

    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    return r;
}
