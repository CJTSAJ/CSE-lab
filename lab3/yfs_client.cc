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

#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  lc = new lock_client(lock_dst);
  lc->acquire(1);
  if (ec->put(1, "") != extent_protocol::OK)
      printf("error init root dir\n"); // XYB: init root dir
  lc->release(1);
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
yfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_LINK) {
        printf("issymlink: %lld is a symlink\n", inum);
        return true;
    }
    return false;
}
int
yfs_client::symlink(inum parent, const char* name, const char* path, inum& ino_out)
{
  lc->acquire(parent);
  int r = OK;
  if(!isdir(parent) || !name || !path){
    lc->release(parent);
    return IOERR;
  }

  std::string buf;
  std::string temp_name = std::string(name);
  std::string path_s = std::string(path);
  std::ostringstream ost;

  bool found = false;
  inum temp_ino;
  lookup_unlock(parent, name, found, temp_ino);

  if(found){
    lc->release(parent);
    return EXIST;
  }


  /*write the path to */
  EXT_RPC(ec->create(extent_protocol::T_LINK, ino_out));
  EXT_RPC(ec->put(ino_out, path_s));

  /*write it to parent*/
  EXT_RPC(ec->get(parent, buf));

  ost.put((unsigned char)(temp_name.length()));
  ost.write(name, temp_name.length());
  ost.write((char*)&ino_out, sizeof(inum));
  buf = buf + ost.str();

  EXT_RPC(ec->put(parent, buf));
  lc->release(parent);
release:
  return r;
}
int
yfs_client::readlink(inum ino, std::string&link)
{
  int r = OK;
  if(ino < 1 || ino > INODE_NUM)
    return IOERR;

  EXT_RPC(ec->get(ino, link));
release:
  return r;
}

bool
yfs_client::isdir(inum inum)
{
    extent_protocol::attr a;
    if(ec->getattr(inum, a) != extent_protocol::OK)
      return IOERR;

    if(a.type ==  extent_protocol::T_DIR)
      return true;
    else return false;

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

    lc->acquire(ino);

    int old_size;
    std::string buf;
    EXT_RPC(ec->get(ino, buf));

    old_size = buf.length();

    if(size == old_size) {
      lc->release(ino);
      return r;
    }

    buf.resize(size, '\0');

    EXT_RPC(ec->put(ino, buf));

    lc->release(ino);
release:
    return r;
}

/*
 * your code goes here
 * note: lookup is what you need to check if file exist;
 * after create file or dir,
 * you must remember to modify the parent infomation.
 */
int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    lc->acquire(parent);
    int r = OK;
    if(!isdir(parent) || !name){
      lc->release(parent);
      return IOERR;
    }


    bool found = false;
    inum temp_ino;
    lookup_unlock(parent, name, found, temp_ino);

    if(found){
      lc->release(parent);
      return EXIST;
    }

    std::string temp_name = std::string(name);
    std::ostringstream ost;
    std::string buf;
    /*create file*/
    EXT_RPC(ec->create(extent_protocol::T_FILE, ino_out));


    /*write it to parent*/

    EXT_RPC(ec->get(parent, buf));
    //std::cout << "old_buf" << buf << "len:" << buf.length();
    //printf("yfs_client create: parent %d name %s ino_out %d\n", parent, name, ino_out);
    ost.put((unsigned char)(temp_name.length()));
    ost.write(name, temp_name.length());
    ost.write((char*)&ino_out, sizeof(inum));
    buf = buf + ost.str();
    //std::cout << "new_buf" << buf << "len:" << buf.length();
    //printf("yfs_client create: ost.str():%s len %d \n", ost.str().c_str(), temp_name.length());
    EXT_RPC(ec->put(parent, buf));

    lc->release(parent);
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
    lc->acquire(parent);
    int r = OK;
    if(!isdir(parent))
      return IOERR;

    std::string temp_name = std::string(name);
    std::ostringstream ost;
    std::string buf;

    bool found = false;
    inum temp_ino;
    lookup_unlock(parent, name, found, temp_ino);

    if(found){
      lc->release(parent);
      return EXIST;
    }

    /*create file*/
    EXT_RPC(ec->create(extent_protocol::T_DIR, ino_out));

    /*write it to parent*/
    EXT_RPC(ec->get(parent, buf));

    ost.put((unsigned char)(temp_name.length()));
    ost.write(name, temp_name.length());
    ost.write((char*)&ino_out, sizeof(inum));
    buf = buf + ost.str();

    EXT_RPC(ec->put(parent, buf));

    lc->release(parent);
release:
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
    //printf("look up:%d %s\n", parent, name);
    int r = OK;
    found = false;
    if(parent < 1 || parent > INODE_NUM || !name)
      return IOERR;

    extent_protocol::attr a;
    if(ec->getattr(parent, a) != extent_protocol::OK)
      return IOERR;
    if(a.type != extent_protocol::T_DIR)
      return IOERR;

    std::string filename = std::string(name);
    std::list<dirent> entryList;
    EXT_RPC(readdir(parent, entryList));

    for(std::list<dirent>::iterator it = entryList.begin(); it != entryList.end(); it++){
      //std::cout<< "entry:  " << it->name << '\n';
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
    lc->acquire(dir);
    EXT_RPC(ec->get(dir, dir_content));
    lc->release(dir);
    //std::cout << "dir_content:" << dir_content << '\n';
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

/*
 * your code goes here.
 * note: read using ec->get().
 */
int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    //printf("yfs:read %d %d %d\n", ino, size, off);
    int r = OK;

    if(ino < 1 || ino > INODE_NUM || off < 0)
      return IOERR;

    std::string fileContent;
    lc->acquire(ino);
    EXT_RPC(ec->get(ino, fileContent));
    lc->release(ino);
    if(size > (fileContent.length() - off))
      size = fileContent.length() - off;
    data = fileContent.substr(off, size);
    data.resize(size);

release:
    return r;
}

/*
 * your code goes here.
 * note: write using ec->put().
 * when off > length of original file, fill the holes with '\0'.
 */
int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    //std::cout<<"yfs:write off "<<off<<"size "<<size<<"data "<<data<<'\n';
    int r = OK;
    bytes_written = 0;
    if(ino < 1 || ino > INODE_NUM || off < 0 || !data)
      return IOERR;

    std::string old_content;
    lc->acquire(ino);
    //int new_size;
    EXT_RPC(ec->get(ino, old_content));
    //printf("yfs: write old_content len: %d\n", old_content.length());
    if((off + size) > old_content.length()){
      old_content.resize(off + size, '\0');
    }

    old_content.replace(off, size, std::string(data, size));

    EXT_RPC(ec->put(ino, old_content));
    bytes_written = size;

    lc->release(ino);
release:
    return r;
}

/*
 * your code goes here.
 * note: you should remove the file using ec->remove,
 * and update the parent directory content.
 */
int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    if (!isdir(parent) || !name)
        return IOERR;

    std::list<dirent> all_files;
    std::list<dirent>::iterator it;
    std::string file_name = std::string(name);
    std::ostringstream ost;
    inum ino_;

    EXT_RPC(readdir(parent, all_files));
    lc->acquire(parent);
    for(it = all_files.begin(); it != all_files.end(); it++){
      if(file_name == it->name)
        break;
    }

    if(it == all_files.end()){
      lc->release(parent);
      return NOENT;
    }




    ino_ = it->inum;
    if (!isfile(ino_) && !issymlink(ino_)){
      lc->release(parent);
      return IOERR;
    }



    all_files.erase(it);

    for(it = all_files.begin(); it != all_files.end(); it++){
      ost.put((unsigned char)(it->name.length()));
      ost.write(it->name.c_str(), it->name.length());
      ost.write((char*)&(it->inum), sizeof(inum));
    }

    EXT_RPC(ec->put(parent, ost.str()));
    lc->release(parent);

    lc->acquire(ino_);
    EXT_RPC(ec->remove(ino_));
    lc->release(ino_);
release:
    return r;
}

int
yfs_client::readdir_unlock(inum dir, std::list<dirent> &list)
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

  //std::cout << "dir_content:" << dir_content << '\n';
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
yfs_client::lookup_unlock(inum parent, const char *name, bool &found, inum &ino_out)
{
    //printf("look up:%d %s\n", parent, name);
    int r = OK;
    found = false;
    if(parent < 1 || parent > INODE_NUM || !name)
      return IOERR;

    extent_protocol::attr a;
    if(ec->getattr(parent, a) != extent_protocol::OK)
      return IOERR;
    if(a.type != extent_protocol::T_DIR)
      return IOERR;

    std::string filename = std::string(name);
    std::list<dirent> entryList;
    EXT_RPC(readdir_unlock(parent, entryList));

    for(std::list<dirent>::iterator it = entryList.begin(); it != entryList.end(); it++){
      //std::cout<< "entry:  " << it->name << '\n';
      if(filename == it->name){
        found = true;
        ino_out = it->inum;
        break;
      }
    }

release:
    return r;
}
