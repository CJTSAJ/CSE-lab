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
int
yfs_client::symlink(inum parent, const char* name, const char* path, inum& ino_out)
{
  int r = OK;
  if(!isdir(parent) || !name || !path)
    return IOERR;
  std::string buf;
  std::string temp_name = std::string(name);
  std::string path_s = std::string(path);
  std::ostringstream ost;

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
    if(!isdir(parent))
      return IOERR;

    std::string temp_name = std::string(name);
    std::ostringstream ost;
    std::string buf;

    bool found = false;
    inum temp_ino;
    lookup(parent, name, found, temp_ino);
    if(found)
      EXIST;
    /*create file*/
    EXT_RPC(ec->create(extent_protocol::T_DIR, ino_out));

    /*write it to parent*/
    EXT_RPC(ec->get(parent, buf));

    ost.put((unsigned char)(temp_name.length()));
    ost.write(name, temp_name.length());
    ost.write((char*)&ino_out, sizeof(inum));
    buf = buf + ost.str();

    EXT_RPC(ec->put(parent, buf));
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
    printf("look up:%d %s\n", parent, name);
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
      std::cout<< "entry:  " << it->name << '\n';
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
    std::cout << "dir_content:" << dir_content << '\n';
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
    printf("yfs:read %d %d %d\n", ino, size, off);
    int r = OK;

    if(ino < 1 || ino > INODE_NUM || off < 0)
      return IOERR;

    std::string fileContent;

    EXT_RPC(ec->get(ino, fileContent));
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
    std::cout<<"yfs:write off "<<off<<"size "<<size<<"data "<<data<<'\n';
    int r = OK;
    bytes_written = 0;
    if(ino < 1 || ino > INODE_NUM || off < 0 || !data)
      return IOERR;

    std::string old_content;

    int new_size;
    EXT_RPC(ec->get(ino, old_content));
    //printf("yfs: write old_content len: %d\n", old_content.length());
    if((off + size) > old_content.length()){
      old_content.resize(off + size, '\0');
    }

    old_content.replace(off, size, std::string(data, size));

    EXT_RPC(ec->put(ino, old_content));
    bytes_written = size;
release:
    return r;
}
/*

char *temp = content_buf;
std::string new_content;
for(int i = 0; i < size + off; i++){ // fill the space with \0
  *temp = '\0';
  temp++;
}
if(old_content.length() >= (off + size)){
  new_size = old_content.length();
}else
char *content_buf;
char *temp;*/
/*content_buf = (char *)malloc(new_size);
temp = content_buf;
for(int i = 0; i < new_size; i++){// fill the space with \0
  *temp = '\0';
  temp++;
}
memcpy(content_buf, old_content.c_str(), old_content.length());
memcpy(content_buf + off, data, size);
new_content = std::string(content_buf, new_size);*/
//new_content.resize(off + size);
/*printf("off:%d  size: %d new_content size: %d  data : %s  content_buf:%s \n", off, size, new_content.length(), data, content_buf);
std::cout << "old_content:" << old_content;
std::cout << "new_content:" << new_content;*/

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

    EXT_RPC(readdir(parent, all_files));

    for(it = all_files.begin(); it != all_files.end(); it++){
      if(file_name == it->name)
        break;
    }

    if(it == all_files.end())
      return NOENT;

    if (!isfile(it->inum)) 
      return IOERR;

    EXT_RPC(ec->remove(it->inum));
    all_files.erase(it);


    for(it = all_files.begin(); it != all_files.end(); it++){
      ost.put((unsigned char)(it->name.length()));
      ost.write(it->name.c_str(), it->name.length());
      ost.write((char*)&(it->inum), sizeof(inum));
    }

    EXT_RPC(ec->put(parent, ost.str()));
release:
    return r;
}
