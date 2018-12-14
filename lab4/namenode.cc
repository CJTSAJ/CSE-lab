#include "namenode.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sys/stat.h>
#include <unistd.h>
#include "threader.h"

using namespace std;

void NameNode::init(const string &extent_dst, const string &lock_dst) {
  ec = new extent_client(extent_dst);
  lc = new lock_client_cache(lock_dst);
  yfs = new yfs_client(extent_dst, lock_dst);
  heart_beats = 0;
  /* Add your init logic here */
  NewThread(this, &NameNode::Beat);
}

//keep beating
void NameNode::Beat()
{
  while(true){
    heart_beats++;
    sleep(0);
  }
}

list<NameNode::LocatedBlock> NameNode::GetBlockLocations(yfs_client::inum ino) {
  printf("GetBlockLocations\n");fflush(stdout);
  list<blockid_t> blockids;
  ec->get_block_ids(ino, blockids);

  //convert to LocatedBlock
  list<NameNode::LocatedBlock> locs;
  uint64_t offset = 0;

  extent_protocol::attr attr;
  ec->getattr(ino, attr);
  uint64_t last_size = (attr.size % BLOCK_SIZE) ? (attr.size % BLOCK_SIZE) : BLOCK_SIZE;

  int len = blockids.size();
  int cnt = 0;
  for(auto i : blockids){
    cnt++;
    LocatedBlock tmpLB(i, offset, cnt < len ? BLOCK_SIZE : last_size, master_datanode);
    locs.push_back(tmpLB);
    offset += BLOCK_SIZE;
  }
  return locs;
}

bool NameNode::Complete(yfs_client::inum ino, uint32_t new_size) {
  printf("Complete\n");fflush(stdout);
  ec->complete(ino, new_size);
  lc->release(ino);
  return true;
}

NameNode::LocatedBlock NameNode::AppendBlock(yfs_client::inum ino) {
  printf("AppendBlock\n");fflush(stdout);
  blockid_t blockid;
  extent_protocol::attr attr;

  ec->getattr(ino, attr);
  ec->append_block(ino, blockid);
  record_blocks.insert(blockid);
  return LocatedBlock(blockid, attr.size,
    (attr.size % BLOCK_SIZE) ? attr.size % BLOCK_SIZE : BLOCK_SIZE, master_datanode);
}

bool NameNode::Rename(yfs_client::inum src_dir_ino, string src_name, yfs_client::inum dst_dir_ino, string dst_name) {
  printf("Rename\n");fflush(stdout);
  //check whether the dir and file exist
  yfs_client::inum ino;
  if(yfs->delEntry(src_dir_ino, ino, src_name) != yfs_client::OK)
    return false;

  if(yfs->addEntry(dst_dir_ino, ino, dst_name) != yfs_client::OK)
    return false;
  return true;
}

bool NameNode::Mkdir(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
  printf("Mkdir\n");fflush(stdout);
  return (yfs->mkdir(parent, name.c_str(), mode, ino_out) == yfs_client::OK);
}

bool NameNode::Create(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
  printf("Create\n");fflush(stdout);
  int ret = yfs->create(parent, name.c_str(), mode, ino_out);
  if(ret == yfs_client::OK){
    lc->acquire(ino_out);
    return true;
  }
  else
    return false;
}

bool NameNode::Isfile(yfs_client::inum ino) {
  printf("Isfile\n");fflush(stdout);
  extent_protocol::attr a;
  if(ec->getattr(ino, a) != extent_protocol::OK)
    return false;
  return a.type == extent_protocol::T_FILE;
}

bool NameNode::Isdir(yfs_client::inum ino) {
  printf("Isdir\n");fflush(stdout);
  extent_protocol::attr a;
  if(ec->getattr(ino, a) != extent_protocol::OK)
    return false;
  return a.type == extent_protocol::T_DIR;
}

bool NameNode::Getfile(yfs_client::inum ino, yfs_client::fileinfo &info) {
  printf("Getfile\n");fflush(stdout);
  extent_protocol::attr a;
  if (ec->getattr(ino, a) != extent_protocol::OK)
    return false;

  info.atime = a.atime;
  info.mtime = a.mtime;
  info.ctime = a.ctime;
  info.size = a.size;
  return true;
}

bool NameNode::Getdir(yfs_client::inum ino, yfs_client::dirinfo &info) {
  printf("Getdir\n");fflush(stdout);
  extent_protocol::attr a;
  if (ec->getattr(ino, a) != extent_protocol::OK)
    return false;

  info.atime = a.atime;
  info.mtime = a.mtime;
  info.ctime = a.ctime;
  return true;
}

bool NameNode::Readdir(yfs_client::inum ino, std::list<yfs_client::dirent> &dir) {
  printf("Readdir\n");fflush(stdout);
  lc->release(ino);
  int ret = yfs->readdir(ino, dir);
  lc->acquire(ino);

  if(ret != yfs_client::OK)
    return false;
  return true;
}

bool NameNode::Unlink(yfs_client::inum parent, string name, yfs_client::inum ino) {
  printf("Unlink\n");fflush(stdout);
  /*//lc->release(parent);
  //int ret = yfs->unlink(parent, name.c_str());
  //lc->acquire(parent);

  if(ret != yfs_client::OK)
    return false;*/
  yfs_client::inum tmp_ino;
  if(yfs->delEntry(parent, tmp_ino, name) != yfs_client::OK)
    return false;

  if(ec->remove(ino) != extent_protocol::OK)
    return false;
  return true;
}

void NameNode::DatanodeHeartbeat(DatanodeIDProto id) {
  printf("DatanodeHeartbeat\n");fflush(stdout);
  datanodes[id] = heart_beats;
}

void NameNode::RegisterDatanode(DatanodeIDProto id) {
  printf("RegisterDatanode\n");fflush(stdout);
  for(auto i : record_blocks){
    ReplicateBlock(i, master_datanode, id);
  }
  datanodes.insert(make_pair(id, this->heart_beats));
}

list<DatanodeIDProto> NameNode::GetDatanodes() {
  printf("GetDatanodes\n");fflush(stdout);
  list<DatanodeIDProto> result;
  for(auto i : datanodes){
    if(i.second >= this->heart_beats - 3)
      result.push_back(i.first);
  }
  return result;
}
