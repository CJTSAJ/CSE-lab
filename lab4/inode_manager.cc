#include "inode_manager.h"
#include <string>
// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || !buf)
        return;

  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || !buf)
        return;

  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------
// Allocate a free disk block.
/*
 * your code goes here.
 * note: you should mark the corresponding bit in block bitmap when alloc.
 * you need to think about which block you can start to be allocated.
 */
blockid_t
block_manager::alloc_block()
{
  char buf[BLOCK_SIZE]; //all bits of every bitmap block
  blockid_t bitmap_blockid;
  char mask;
  char temp;
  int last_inode_block = IBLOCK(INODE_NUM, sb.nblocks);

  for(int i = 0; i < BLOCK_NUM; i += BPB){
    /*read the block of bitmap*/
    bitmap_blockid = BBLOCK(i);
    read_block(bitmap_blockid, buf);

    /*check every bit of block*/
    for(int j = 0; j < BPB; j++){
      /*find block from the 1st block after the last inode*/
      if(j <= last_inode_block) continue;

      mask = 1 << (j % 8);
      temp = buf[j/8];

      if(!(mask&temp)){// if is free
        /*mark the bit of the block and return the blockid*/
        buf[j/8] = mask | temp;
        write_block(bitmap_blockid, buf);
        return j + i;
      }
    }
  }

  /*if there is no free block*/
  return -1;
}

void
block_manager::free_block(uint32_t id)
{
  /*
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */

  /*can't free the bitmap and superblock*/
  if(id < BBLOCK(BLOCK_NUM) || id > BLOCK_NUM)
    return;

  char buf[BLOCK_SIZE];
  blockid_t bit_block = BBLOCK(id);
  int offset = id % BPB;
  int offset_byte = offset / 8;
  char mask = ~(1 << (offset % 8));
  read_block(bit_block, buf);

  buf[offset_byte] = buf[offset_byte] & mask;
  write_block(bit_block, buf);
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  int block_num;
  inode_t *targetInode;
  char buf[BLOCK_SIZE];
  uint32_t inode_num;

  /*find empty inode*/
  for(inode_num = 1; inode_num <= INODE_NUM; inode_num++){
    block_num = IBLOCK(inode_num, bm->sb.nblocks);
    bm->read_block(block_num, buf);
    targetInode = (struct inode*)buf + (inode_num - 1)%IPB;

    if(targetInode->type == 0) break;
  }

  targetInode->type = type;
  targetInode->size = 0;
  targetInode->atime = (unsigned int)time(NULL);
  targetInode->mtime = (unsigned int)time(NULL);
  targetInode->ctime = (unsigned int)time(NULL);

  bm->write_block(block_num, buf);
  /*
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  return inode_num;
}

/*
 * your code goes here.
 * note: you need to check if the inode is already a freed one;
 * if not, clear it, and remember to write back to disk.
 */
void
inode_manager::free_inode(uint32_t inum)
{
  if(inum < 0 || inum > INODE_NUM) return;

  inode_t *targetInode = get_inode(inum);
  targetInode->type = 0;
  put_inode(inum, targetInode);
  /*char buf[BLOCK_SIZE];
  inode_t *targetInode;
  blockid_t inode_blockid = IBLOCK(inumm, bm->sb.nblocks);

  bm->read_block(inode_blockid, buf);
  targetInode = (inode_t *)buf + (inum - 1) % IPB;*/
  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode*
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d \n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + (inum - 1)%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  printf("getindoe inodetype %d inodesize %d\n", ino->type, ino->size);
  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d inodetype %d inodesize %d\n", inum, ino->type, ino->size);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + (inum - 1)%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum.
 * Return alloced data, should be freed by caller. */
 /*
  * your code goes here.
  * note: read blocks related to inode number inum,
  * and copy them to buf_Out
  */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  inode_t *ino = get_inode(inum);

  if(!ino) return; //not exist

  char buf[BLOCK_SIZE];
  blockid_t indirect_buf[NINDIRECT];
  *size = ino->size;
  *buf_out = (char *)malloc(ino->size);

  int all_block_num = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  int rest_bytes = ino->size % BLOCK_SIZE;

  if(all_block_num <= NDIRECT){
    int temp = all_block_num;
    if(rest_bytes) temp--;
    for(int i = 0; i < temp; i++)
      bm->read_block(ino->blocks[i], *buf_out + i * BLOCK_SIZE);

    if(rest_bytes){
      bm->read_block(ino->blocks[all_block_num - 1], buf);
      memcpy(*buf_out + (all_block_num - 1) * BLOCK_SIZE, buf, rest_bytes);
    }
  }else{
    for(int i = 0; i < NDIRECT; i++)
      bm->read_block(ino->blocks[i], *buf_out + i * BLOCK_SIZE);

    bm->read_block(ino->blocks[NDIRECT], (char *)indirect_buf);
    int delt_num = all_block_num - NDIRECT;

    if(rest_bytes) delt_num--;
    for(int i = 0; i < delt_num; i++)
      bm->read_block(indirect_buf[i], *buf_out + (NDIRECT + i) * BLOCK_SIZE);

    if(rest_bytes){
      bm->read_block(indirect_buf[all_block_num - NDIRECT - 1], buf);
      memcpy(*buf_out + (all_block_num - 1) * BLOCK_SIZE, buf, rest_bytes);
    }
  }

  ino->atime = (unsigned int)time(NULL);
  put_inode(inum, ino);
  free(ino);
  return;
}

/* alloc/free blocks if needed */
/*
 * your code goes here.
 * note: write buf to blocks of inode inum.
 * you need to consider the situation when the size of buf
 * is larger or smaller than the size of original inode
 */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{

  if(size < 0 || !buf) return;
  if (inum < 0 || inum > INODE_NUM) {
    printf("write_file: inum out of range\n");
    return;
  }

  inode_t *ino = get_inode(inum);
  int old_block_num = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  int new_block_num = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  blockid_t indirect_buf[NINDIRECT];

  blockid_t all_blockids[MAXFILE]; // contain all blockid of the new inode

  //copy the old blockids to all_blockids
  if(old_block_num <= NDIRECT){
    memcpy(all_blockids, ino->blocks, old_block_num * sizeof(blockid_t));
  }else{
    memcpy(all_blockids, ino->blocks, NDIRECT * sizeof(blockid_t));
    bm->read_block(ino->blocks[NDIRECT], (char *)indirect_buf);
    memcpy(all_blockids + NDIRECT, indirect_buf, (old_block_num - NDIRECT) * sizeof(blockid_t));
  }

  //adjust the number of blocks
  // no adjust if thay have the same number of blocks
  if(old_block_num > new_block_num){// smaller than the old  need to free some blocks
    for(int i = new_block_num; i < old_block_num; i++)
      bm->free_block(all_blockids[i]);

    if(old_block_num > NDIRECT && new_block_num <= NDIRECT)
      bm->free_block(ino->blocks[NDIRECT]);
  }else if(old_block_num < new_block_num){// bigger than the old need to alloc blocks
    for(int i = old_block_num; i < new_block_num; i++)
      all_blockids[i] = bm->alloc_block();

    if(old_block_num <= NDIRECT && new_block_num > NDIRECT)
      ino->blocks[NDIRECT] = bm->alloc_block();

  }

  //write data to disk
  int temp_num = size / BLOCK_SIZE;
  int rest_bytes = size % BLOCK_SIZE;
  char temp_buf[BLOCK_SIZE];

  for(int i = 0; i < temp_num; i++)
    bm->write_block(all_blockids[i], buf + i * BLOCK_SIZE);

  if(rest_bytes){
    memcpy(temp_buf, buf + BLOCK_SIZE * temp_num, rest_bytes);
    bm->write_block(all_blockids[temp_num], temp_buf);
  }
  //write the block id to ino
  if(new_block_num <= NDIRECT){
    memcpy(ino->blocks, all_blockids, new_block_num * sizeof(blockid_t));
  }else{
    memcpy(ino->blocks, all_blockids, NDIRECT * sizeof(blockid_t));
    char tempBuf[BLOCK_SIZE];
    memcpy(tempBuf, all_blockids + NDIRECT, (new_block_num - NDIRECT) * sizeof(blockid_t));
    bm->write_block(ino->blocks[NDIRECT], tempBuf);
  }

  ino->size = size;
  ino->ctime = (unsigned int)time(NULL);
  ino->mtime = (unsigned int)time(NULL);
  put_inode(inum, ino);
  free(ino);
  return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  if (!inum)
    return;

  if (inum < 0 || inum >= INODE_NUM) {
    printf("getattr: inum out of range\n");
    return;
  }

  inode_t *temp = get_inode(inum);

  if(!temp) return;
  a.type = temp->type;
  a.size = temp->size;
  a.ctime = temp->ctime;
  a.mtime = temp->mtime;
  a.atime = temp->atime;
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */

  return;
}

/*
 * your code goes here
 * note: you need to consider about both the data block and inode of the file
 */
void
inode_manager::remove_file(uint32_t inum)
{
  inode_t *ino = get_inode(inum);
  if(!ino) return; // not exisst

  blockid_t indirect_buf[NINDIRECT];

  int all_block_num = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;

  if(all_block_num <= NDIRECT){
    for(int i = 0; i < all_block_num; i++)
      bm->free_block(ino->blocks[i]);
  }else{
    for(int i = 0; i < NDIRECT; i++)
      bm->free_block(ino->blocks[i]);

    /*free indirect block*/
    bm->read_block(ino->blocks[NDIRECT], (char *)indirect_buf);
    int delt_num = all_block_num - NDIRECT;
    for(int i = 0; i < delt_num; i++)
      bm->free_block(indirect_buf[i]);

    bm->free_block(ino->blocks[NDIRECT]);
  }

  free_inode(inum);
  free(ino);
  return;
}

//append a block to the node and return its block_id
void
inode_manager::append_block(uint32_t inum, blockid_t &bid)
{
  inode_t *ino = get_inode(inum);
  if(!ino) return; //not exist

  int all_block_num = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  ino->size += BLOCK_SIZE;
  blockid_t indirect_buf[NINDIRECT];

  if(all_block_num < NDIRECT){
    bid = bm->alloc_block();
    ino->blocks[all_block_num] = bid;
  }else{
    bm->read_block(ino->blocks[NDIRECT], (char*)indirect_buf);
    blockid_t* temp_buf = (blockid_t*)indirect_buf;
    bid = bm->alloc_block();
    temp_buf[all_block_num - NDIRECT] = bid;
  }

  //write back the inode
  put_inode(inum, ino);
  free(ino);
}

void
inode_manager::get_block_ids(uint32_t inum, std::list<blockid_t> &block_ids)
{
  inode_t *ino = get_inode(inum);
  if(!ino) return; //not exist

  int all_block_num = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  blockid_t indirect_buf[NINDIRECT];

  if(all_block_num <= NDIRECT){
    for(int i = 0; i < all_block_num; i++)
      block_ids.push_back(ino->blocks[i]);
  }else{
    for(int i = 0; i < NDIRECT; i++)
      block_ids.push_back(ino->blocks[i]);

    //read indirect block id
    bm->read_block(ino->blocks[NDIRECT], (char *)indirect_buf);
    //blockid_t* temp_buf = (blockid_t*)indirect_buf;
    int delt = all_block_num - NDIRECT;
    for(int i = 0; i < delt; i++)
      block_ids.push_back(indirect_buf[i]);
  }

  free(ino);
}

void
inode_manager::read_block(blockid_t id, char buf[BLOCK_SIZE])
{
  bm->read_block(id, buf);
}

void
inode_manager::write_block(blockid_t id, const char buf[BLOCK_SIZE])
{
  bm->write_block(id, buf);
}

void
inode_manager::complete(uint32_t inum, uint32_t size)
{
  inode_t *ino = get_inode(inum);
  if(!ino) return; //not exist

  ino->size = size;
  ino->atime = (unsigned int)time(NULL);
  ino->mtime = (unsigned int)time(NULL);
  ino->ctime = (unsigned int)time(NULL);
  put_inode(inum, ino);
  free(ino);
}
