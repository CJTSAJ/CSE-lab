#include "datanode.h"
#include <arpa/inet.h>
#include "extent_client.h"
#include <unistd.h>
#include <algorithm>
#include "threader.h"

using namespace std;

int DataNode::init(const string &extent_dst, const string &namenode, const struct sockaddr_in *bindaddr) {
  ec = new extent_client(extent_dst);

  // Generate ID based on listen address
  id.set_ipaddr(inet_ntoa(bindaddr->sin_addr));
  id.set_hostname(GetHostname());
  id.set_datanodeuuid(GenerateUUID());
  id.set_xferport(ntohs(bindaddr->sin_port));
  id.set_infoport(0);
  id.set_ipcport(0);

  // Save namenode address and connect
  make_sockaddr(namenode.c_str(), &namenode_addr);

  if (!ConnectToNN()) {
    delete ec;
    ec = NULL;
    return -1;
  }

  // Register on namenode
  if (!RegisterOnNamenode()) {
    delete ec;
    ec = NULL;
    close(namenode_conn);
    namenode_conn = -1;
    return -1;
  }

  /* Add your initialization here */
  NewThread(this, &DataNode::KeepBeating);
  return 0;
}

void DataNode::KeepBeating()
{
  while(true){
    SendHeartbeat();
    sleep(1);
  }
}

//read bblock maybe only read parts of file
bool DataNode::ReadBlock(blockid_t bid, uint64_t offset, uint64_t len, string &buf) {
  /* Your lab4 part 2 code */
  printf("datanode\tReadBlock\tbid:%d\toff:%d\tlen:%d\n",bid,offset,len);fflush(stdout);
  string tmp_buf;
  extent_protocol::status ret = ec->read_block(bid, tmp_buf);
  printf("datanode\tReadBlock\tbuf:%s\n",tmp_buf.c_str());fflush(stdout);
  if(ret != extent_protocol::OK)
    return false;

  buf = tmp_buf.substr(offset, len);
  return true;
}

//write block maybe only write parts of file
bool DataNode::WriteBlock(blockid_t bid, uint64_t offset, uint64_t len, const string &buf) {
  /* Your lab4 part 2 code */
  printf("datanode\tWriteBlock\tbid:%d\toff:%d\tlen:%d\tbuf:%s\n",bid,offset,len,buf.c_str());fflush(stdout);
  string tmp_buf;
  extent_protocol::status ret = ec->read_block(bid, tmp_buf);
  if(ret != extent_protocol::OK)
    return false;

  tmp_buf.replace(offset, len, buf);
  ret = ec->write_block(bid, tmp_buf);
  if(ret != extent_protocol::OK)
    return false;

  return true;
}
