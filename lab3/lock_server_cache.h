#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include <queue>
#include <map>

#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"

typedef struct{
  rlock_protocol::state lock_state;
  string owner;
  queue<string> waitting_thread;
}lock_info;

class lock_server_cache {
 private:
  int nacquire;
  pthread_mutex_t mutex;
  //pthread_cond_t cond;
  map<lock_protocol::lockid_t, lock_info> lock_list;

 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
