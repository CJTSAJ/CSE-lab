// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include <map>
#include <queue>

#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"

using namespace std;
// Classes that inherit lock_release_user can override dorelease so that
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 6.

struct thread_info{
  pthread_t thread;
  pthread_cond_t cond;

  thread_info(pthread_t p){thread = p;pthread_cond_init(&cond, NULL);}
};

typedef struct{
  bool recieve_revoke;
  pthread_t owner;
  queue<thread_info*> waitting_thread;
  rlock_protocol::state lock_state;
} lock_info;

class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user() {};
};

class lock_client_cache : public lock_client {
 private:
  class lock_release_user *lu;
  int rlock_port;
  std::string hostname;
  std::string id;
  map<lock_protocol::lockid_t, lock_info> lock_list; //all locks that this client hold
  pthread_mutex_t mutex;
  //pthread_cond_t cond;

 public:
  static int last_port;
  lock_client_cache(std::string xdst, class lock_release_user *l = 0);
  virtual ~lock_client_cache() {};
  lock_protocol::status acquire(lock_protocol::lockid_t);
  lock_protocol::status release(lock_protocol::lockid_t);
  rlock_protocol::status revoke_handler(lock_protocol::lockid_t,
                                        int &);
  rlock_protocol::status retry_handler(lock_protocol::lockid_t,
                                       int &);
  void checkAndAcquire(lock_protocol::lockid_t lid,
                                   int &);
  void acquireUntilGot(lock_protocol::lockid_t lid,
                                   thread_info* tmpThread);
};


#endif
