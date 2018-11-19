// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


lock_server_cache::lock_server_cache()
{
  //init
  lock_list.clear();
  pthread_mutex_init(&mutex, NULL);
  //pthread_cond_init(&cond, NULL);
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id,
                               int &)
{
  //tprintf("server acquire\n");
  pthread_mutex_lock(&mutex);
  tprintf("lock-server\tid:%s\tacquire lock:%ld\n",id.c_str(),lid);
  lock_protocol::status ret = lock_protocol::OK;
  //alloc a new lock
  if(lock_list.find(lid) == lock_list.end()){
    tprintf("server acquire: alloc new lock\n");
    lock_info tmpLockInfo;
    tmpLockInfo.owner = id;
    tmpLockInfo.lock_state = rlock_protocol::LOCKED;
    lock_list[lid] = tmpLockInfo;

    pthread_mutex_unlock(&mutex);
  }else{
    //check the state of lock
    switch (lock_list[lid].lock_state) {
      case rlock_protocol::FREE:{
        tprintf("lock-server\tid:%s\tacquire lock:%ld\tfree lock\n",id.c_str(),lid);
        lock_list[lid].owner = id;
        lock_list[lid].lock_state = rlock_protocol::LOCKED;
        pthread_mutex_unlock(&mutex);
        break;
      }
      case rlock_protocol::LOCKED:{
        tprintf("lock-server\tid:%s\tacquire lock:%ld\tlocked lock\n",id.c_str(),lid);
        if(id == lock_list[lid].owner){
          pthread_mutex_unlock(&mutex);
          return lock_protocol::OWNED;
        }
        //add the thread to waitting queue, return retry and send revoke to client that hold the clock
        lock_list[lid].waitting_client.push(id);
        lock_list[lid].lock_state = rlock_protocol::ACQUIRING;
        //Since bind may block, the caller probably should not hold a mutex when calling safebind
        pthread_mutex_unlock(&mutex);
        tprintf("lock-server\tid:%s\tacquire lock:%ld\trevoke owner:%s\n",id.c_str(),lid,lock_list[lid].owner.c_str());
        handle tmpHandle(lock_list[lid].owner);
        rpcc* cl = tmpHandle.safebind();

        int r;
        cl->call(rlock_protocol::revoke, lid, r);

        return lock_protocol::RETRY;
      }
      case rlock_protocol::ACQUIRING:{
        tprintf("lock-server\tid:%s\tacquire lock:%ld\tacquiring lock\n",id.c_str(),lid);
        lock_list[lid].waitting_client.push(id);
        pthread_mutex_unlock(&mutex);
        return lock_protocol::RETRY;
      }
      default:{
        tprintf("server acquire: default\n");
        pthread_mutex_unlock(&mutex);
        break;
      }
    }
  }
  return ret;
}

int
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id,
         int &r)
{
  pthread_mutex_lock(&mutex);
  tprintf("lock-server\tid:%s\trelease lock:%ld\n",id.c_str(),lid);
  lock_protocol::status ret = lock_protocol::OK;

  //check exist   check owner
  if(lock_list.find(lid) == lock_list.end() || lock_list[lid].owner != id){
    pthread_mutex_unlock(&mutex);
    return rlock_protocol::RPCERR;
  }

  //check if there are waitting clients
  if(lock_list[lid].waitting_client.empty()){
    //if empty, set the lock FREE
    tprintf("lock-server\tid:%s\trelease lock empty:%ld\n",id.c_str(),lid);
    lock_list[lid].lock_state = rlock_protocol::FREE;
    pthread_mutex_unlock(&mutex);
  }else{
    string nextClient = lock_list[lid].waitting_client.front();
    tprintf("lock-server\tid:%s\trelease lock next:%s\n",id.c_str(),nextClient.c_str());
    lock_list[lid].waitting_client.pop();
    lock_list[lid].lock_state = rlock_protocol::LOCKED;
    lock_list[lid].owner = nextClient;

    //let the next client retry
    pthread_mutex_unlock(&mutex);
    handle tmpHandle(nextClient);
    rpcc* cl = tmpHandle.safebind();

    int r;
    cl->call(rlock_protocol::retry, lid, r);
  }
  return ret;
}

/*void
lock_server_cache::addWait(lock_protocol::lockid_t, std::string id, int &)
{
  lock_list[lid].waitting_client.push(id);
}*/

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}
