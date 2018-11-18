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
  lock_protocol::status ret = lock_protocol::OK;

  pthread_mutex_lock(&mutex);
  //alloc a new lock
  if(lock_list.find(lid) == lock_list.end()){
    lock_info tmpLockInfo;
    tmpLockInfo.owner = lid;
    tmpLockInfo.lock_state = rlock_protocol::LOCKED;
    lock_list[lid] = tmpLockInfo;

    pthread_mutex_unlock(&mutex);
  }else{
    //check the state of lock
    switch (lock_list[lid].lock_state) {
      case rlock_protocol::FREE:{
        lock_list[lid].owner = id;
        lock_list[lid].lock_state = rlock_protocol::LOCKED;
        pthread_mutex_unlock(&mutex);
        break;
      }
      case rlock_protocol::LOCKED:{
        if(id == lock_list[lid].owner){
          pthread_mutex_unlock(&mutex);
          return lock_protocol::OWNED;
        }
        //add the thread to waitting queue, return retry and send revoke to client that hold the clock
        lock_list[lid].waitting_client.push(id);

        //Since bind may block, the caller probably should not hold a mutex when calling safebind
        pthread_mutex_unlock(&mutex);

        handle tmpHandle(lock[lid].owner);
        rpcc* cl = tmpHandle.safebind();

        int r;
        cl->call(rlock_protocol::revoke, lid, r);

        return lock_protocol::RETRY;
      }
      default:{
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
  lock_protocol::status ret = lock_protocol::OK;

  //check exist   check owner
  if(lock_list.find(lid) == lock_list.end() || lock_list[lid].owner != id){
    pthread_mutex_unlock(&mutex);
    return rlock_protocol::RPCERR;
  }

  //check if there are waitting clients
  if(lock_list[lid].waitting_client.empty()){
    //if empty, set the lock FREE
    lock_list[lid].lock_state = rlock_protocol::FREE;
    pthread_mutex_unlock(&mutex);
  }else{
    string nextClient = lock_list[lid].waitting_client.front();
    lock_list.waitting_client.pop();
    lock_list.lock_state = rlock_protocol::LOCKED;
    lock_list.owner = id;

    //let the next client retry
    pthread_mutex_unlock(&mutex);
    handle tmpHandle(nextClient);
    rpcc* cl = tmpHandle.safebind();

    int r;
    cl->call(rlock_protocol::retry, lid, r);
  }
  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}
