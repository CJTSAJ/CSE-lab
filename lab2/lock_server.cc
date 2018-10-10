// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire(0)
{
  lock_list.clear();
  lock_nacquire.clear();
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&cond, NULL);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  pthread_mutex_lock(&mutex);
  if(lock_nacquire.find(lid) == lock_nacquire.end()){
    r = 0;
  }else{
    r = lock_nacquire[lid];
  }
  pthread_mutex_unlock(&mutex);
  return lock_protocol::OK;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  printf("acquire request from clt %d\n", clt);
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2 part2 code goes here
  pthread_mutex_lock(&mutex);

  if(lock_list.find(lid) != lock_list.end()){ // lock exist
    while(lock_list[lid] == true){
      pthread_cond_wait(&cond, &mutex);
    }
  }else{
    lock_nacquire[lid] = 0;
  }

  lock_list[lid] = true;
  lock_nacquire[lid]++;
  pthread_mutex_unlock(&mutex);

  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  printf("release request from clt %d\n", clt);
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2 part2 code goes here
  pthread_mutex_lock(&mutex);

  if(lock_list.find(lid) == lock_list.end())
    return lock_protocol::NOENT;

  lock_list[lid] = false;

  pthread_cond_broadcast(&cond);
  pthread_mutex_unlock(&mutex);
  return ret;
}
