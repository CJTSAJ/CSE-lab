// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"


int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst,
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

	pthread_mutex_init(&mutex, NULL);
	lock_list.clear();
	//pthread_cond_init(&cond);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
	//tprintf("client acquire\n");
	pthread_mutex_lock(&mutex);

	pthread_t selfThread = pthread_self();
	tprintf("lock-client\tid:%s\tthread:%ld\tacquire lock:%ld\n",id.c_str(),selfThread,lid);
  int ret = lock_protocol::OK;
	int r;
	thread_info* tmpThread = new thread_info(selfThread);
	//check if the client hold the clock
	if(lock_list.find(lid) == lock_list.end()){
		tprintf("first acquire\n");
		//no have the clock and acquire it from lock_server
		lock_info tmpLock;
		tmpLock.recieve_revoke = false;
		tmpLock.lock_state = rlock_protocol::ACQUIRING;
		tmpLock.owner = -1;

		lock_list[lid] = tmpLock;
		acquireUntilGot(lid, tmpThread);
		/*pthread_mutex_unlock(&mutex);

		lock_protocol::status tmpRet = cl->call(lock_protocol::acquire, lid, id, r);

		pthread_mutex_lock(&mutex);
		if(tmpRet == lock_protocol::OK){// acquire successfully
			lock_list[lid].lock_state = rlock_protocol::LOCKED;
			lock_list[lid].owner = selfThread;
		}else{ //RETRY   and impossible for OWNED
			tprintf("lock-client\tid:%s\tthread:%ld\tcall return RETRY:%ld\n",id.c_str(),selfThread,lid);
			thread_info* tmpThread = new thread_info(selfThread);
			lock_list[lid].waitting_thread.push(tmpThread);

			while(lock_list[lid].owner != selfThread){
				pthread_cond_wait(&(tmpThread->cond), &mutex);
				tprintf("lock-client\tid:%s\tthread:%ld\tacquire wake go on:%ld\n",id.c_str(),selfThread,lid);
			}
		}*/

	}else{

		//check the state of lock
		switch (lock_list[lid].lock_state) {
			case rlock_protocol::FREE:{
				lock_list[lid].owner = selfThread;
				lock_list[lid].lock_state = rlock_protocol::LOCKED;
				break;
			}
			case rlock_protocol::RELEASING:
			case rlock_protocol::ACQUIRING:
			case rlock_protocol::LOCKED:{
				lock_list[lid].waitting_thread.push(tmpThread);
				pthread_cond_wait(&(tmpThread->cond), &mutex);
				if(lock_list[lid].lock_state == rlock_protocol::LOCKED &&
					lock_list[lid].owner == selfThread){
					pthread_mutex_unlock(&mutex);
					return lock_protocol::OK;
				}
			}
			case rlock_protocol::NONE:{// acquire the lock from the server
				tprintf("lock-client\tid:%s\tthread:%ld\tacquire NONE:%ld\n",id.c_str(),selfThread,lid);
				acquireUntilGot(lid, tmpThread);
				/*lock_list[lid].lock_state = rlock_protocol::ACQUIRING;
				pthread_mutex_unlock(&mutex);

				lock_protocol::status tmpRet = cl->call(lock_protocol::acquire, lid, id, r);

				pthread_mutex_lock(&mutex);
				if(tmpRet == lock_protocol::OK){// acquire successfully
					lock_list[lid].lock_state = rlock_protocol::LOCKED;
					lock_list[lid].owner = selfThread;
				}else{ //RETRY and OWNED
					tprintf("lock-client\tid:%s\tthread:%ld\tacquire NONE RETRY:%ld\n",id.c_str(),selfThread,lid);
					thread_info* tmpThread = new thread_info(selfThread);
					lock_list[lid].waitting_thread.push(tmpThread);

					while(lock_list[lid].owner != selfThread)
						pthread_cond_wait(&(tmpThread->cond), &mutex);
				}*/
				break;
			}
			default:{ // sleep and return OK later
				/*tprintf("lock-client\tid:%s\tthread:%ld\tacquire default:%d\n",id.c_str(),selfThread,lock_list[lid].lock_state);
				thread_info* tmpThread = new thread_info(selfThread);
				lock_list[lid].waitting_thread.push(tmpThread);

				while(lock_list[lid].owner != selfThread)
					pthread_cond_wait(&(tmpThread->cond), &mutex);*/

				break;
			}
		}
	}

	pthread_mutex_unlock(&mutex);
  return ret;
}

//if server send a revoke signal, return lock to server
lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
	//tprintf("client release\n");
	pthread_mutex_lock(&mutex);
	int r;
  int ret = lock_protocol::OK;
	pthread_t selfThread = pthread_self();
	tprintf("lock-client\tid:%s\tthread:%ld\trelease lock:%ld\n",id.c_str(),selfThread,lid);
	//check if the client have the lock
	if(lock_list.find(lid) == lock_list.end() || lock_list[lid].owner != selfThread){
		tprintf("lock-client\trelease lock error\n")
		pthread_mutex_unlock(&mutex);
		return lock_protocol::RPCERR;
	}

	bool hasRevoke = false;
	//return the lock
	if(lock_list[lid].recieve_revoke){
		hasRevoke = true;
		lock_list[lid].lock_state = rlock_protocol::RELEASING;
		pthread_mutex_unlock(&mutex);
		tprintf("lock-client\tid:%s\tthread:%ld\trelease revoke:%ld\n",id.c_str(),pthread_self(),lid);
		cl->call(lock_protocol::release, lid, id, r);

		pthread_mutex_lock(&mutex);
		lock_list[lid].recieve_revoke = false;
		lock_list[lid].lock_state = rlock_protocol::NONE;
		//checkAndAcquire(lid, r);
	}else lock_list[lid].lock_state = rlock_protocol::FREE;

	if(!lock_list[lid].waitting_thread.empty()){
		thread_info* tmpThread = lock_list[lid].waitting_thread.front();
		lock_list[lid].waitting_thread.pop();

		if(!hasRevoke){
			lock_list[lid].owner = tmpThread->thread;
			lock_list[lid].lock_state = rlock_protocol::LOCKED;
		}
		pthread_cond_signal(&(tmpThread->cond));
	}else{//if empty return
		lock_list[lid].lock_state = rlock_protocol::RELEASING;
		pthread_mutex_unlock(&mutex);
		cl->call(lock_protocol::release, lid, id, r);
		pthread_mutex_lock(&mutex);
		lock_list[lid].lock_state = rlock_protocol::NONE;
		if(!lock_list[lid].waitting_thread.empty()){
			thread_info* tmpThread = lock_list[lid].waitting_thread.front();
			lock_list[lid].waitting_thread.pop();
			pthread_cond_signal(&(tmpThread->cond));
		}
	}
	pthread_mutex_unlock(&mutex);
  return lock_protocol::OK;

	/*else{//wake up a thread
		if(!lock_list[lid].waitting_thread.empty()){
			thread_info* tmpThread = lock_list[lid].waitting_thread.front();
			tprintf("lock-client\tid:%s\tthread:%ld\trelease wake:%ld\n",id.c_str(),pthread_self(),tmpThread->thread);
			lock_list[lid].waitting_thread.pop();
			lock_list[lid].lock_state = rlock_protocol::LOCKED;
			lock_list[lid].owner = tmpThread->thread;
			pthread_cond_signal(&(tmpThread->cond));
		}else {
			tprintf("lock-client\tid:%s\tthread:%ld\trelease return:%ld\n",id.c_str(),pthread_self(),lid);
			lock_list[lid].lock_state = rlock_protocol::RELEASING;
			pthread_mutex_unlock(&mutex);

			cl->call(lock_protocol::release, lid, id, r);

			pthread_mutex_lock(&mutex);
			checkAndAcquire(lid, r);
		}
	}*/
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid,
                                  int &)
{
	tprintf("lock-client\tid:%s\tthread:%ld\trevoke:%ld\n",id.c_str(),pthread_self(),lid);
	pthread_mutex_lock(&mutex);
  int ret = rlock_protocol::OK;
	int r;
	if(lock_list[lid].lock_state == rlock_protocol::FREE){
		lock_list[lid].lock_state = rlock_protocol::RELEASING;
		pthread_mutex_unlock(&mutex);

		cl->call(lock_protocol::release, lid, id, r);

		pthread_mutex_lock(&mutex);
		lock_list[lid].lock_state = rlock_protocol::NONE;
		if(!lock_list[lid].waitting_thread.empty()){
			thread_info* tmpThread = lock_list[lid].waitting_thread.front();
			lock_list[lid].waitting_thread.pop();
			pthread_cond_signal(&(tmpThread->cond));
		}
	}else lock_list[lid].recieve_revoke = true;

	pthread_mutex_unlock(&mutex);
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid,
                                 int &)
{
	//tprintf("client retry\n");
	pthread_mutex_lock(&mutex);
	tprintf("lock-client\tid:%s\tthread:%ld\tretry:%ld\n",id.c_str(),pthread_self(),lid);
	//pthread_t selfThread = pthread_self();
  int ret = rlock_protocol::OK;
	int r;
	//wake up a thread
	if(!lock_list[lid].waitting_thread.empty()){
		lock_list[lid].lock_state = rlock_protocol::LOCKED;
		thread_info* tmpThread = lock_list[lid].waitting_thread.front();
		lock_list[lid].owner = tmpThread->thread;
		lock_list[lid].waitting_thread.pop();
		tprintf("lock-client\tid:%s\tthread:%ld\tretry wake up:%ld\n",id.c_str(),pthread_self(),tmpThread->thread);
		pthread_cond_signal(&(tmpThread->cond));
	}else {
		lock_list[lid].lock_state = rlock_protocol::RELEASING;
		pthread_mutex_unlock(&mutex);
		cl->call(lock_protocol::release, lid, id, r);
		pthread_mutex_lock(&mutex);
		lock_list[lid].lock_state = rlock_protocol::NONE;
		if(!lock_list[lid].waitting_thread.empty()){
			thread_info* tmpThread = lock_list[lid].waitting_thread.front();
			lock_list[lid].waitting_thread.pop();
			pthread_cond_signal(&(tmpThread->cond));
		}
		//checkAndAcquire(lid, r);
	}

	pthread_mutex_unlock(&mutex);
  return ret;
}

void
lock_client_cache::checkAndAcquire(lock_protocol::lockid_t lid, int &r)
{
	if(lock_list[lid].waitting_thread.empty())
		lock_list[lid].lock_state = rlock_protocol::NONE;
	else{
		lock_list[lid].lock_state = rlock_protocol::ACQUIRING;
		pthread_mutex_unlock(&mutex);
		lock_protocol::status tmpRet = cl->call(lock_protocol::acquire, lid, id, r);
		pthread_mutex_lock(&mutex);
		if(tmpRet == lock_protocol::OK){
			if(!lock_list[lid].waitting_thread.empty()){
				thread_info* tmpThread = lock_list[lid].waitting_thread.front();
				lock_list[lid].waitting_thread.pop();
				lock_list[lid].lock_state = rlock_protocol::LOCKED;
				lock_list[lid].owner = tmpThread->thread;
				pthread_cond_signal(&(tmpThread->cond));
			}else cl->call(lock_protocol::release, lid, id, r);
		}
	}
}

void
lock_client_cache::acquireUntilGot(lock_protocol::lockid_t lid, thread_info* tmpThread)
{
	int r;
	lock_list[lid].lock_state = rlock_protocol::ACQUIRING;
	while(true){
		pthread_mutex_unlock(&mutex);
		lock_protocol::status tmpRet = cl->call(lock_protocol::acquire, lid, id, r);
		pthread_mutex_lock(&mutex);

		if(tmpRet == lock_protocol::OK){// acquire successfully
			lock_list[lid].lock_state = rlock_protocol::LOCKED;
			lock_list[lid].owner = tmpThread->thread;
			return;
		}else{ //RETRY and OWNED
			lock_list[lid].waitting_thread.push(tmpThread);

			pthread_cond_wait(&(tmpThread->cond), &mutex);
			lock_list[lid].recieve_revoke = false;
			if(lock_list[lid].owner == tmpThread->thread &&
				lock_list[lid].lock_state == rlock_protocol::LOCKED)
				break;
		}
	}
}
