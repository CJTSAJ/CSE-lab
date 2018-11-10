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
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;

	//check if the client hold the clock
	if(lock_list.find(lid) == lock_list.end()){
		//no have the clock and acquire it from lock_server

	}else{
		//check the state of lock
		switch (lock_list[lid].lock_state) {
			case rlock_protocol::FREE:{

			}
			case rlock_protocol::LOCKED:{

			}
			case rlock_protocol::NONE:{

			}
			case rlock_protocol::ACQUIRING{

			}
			default:{

			}
		}
	}
  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
	//check if the client have the lock
	if(lock_list.find(lid) == lock_list.end()){

	}

	//check the state of the lock
	switch (lock_list[lid].lock_state) {
		case :
	}
  return lock_protocol::OK;

}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid,
                                  int &)
{
  int ret = rlock_protocol::OK;
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid,
                                 int &)
{
  int ret = rlock_protocol::OK;
  return ret;
}
