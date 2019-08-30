/** Copyright 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018 Roland Olbricht et al.
 *
 * This file is part of Overpass_API.
 *
 * Overpass_API is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * Overpass_API is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Overpass_API.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "dispatcher_client.h"
#include "dispatcher.h"

#include <fcntl.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>


Dispatcher_Client::Dispatcher_Client
    (const std::string& dispatcher_share_name_)
    : dispatcher_share_name(dispatcher_share_name_), socket("")
{
  signal(SIGPIPE, SIG_IGN);

  // open dispatcher_share
  dispatcher_shm_fd = shm_open
      (dispatcher_share_name.c_str(), O_RDONLY, S_444);
  if (dispatcher_shm_fd < 0)
    throw File_Error
        (errno, dispatcher_share_name, "Dispatcher_Client::1");
  struct stat stat_buf;
  fstat(dispatcher_shm_fd, &stat_buf);
  dispatcher_shm_ptr = (uint8*)mmap
      (0, stat_buf.st_size,
       PROT_READ, MAP_SHARED, dispatcher_shm_fd, 0);

  // get db_dir and shadow_name
  db_dir = std::string((const char *)(dispatcher_shm_ptr + 4*sizeof(uint32)),
		  *(uint32*)(dispatcher_shm_ptr + 3*sizeof(uint32)));
  shadow_name = std::string((const char *)(dispatcher_shm_ptr + 5*sizeof(uint32)
      + db_dir.size()), *(uint32*)(dispatcher_shm_ptr + db_dir.size() +
		       4*sizeof(uint32)));

  // initialize the socket for the client
  socket.open(db_dir + dispatcher_share_name_);
  std::string socket_name = db_dir + dispatcher_share_name_;

// TODO: No need to send PID anymore
//  pid_t pid = getpid();
//  if (send(socket.descriptor(), &pid, sizeof(pid_t), 0) == -1)
//    throw File_Error(errno, dispatcher_share_name, "Dispatcher_Client::4");
}


bool file_present(const std::string& full_path)
{
  struct stat stat_buf;
  int result = stat(full_path.c_str(), &stat_buf);
  return result == 0;
}


Dispatcher_Client::~Dispatcher_Client()
{
  munmap((void*)dispatcher_shm_ptr,
	 Dispatcher::SHM_SIZE + db_dir.size() + shadow_name.size());
  close(dispatcher_shm_fd);
}


template< class TObject >
void Dispatcher_Client::send_message(TObject message, const std::string& source_pos)
{
  if (send(socket.descriptor(), &message, sizeof(TObject), 0) == -1)
    throw File_Error(errno, dispatcher_share_name, source_pos);
}


uint32 Dispatcher_Client::ack_arrived()
{
  uint32 answer = 0;
  int bytes_read = recv(socket.descriptor(), &answer, sizeof(uint32), 0);
  while (bytes_read == -1)
  {
    millisleep(50);
    bytes_read = recv(socket.descriptor(), &answer, sizeof(uint32), 0);
  }
  if (bytes_read == sizeof(uint32))
    return answer;

  return 0;
}


void Dispatcher_Client::write_start()
{
  pid_t pid = getpid();

  send_message(Dispatcher::WRITE_START, "Dispatcher_Client::write_start::socket");

  while (true)
  {
    if (ack_arrived() && file_exists(shadow_name + ".lock"))
    {
      try
      {
	pid_t locked_pid = 0;
	std::ifstream lock((shadow_name + ".lock").c_str());
	lock>>locked_pid;
	if (locked_pid == pid)
	  return;
      }
      catch (...) {}
    }
    millisleep(500);
  }
}


void Dispatcher_Client::write_rollback()
{
  pid_t pid = getpid();

  send_message(Dispatcher::WRITE_ROLLBACK, "Dispatcher_Client::write_rollback::socket");

  while (true)
  {
    if (ack_arrived())
    {
      if (file_exists(shadow_name + ".lock"))
      {
        try
        {
	  pid_t locked_pid;
	  std::ifstream lock((shadow_name + ".lock").c_str());
	  lock>>locked_pid;
	  if (locked_pid != pid)
	    return;
        }
        catch (...) {}
      }
      else
        return;
    }

    millisleep(500);
  }
}


void Dispatcher_Client::write_commit()
{
  pid_t pid = getpid();

  send_message(Dispatcher::WRITE_COMMIT, "Dispatcher_Client::write_commit::socket");
  millisleep(200);

  while (true)
  {
    if (ack_arrived())
    {
      if (file_exists(shadow_name + ".lock"))
      {
        try
        {
	  pid_t locked_pid;
	  std::ifstream lock((shadow_name + ".lock").c_str());
	  lock>>locked_pid;
	  if (locked_pid != pid)
	    return;
        }
        catch (...) {}
      }
      else
        return;
    }

    send_message(Dispatcher::WRITE_COMMIT, "Dispatcher_Client::write_commit::socket");
    millisleep(200);
  }
}


void Dispatcher_Client::request_read_and_idx(uint32 max_allowed_time, uint64 max_allowed_space,
					     uint32 client_token)
{
  uint counter = 0;
  uint32 ack = 0;
  while (ack == 0 && ++counter <= 100)
  {
    struct req_read_and_idx_msg_t {
      uint32 msg;
      uint32 max_allowed_time;
      uint32 max_allowed_space_lower;
      uint32 max_allowed_space_upper;
      uint32 client_token;
    } req_read_and_idx_msg;

    static_assert(sizeof(req_read_and_idx_msg) == 20, "Alignment issue");

    req_read_and_idx_msg.msg = Dispatcher::REQUEST_READ_AND_IDX;
    req_read_and_idx_msg.max_allowed_time  = max_allowed_time;
    req_read_and_idx_msg.max_allowed_space_lower = (uint32) max_allowed_space;
    req_read_and_idx_msg.max_allowed_space_upper = max_allowed_space >> 32;
    req_read_and_idx_msg.client_token = client_token;

    send_message(req_read_and_idx_msg, "Dispatcher_Client::request_read_and_idx::socket::1");
    
    ack = ack_arrived();
    if (ack == Dispatcher::REQUEST_READ_AND_IDX)
      return;

    millisleep(300);
  }
  if (ack == Dispatcher::RATE_LIMITED)
    throw File_Error(0, dispatcher_share_name, "Dispatcher_Client::request_read_and_idx::rate_limited");
  else
    throw File_Error(0, dispatcher_share_name, "Dispatcher_Client::request_read_and_idx::timeout");
}


void Dispatcher_Client::read_idx_finished()
{
  uint counter = 0;
  while (++counter <= 300)
  {
    send_message(Dispatcher::READ_IDX_FINISHED, "Dispatcher_Client::read_idx_finished::socket");

    if (ack_arrived())
      return;
  }
  throw File_Error(0, dispatcher_share_name, "Dispatcher_Client::read_idx_finished::timeout");
}


void Dispatcher_Client::read_finished()
{
  uint counter = 0;
  while (++counter <= 300)
  {
    send_message(Dispatcher::READ_FINISHED, "Dispatcher_Client::read_finished::socket");

    if (ack_arrived())
      return;
  }
  throw File_Error(0, dispatcher_share_name, "Dispatcher_Client::read_finished::timeout");
}


void Dispatcher_Client::purge(uint32 pid)
{
  while (true)
  {
    struct purge_msg_t {
      uint32 msg;
      uint32 pid;
    } purge_msg;

    static_assert(sizeof(purge_msg) == 8, "Alignment issue");

    purge_msg.msg = Dispatcher::PURGE;
    purge_msg.pid = pid;

    send_message(purge_msg, "Dispatcher_Client::purge::socket::1");

    if (ack_arrived())
      return;
  }
}


pid_t Dispatcher_Client::query_by_token(uint32 token)
{
  struct query_token_msg_t {
    uint32 msg;
    uint32 token;
  } query_token_msg;

  static_assert(sizeof(query_token_msg) == 8, "Alignment issue");

  query_token_msg.msg = Dispatcher::QUERY_BY_TOKEN;
  query_token_msg.token = token;

  send_message(query_token_msg, "Dispatcher_Client::query_by_token::socket::1");
    
  return ack_arrived();
}


Client_Status Dispatcher_Client::query_my_status(uint32 token)
{
  struct query_my_status_msg_t {
    uint32 msg;
    uint32 token;
  } query_my_status_msg;

  static_assert(sizeof(query_my_status_msg) == 8, "Alignment issue");

  query_my_status_msg.msg = Dispatcher::QUERY_MY_STATUS;
  query_my_status_msg.token = token;

  send_message(query_my_status_msg, "Dispatcher_Client::query_my_status::socket::1");

  Client_Status result;
  result.rate_limit = ack_arrived();

  while (true)
  {
    Running_Query query;
    query.status = ack_arrived();
    if (query.status == 0)
      break;
    query.pid = ack_arrived();
    query.max_time = ack_arrived();
    query.max_space = ((uint64)ack_arrived() <<32) | ack_arrived();
    query.start_time = ack_arrived();
    result.queries.push_back(query);
  }

  while (true)
  {
    uint32 slot_start = ack_arrived();
    if (slot_start == 0)
      break;
    result.slot_starts.push_back(slot_start);
  }

  std::sort(result.slot_starts.begin(), result.slot_starts.end());

  return result;
}


void Dispatcher_Client::set_global_limits(uint64 max_allowed_space, uint64 max_allowed_time_units,
                                          int rate_limit)
{
  while (true)
  {
    struct global_limit_msg_t {
      uint32 msg;
      uint32 max_allowed_space_lower;
      uint32 max_allowed_space_upper;
      uint32 max_allowed_time_units_lower;
      uint32 max_allowed_time_units_upper;
      uint32 rate_limit;
    } global_limit_msg;

    static_assert(sizeof(global_limit_msg) == 24, "Alignment issue");

    global_limit_msg.msg = Dispatcher::SET_GLOBAL_LIMITS;
    global_limit_msg.max_allowed_space_lower  = (uint32) max_allowed_space;
    global_limit_msg.max_allowed_space_upper  = max_allowed_space >> 32;
    global_limit_msg.max_allowed_time_units_lower = (uint32) max_allowed_time_units;
    global_limit_msg.max_allowed_time_units_upper = max_allowed_time_units >> 32;
    global_limit_msg.rate_limit = rate_limit;

    send_message(global_limit_msg, "Dispatcher_Client::set_global_limits::1");
    
    if (ack_arrived())
      return;
  }
}


void Dispatcher_Client::ping()
{
// Ping-Feature removed. The concept of unassured messages doesn't fit in the context of strict
// two-directional communication.
//   send_message(Dispatcher::PING, "Dispatcher_Client::ping::socket");
}


void Dispatcher_Client::terminate()
{
  while (true)
  {
    send_message(Dispatcher::TERMINATE, "Dispatcher_Client::terminate::socket");

    if (ack_arrived())
      return;
  }
}


void Dispatcher_Client::output_status()
{
  while (true)
  {
    send_message(Dispatcher::OUTPUT_STATUS, "Dispatcher_Client::output_status::socket");

    if (ack_arrived())
      break;
  }

  std::ifstream status((shadow_name + ".status").c_str());
  std::string buffer;
  std::getline(status, buffer);
  while (status.good())
  {
    std::cout<<buffer<<'\n';
    std::getline(status, buffer);
  }
}
