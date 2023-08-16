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

#include <filesystem>
#include <sys/socket.h>
#include <sys/un.h>
#include "types.h"

#include <cstring>
#include <memory>
#include <thread>

namespace fs = std::filesystem;

void copy_file(const std::string& source, const std::string& dest)
{
  std::error_code ec;

  try {
    fs::copy_file(source, dest, {}, ec);
  } catch(fs::filesystem_error& e) {
    throw File_Error(ec.value(), source, "Dispatcher::1");
  }
}

void rename_file(const std::string& source, const std::string& dest)
{
  if (!file_exists(source))
    return;

  int result = std::rename(source.c_str(), dest.c_str());
  if (result != 0) {
     throw File_Error(errno, source,  "Dispatcher:7");
  }
}


int& global_read_counter()
{
  static int counter = 0;
  return counter;
}

bool& fastcgi_enabled()
{
  static bool enabled = false;
  return enabled;
}


void millisleep(uint32 milliseconds)
{
  std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds));
}


Unix_Socket::Unix_Socket(const std::string& socket_name, uint max_num_reading_processes_)
  : socket_descriptor(-1), max_num_reading_processes(max_num_reading_processes_)
{
  if (!socket_name.empty())
    open(socket_name);
}


void Unix_Socket::open(const std::string& socket_name)
{
  socket_descriptor = socket(AF_UNIX, SOCK_STREAM, 0);
  if (socket_descriptor == -1)
    throw File_Error
        (errno, socket_name, "Unix_Socket::1");

  if (max_num_reading_processes > 0)
  {
    if (fcntl(socket_descriptor, F_SETFL, O_RDWR|O_NONBLOCK) == -1)
      throw File_Error
          (errno, socket_name, "Unix_Socket::2");
  }

  struct sockaddr_un local;
  local.sun_family = AF_UNIX;
  if (socket_name.size() < sizeof local.sun_path - 1)
    strcpy(local.sun_path, socket_name.c_str());
  else
    throw File_Error(0, socket_name, "Unix_Socket::3");
#ifdef __APPLE__
  local.sun_len = socket_name.size() + 1;
#endif

  if (max_num_reading_processes > 0)
  {
    if (bind(socket_descriptor, (struct sockaddr*)&local,
        sizeof(struct sockaddr_un)) == -1)
      throw File_Error(errno, socket_name, "Unix_Socket::4");
    if (chmod(socket_name.c_str(), S_666) == -1)
      throw File_Error(errno, socket_name, "Unix_Socket::5");
    if (listen(socket_descriptor, max_num_reading_processes) == -1)
      throw File_Error(errno, socket_name, "Unix_Socket::6");
  }
  else
    if (connect(socket_descriptor, (struct sockaddr*)&local,
        sizeof(struct sockaddr_un)) == -1)
      throw File_Error
          (errno, socket_name, "Unix_Socket::7");
}


Unix_Socket::~Unix_Socket()
{
  if (socket_descriptor != -1)
    close(socket_descriptor);
}
