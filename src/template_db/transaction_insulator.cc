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

#include "dispatcher.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <vector>


void Idx_Footprints::set_current_footprint(std::vector< bool > && footprint)
{
  current_footprint = std::make_shared<std::vector< bool > >(std::move(footprint));
}


void Idx_Footprints::register_pid(pid_t pid)
{
  footprint_per_pid[pid] = current_footprint;
}


void Idx_Footprints::unregister_pid(pid_t pid)
{
  footprint_per_pid.erase(pid);
}


std::vector< Idx_Footprints::pid_t > Idx_Footprints::registered_processes() const
{
  std::vector< pid_t > result;
  for (const auto & [pid, _] : footprint_per_pid)
    result.push_back(pid);
  return result;
}


std::vector< bool > Idx_Footprints::total_footprint() const
{
  std::vector< bool > result(*current_footprint.get());
  for (auto it(footprint_per_pid.begin()); it != footprint_per_pid.end(); ++it)
  {
    // By construction, it->second.size() <= result.size()
    for (std::vector< bool >::size_type i = 0; i < it->second.get()->size(); ++i)
      result[i] = result[i] || (*it->second)[i];
  }
  return result;
}


Transaction_Insulator::Transaction_Insulator(
    const std::string& db_dir, const std::vector< File_Properties* >& controlled_files_)
    : db_dir_(db_dir), controlled_files(controlled_files_),
    data_footprints(controlled_files_.size()), map_footprints(controlled_files_.size())
{
  // get the absolute pathname of the current directory
  if (db_dir.substr(0, 1) != "/")
    db_dir_ = getcwd() + db_dir;
}


void Transaction_Insulator::request_read_and_idx(pid_t pid)
{
  for (auto it(data_footprints.begin());
      it != data_footprints.end(); ++it)
    it->register_pid(pid);
  for (auto it(map_footprints.begin());
      it != map_footprints.end(); ++it)
    it->register_pid(pid);
}


void Transaction_Insulator::read_finished(pid_t pid)
{
  for (auto it(data_footprints.begin());
      it != data_footprints.end(); ++it)
    it->unregister_pid(pid);
  for (auto it(map_footprints.begin());
      it != map_footprints.end(); ++it)
    it->unregister_pid(pid);
}


//void Transaction_Insulator::copy_shadows_to_mains()
//{
//  for (std::vector< File_Properties* >::const_iterator it(controlled_files.begin());
//      it != controlled_files.end(); ++it)
//  {
//      copy_file(db_dir() + (*it)->get_file_name_trunk() + (*it)->get_data_suffix()
//                + (*it)->get_index_suffix() + (*it)->get_shadow_suffix(),
//		db_dir() + (*it)->get_file_name_trunk() + (*it)->get_data_suffix()
//		+ (*it)->get_index_suffix());
//      copy_file(db_dir() + (*it)->get_file_name_trunk() + (*it)->get_id_suffix()
//                + (*it)->get_index_suffix() + (*it)->get_shadow_suffix(),
//		db_dir() + (*it)->get_file_name_trunk() + (*it)->get_id_suffix()
//		+ (*it)->get_index_suffix());
//  }
//}

void Transaction_Insulator::rename_shadows_to_mains()
{
  for (std::vector< File_Properties* >::const_iterator it(controlled_files.begin());
      it != controlled_files.end(); ++it)
  {
      rename_file(db_dir() + (*it)->get_file_name_trunk() + (*it)->get_data_suffix()
                + (*it)->get_index_suffix() + (*it)->get_shadow_suffix(),
                db_dir() + (*it)->get_file_name_trunk() + (*it)->get_data_suffix()
                + (*it)->get_index_suffix());
      rename_file(db_dir() + (*it)->get_file_name_trunk() + (*it)->get_id_suffix()
                + (*it)->get_index_suffix() + (*it)->get_shadow_suffix(),
                db_dir() + (*it)->get_file_name_trunk() + (*it)->get_id_suffix()
                + (*it)->get_index_suffix());
  }
}


void Transaction_Insulator::copy_mains_to_shadows()
{
  for (std::vector< File_Properties* >::const_iterator it(controlled_files.begin());
      it != controlled_files.end(); ++it)
  {
      copy_file(db_dir() + (*it)->get_file_name_trunk() + (*it)->get_data_suffix()
                + (*it)->get_index_suffix(),
		db_dir() + (*it)->get_file_name_trunk() + (*it)->get_data_suffix()
		+ (*it)->get_index_suffix() + (*it)->get_shadow_suffix());
      copy_file(db_dir() + (*it)->get_file_name_trunk() + (*it)->get_id_suffix()
                + (*it)->get_index_suffix(),
		db_dir() + (*it)->get_file_name_trunk() + (*it)->get_id_suffix()
		+ (*it)->get_index_suffix() + (*it)->get_shadow_suffix());
  }
}


void Transaction_Insulator::remove_shadows()
{
  for (std::vector< File_Properties* >::const_iterator it(controlled_files.begin());
      it != controlled_files.end(); ++it)
  {
    remove((db_dir() + (*it)->get_file_name_trunk() + (*it)->get_data_suffix()
            + (*it)->get_index_suffix() + (*it)->get_shadow_suffix()).c_str());
    remove((db_dir() + (*it)->get_file_name_trunk() + (*it)->get_id_suffix()
            + (*it)->get_index_suffix() + (*it)->get_shadow_suffix()).c_str());
    remove((db_dir() + (*it)->get_file_name_trunk() + (*it)->get_data_suffix()
            + (*it)->get_shadow_suffix()).c_str());
    remove((db_dir() + (*it)->get_file_name_trunk() + (*it)->get_id_suffix()
            + (*it)->get_shadow_suffix()).c_str());
  }
}


void Transaction_Insulator::set_current_footprints()
{
  for (std::vector< File_Properties* >::size_type i = 0;
      i < controlled_files.size(); ++i)
  {
    try
    {
      data_footprints[i].set_current_footprint
          (controlled_files[i]->get_data_footprint(db_dir()));
    }
    catch (File_Error &e)
    {
      std::cerr<<"File_Error "<<e.error_number<<' '<<strerror(e.error_number)<<' '<<e.filename<<' '<<e.origin<<'\n';
    }
    catch (...) {}

    try
    {
      map_footprints[i].set_current_footprint
          (controlled_files[i]->get_map_footprint(db_dir()));
    }
    catch (File_Error &e)
    {
      std::cerr<<"File_Error "<<e.error_number<<' '<<strerror(e.error_number)<<' '<<e.filename<<' '<<e.origin<<'\n';
    }
    catch (...) {}
  }
}


std::set< pid_t > Transaction_Insulator::registered_pids() const
{
  std::set< pid_t > registered;

  for (auto it(data_footprints.begin());
      it != data_footprints.end(); ++it)
  {
    std::vector< Idx_Footprints::pid_t > registered_processes = it->registered_processes();
    for (std::vector< Idx_Footprints::pid_t >::const_iterator it = registered_processes.begin();
        it != registered_processes.end(); ++it)
      registered.insert(*it);
  }
  for (auto it(map_footprints.begin());
      it != map_footprints.end(); ++it)
  {
    std::vector< Idx_Footprints::pid_t > registered_processes = it->registered_processes();
    for (std::vector< Idx_Footprints::pid_t >::const_iterator it = registered_processes.begin();
        it != registered_processes.end(); ++it)
      registered.insert(*it);
  }

  return registered;
}

namespace {

// Calculate space efficient format for "false" entries in footprint
//
// see get_data_index_footprint:
// - start with footprint set to true
// - for each entry in the footprint vector, set "pair.first" elements to false, starting at offset "pair.second"

std::vector< std::pair< uint32, uint32 > > condense_footprint(
    const std::vector< bool > &footprint)
{
  std::vector< std::pair< uint32, uint32 > > buffer;
  buffer.reserve(footprint.size());
  uint32 last_start = 0;
  uint32 i = 0;
  for (bool ft : footprint)
  {
    if (ft)
    {
      if (last_start < i)
      {
        buffer.emplace_back(i - last_start, last_start);
      }
      last_start = i + 1;
    }
    ++i;
  }
  if (last_start < footprint.size())
  {
    buffer.emplace_back(footprint.size() - last_start, last_start);
  }
  return buffer;
}

void write_to_index_empty_file_data(const std::vector< bool >& footprint, const std::string& filename)
{
  auto buffer = condense_footprint(footprint);

  Raw_File file(filename, O_RDWR|O_CREAT|O_TRUNC,
		S_666, "write_to_index_empty_file_data:1");
  file.write((uint8*)buffer.data(), buffer.size() * 8, "Dispatcher:26");
}


void write_to_index_empty_file_ids(const std::vector< bool >& footprint, const std::string& filename)
{
  auto buffer = condense_footprint(footprint);

  Raw_File file(filename, O_RDWR|O_CREAT|O_TRUNC,
		S_666, "write_to_index_empty_file_ids:1");
  file.write((uint8*)buffer.data(), buffer.size() * 8, "Dispatcher:36");
}

}

void Transaction_Insulator::write_index_of_empty_blocks()
{
  for (std::vector< File_Properties* >::size_type i = 0;
      i < controlled_files.size(); ++i)
  {
    if (file_exists(db_dir() + controlled_files[i]->get_file_name_trunk()
        + controlled_files[i]->get_data_suffix()
	+ controlled_files[i]->get_index_suffix()
	+ controlled_files[i]->get_shadow_suffix()))
    {
      write_to_index_empty_file_data
          (data_footprints[i].total_footprint(),
	   db_dir() + controlled_files[i]->get_file_name_trunk()
	   + controlled_files[i]->get_data_suffix()
	   + controlled_files[i]->get_shadow_suffix());
    }
    if (file_exists(db_dir() + controlled_files[i]->get_file_name_trunk()
        + controlled_files[i]->get_id_suffix()
	+ controlled_files[i]->get_index_suffix()
	+ controlled_files[i]->get_shadow_suffix()))
    {
      write_to_index_empty_file_ids
          (map_footprints[i].total_footprint(),
	   db_dir() + controlled_files[i]->get_file_name_trunk()
	   + controlled_files[i]->get_id_suffix()
	   + controlled_files[i]->get_shadow_suffix());
    }
  }
}
