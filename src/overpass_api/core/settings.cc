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

#include "datatypes.h"
#include "settings.h"

#ifndef GIT_VERSION
#define GIT_VERSION "(unknown)"
#endif

#include "../../template_db/file_blocks.h"
#include "../../template_db/file_blocks_index.h"
#include "../../template_db/random_file_index.h"

#include <cstdio>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <vector>
#include <sys/types.h>
#include <unistd.h>


template < typename TVal >
struct OSM_File_Properties final : public File_Properties
{
  OSM_File_Properties(std::string file_base_name_, uint32 block_size_,
		      uint32 map_block_size_)
    : file_base_name(std::move(file_base_name_)), block_size(block_size_), map_block_size(map_block_size_) {}

  const std::string& get_file_name_trunk() const override { return file_base_name; }

  const std::string& get_index_suffix() const override { return basic_settings().INDEX_SUFFIX; }
  const std::string& get_data_suffix() const override { return basic_settings().DATA_SUFFIX; }
  const std::string& get_id_suffix() const override { return basic_settings().ID_SUFFIX; }
  const std::string& get_shadow_suffix() const override { return basic_settings().SHADOW_SUFFIX; }

  uint32 get_block_size() const override { return block_size/8; }
  uint32 get_compression_factor() const override { return 8; }
  Block_Compression get_compression_method() const override { return basic_settings().compression_method; }
  uint32 get_map_block_size() const override { return map_block_size/8; }
  uint32 get_map_compression_factor() const override { return 8; }
  Block_Compression get_map_compression_method() const override { return basic_settings().map_compression_method; }

  std::vector< bool > get_data_footprint(const std::string& db_dir) const override
  {
    std::vector< bool > temp = get_data_index_footprint< TVal >(*this, db_dir);
    return temp;
  }

  std::vector< bool > get_map_footprint(const std::string& db_dir) const override
  {
    return get_map_index_footprint(*this, db_dir);
  }

  File_Blocks_Index_Base* new_data_index
      (bool writeable, bool use_shadow, const std::string& db_dir, const std::string& file_name_extension)
      const override
  {
    return new File_Blocks_Index< TVal >
        (*this, writeable, use_shadow, db_dir, file_name_extension);
  }

  const std::string file_base_name;
  const uint32 block_size;
  const uint32 map_block_size;
};


//-----------------------------------------------------------------------------

Basic_Settings::Basic_Settings()
:
  DATA_SUFFIX(".bin"),
  INDEX_SUFFIX(".idx"),
  ID_SUFFIX(".map"),
  SHADOW_SUFFIX(".shadow"),

  base_directory("./"),
  logfile_name("transactions.log"),
  shared_name_base("/osm3s_v0.7.59_mmd"),
  shared_name_suffix(get_shared_name_suffix()),
#ifndef PACKAGE_VERSION
  version("0.7.59_mmd"),
#else
  version(PACKAGE_VERSION),
#endif
  source_hash(GIT_VERSION),
#ifdef HAVE_LZ4
  compression_method(Block_Compression::LZ4_COMPRESSION),
  map_compression_method(Block_Compression::LZ4_COMPRESSION)
#else
  compression_method(Block_Compression::ZLIB_COMPRESSION),
  map_compression_method(Block_Compression::NO_COMPRESSION)
#endif

{}

inline std::string Basic_Settings::get_shared_name_suffix() {

  const char *suffix_c = std::getenv("OVERPASS_SHARED_NAME_SUFFIX");
  if (suffix_c != nullptr)
    return "_" + std::string(suffix_c);

  return "";
}


Basic_Settings all_settings::basic_settings = { };


//-----------------------------------------------------------------------------

Osm_Base_Settings::Osm_Base_Settings()
:
  NODES(new OSM_File_Properties< Uint32_Index >("nodes", 128*1024, 256*1024)),
  NODE_TAGS_LOCAL(new OSM_File_Properties< Tag_Index_Local >
      ("node_tags_local", 128*1024, 0)),
  NODE_TAGS_GLOBAL(new OSM_File_Properties< Tag_Index_Global >
      ("node_tags_global", 128*1024, 0)),
  NODE_KEYS(new OSM_File_Properties< Uint32_Index >
      ("node_keys", 512*1024, 0)),
  NODES_TAGGED(new OSM_File_Properties< Uint32_Index >("nodes_tagged", 128*1024, 0)),

  WAYS(new OSM_File_Properties< Uint31_Index >("ways", 128*1024, 256*1024)),
  WAY_TAGS_LOCAL(new OSM_File_Properties< Tag_Index_Local >
      ("way_tags_local", 128*1024, 0)),
  WAY_TAGS_GLOBAL(new OSM_File_Properties< Tag_Index_Global >
      ("way_tags_global", 128*1024, 0)),
  WAY_KEYS(new OSM_File_Properties< Uint32_Index >
      ("way_keys", 512*1024, 0)),

  RELATIONS(new OSM_File_Properties< Uint31_Index >("relations", 512*1024, 256*1024)),
  RELATION_ROLES(new OSM_File_Properties< Uint32_Index >
      ("relation_roles", 512*1024, 0)),
  RELATION_TAGS_LOCAL(new OSM_File_Properties< Tag_Index_Local >
      ("relation_tags_local", 128*1024, 0)),
  RELATION_TAGS_GLOBAL(new OSM_File_Properties< Tag_Index_Global >
      ("relation_tags_global", 128*1024, 0)),
  RELATION_KEYS(new OSM_File_Properties< Uint32_Index >
      ("relation_keys", 512*1024, 0)),

  shared_name(basic_settings().shared_name_base + "_osm_base" + basic_settings().shared_name_suffix),
  max_num_processes(20),
  purge_timeout(900),
  total_available_space(12ll*1024*1024*1024),
  total_available_time_units(256*1024)
{}

Osm_Base_Settings::~Osm_Base_Settings()
{
  delete NODES;
  delete NODE_TAGS_LOCAL;
  delete NODE_TAGS_GLOBAL;
  delete NODE_KEYS;
  delete NODES_TAGGED;
  delete WAYS;
  delete WAY_TAGS_LOCAL;
  delete WAY_TAGS_GLOBAL;
  delete WAY_KEYS;
  delete RELATIONS;
  delete RELATION_ROLES;
  delete RELATION_TAGS_LOCAL;
  delete RELATION_TAGS_GLOBAL;
  delete RELATION_KEYS;
}

const Osm_Base_Settings all_settings::osm_base_settings = { };

//-----------------------------------------------------------------------------

Area_Settings::Area_Settings()
:
  AREA_BLOCKS(new OSM_File_Properties< Uint31_Index >
      ("area_blocks", 512*1024, 64*1024)),
  AREAS(new OSM_File_Properties< Uint31_Index >("areas", 2*1024*1024, 256*1024)),
  AREA_TAGS_LOCAL(new OSM_File_Properties< Tag_Index_Local >
      ("area_tags_local", 256*1024, 0)),
  AREA_TAGS_GLOBAL(new OSM_File_Properties< Tag_Index_Global >
      ("area_tags_global", 512*1024, 0)),

  shared_name(basic_settings().shared_name_base + "_areas"  + basic_settings().shared_name_suffix),
  max_num_processes(5),
  purge_timeout(900),
  total_available_space(4ll*1024*1024*1024),
  total_available_time_units(256*1024)
{}

Area_Settings::~Area_Settings()
{
  delete AREA_BLOCKS;
  delete AREAS;
  delete AREA_TAGS_LOCAL;
  delete AREA_TAGS_GLOBAL;
}

const Area_Settings all_settings::area_settings = { };

//-----------------------------------------------------------------------------

Meta_Settings::Meta_Settings()
:
  USER_DATA(new OSM_File_Properties< Uint32_Index >
      ("user_data", 512*1024, 0)),
  USER_INDICES(new OSM_File_Properties< Uint32_Index >
      ("user_indices", 128*1024, 0)),
  NODES_META(new OSM_File_Properties< Uint31_Index >
      ("nodes_meta", 128*1024, 0)),
  WAYS_META(new OSM_File_Properties< Uint31_Index >
      ("ways_meta", 128*1024, 0)),
  RELATIONS_META(new OSM_File_Properties< Uint31_Index >
      ("relations_meta", 128*1024, 0)),
  idxs_{ USER_DATA, USER_INDICES,  NODES_META, WAYS_META, RELATIONS_META }
{
}


Meta_Settings::~Meta_Settings()
{
  for(auto & e : idxs_)
    delete e;
}

const std::vector< File_Properties* >& Meta_Settings::idxs() const
{
  return idxs_;
}

const Meta_Settings all_settings::meta_settings = { };

//-----------------------------------------------------------------------------

Attic_Settings::Attic_Settings()
:
  NODES(new OSM_File_Properties< Uint31_Index >("nodes_attic", 128*1024, 256*1024)),
  NODES_UNDELETED(new OSM_File_Properties< Uint31_Index >("nodes_attic_undeleted", 128*1024, 64*1024)),
  NODE_IDX_LIST(new OSM_File_Properties< Node::Id_Type >
      ("node_attic_indexes", 128*1024, 0)),
  NODE_TAGS_LOCAL(new OSM_File_Properties< Tag_Index_Local >
      ("node_tags_local_attic", 128*1024, 0)),
  NODE_TAGS_GLOBAL(new OSM_File_Properties< Tag_Index_Global >
      ("node_tags_global_attic", 512*1024, 0)),
  NODES_META(new OSM_File_Properties< Uint31_Index >
      ("nodes_meta_attic", 128*1024, 0)),
  NODE_CHANGELOG(new OSM_File_Properties< Timestamp >
      ("node_changelog", 128*1024, 0)),
  NODE_CHANGEPACK(new OSM_File_Properties< Timestamp >
      ("node_changepack", 128*1024, 0)),

  WAYS(new OSM_File_Properties< Uint31_Index >("ways_attic", 128*1024, 256*1024)),
  WAYS_UNDELETED(new OSM_File_Properties< Uint31_Index >("ways_attic_undeleted", 128*1024, 64*1024)),
  WAY_IDX_LIST(new OSM_File_Properties< Way::Id_Type >
      ("way_attic_indexes", 128*1024, 0)),
  WAY_TAGS_LOCAL(new OSM_File_Properties< Tag_Index_Local >
      ("way_tags_local_attic", 128*1024, 0)),
  WAY_TAGS_GLOBAL(new OSM_File_Properties< Tag_Index_Global >
      ("way_tags_global_attic", 512*1024, 0)),
  WAYS_META(new OSM_File_Properties< Uint31_Index >
      ("ways_meta_attic", 128*1024, 0)),
  WAY_CHANGELOG(new OSM_File_Properties< Timestamp >
      ("way_changelog", 128*1024, 0)),

  RELATIONS(new OSM_File_Properties< Uint31_Index >("relations_attic", 512*1024, 256*1024)),
  RELATIONS_UNDELETED(new OSM_File_Properties< Uint31_Index >("relations_attic_undeleted", 128*1024, 64*1024)),
  RELATION_IDX_LIST(new OSM_File_Properties< Relation::Id_Type >
      ("relation_attic_indexes", 128*1024, 0)),
  RELATION_TAGS_LOCAL(new OSM_File_Properties< Tag_Index_Local >
      ("relation_tags_local_attic", 128*1024, 0)),
  RELATION_TAGS_GLOBAL(new OSM_File_Properties< Tag_Index_Global >
      ("relation_tags_global_attic", 512*1024, 0)),
  RELATIONS_META(new OSM_File_Properties< Uint31_Index >
      ("relations_meta_attic", 128*1024, 0)),
  RELATION_CHANGELOG(new OSM_File_Properties< Timestamp >
      ("relation_changelog", 128*1024, 0)),
  idxs_{NODES,     NODES_UNDELETED,     NODE_IDX_LIST,     NODE_TAGS_LOCAL,     NODE_TAGS_GLOBAL,     NODES_META,     NODE_CHANGELOG,
        WAYS,      WAYS_UNDELETED,      WAY_IDX_LIST,      WAY_TAGS_LOCAL,      WAY_TAGS_GLOBAL,      WAYS_META,      WAY_CHANGELOG,
        RELATIONS, RELATIONS_UNDELETED, RELATION_IDX_LIST, RELATION_TAGS_LOCAL, RELATION_TAGS_GLOBAL, RELATIONS_META, RELATION_CHANGELOG,
        NODE_CHANGEPACK
        }
{ }


Attic_Settings::~Attic_Settings()
{
  for(auto & e : idxs_)
    delete e;
}

const std::vector< File_Properties* >& Attic_Settings::idxs() const
{
  return idxs_;
}

const Attic_Settings all_settings::attic_settings = { };

//-----------------------------------------------------------------------------

void show_mem_status()
{
  std::ostringstream proc_file_name_("");
  proc_file_name_<<"/proc/"<<getpid()<<"/stat";
  std::ifstream stat(proc_file_name_.str().c_str());
  while (stat.good())
  {
    std::string line;
    getline(stat, line);
    std::cerr<<line;
  }
  std::cerr<<'\n';
}


//-----------------------------------------------------------------------------

Logger::LEVEL Logger::int_to_level(int level)
{
  switch (level) {
  case 0:
    return LEVEL::error;
   case 1:
    return LEVEL::warn;
  case 2:
    return LEVEL::info;
  case 3:
    return LEVEL::debug;
  case 4:
    return LEVEL::trace;
  default:
    return LEVEL::debug;
  }
}

bool Logger::enabled(LEVEL l) {
  return (l <= log_level);
}

Logger::LEVEL Logger::get_env_log_level()
{
  const char *loglevel_c = std::getenv("OVERPASS_LOG_LEVEL");
  if (loglevel_c == nullptr)
    return Logger::LEVEL::debug;

  int level = atoi(loglevel_c);
  return int_to_level(level);
}

Logger::Logger(const std::string& db_dir)
  : logfile_full_name(db_dir + basic_settings().logfile_name)
{

}

void Logger::annotated_log(LEVEL l, const std::string& message)
{
  if (enabled(l))
    annotated_log(message);
}

void Logger::annotated_log(const std::string& message)
{
  // Collect current time in a user-readable form.
  time_t time_t_ = time(nullptr);
  struct tm* tm_ = gmtime(&time_t_);
  char strftime_buf[21];
  strftime_buf[0] = 0;
  if (tm_)
    strftime(strftime_buf, 21, "%F %H:%M:%S ", tm_);

  std::ofstream out(logfile_full_name.c_str(), std::ios_base::app);
  out<<strftime_buf<<'['<<getpid()<<"] "<<message<<'\n';
}

void Logger::raw_log(LEVEL l, const std::string& message)
{
  if (enabled(l))
    raw_log(message);
}

void Logger::raw_log(const std::string& message)
{
  std::ofstream out(logfile_full_name.c_str(), std::ios_base::app);
  out<<message<<'\n';
}

const std::string& get_logfile_name()
{
  return basic_settings().logfile_name;
}

const Logger::LEVEL Logger::log_level = Logger::get_env_log_level();

const timestamp_t NOW = std::numeric_limits< timestamp_t >::max();

const std::string void_tag::void_tag_value =  { (char) 0xff };
const std::string void_tag::void_tag_value_space = { (char) 0xff, (char) 0x20 };
