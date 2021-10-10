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

#ifndef DE__OSM3S___OVERPASS_API__CORE__SETTINGS_H
#define DE__OSM3S___OVERPASS_API__CORE__SETTINGS_H

#include <limits>
#include <string>

#include "../../template_db/types.h"


struct Basic_Settings
{
  const std::string DATA_SUFFIX;
  const std::string INDEX_SUFFIX;
  const std::string ID_SUFFIX;
  const std::string SHADOW_SUFFIX;

  const std::string base_directory;
  const std::string logfile_name;
  const std::string shared_name_base;
  const std::string shared_name_suffix;

  const std::string version;
  const std::string source_hash;

  uint32 compression_method;
  uint32 map_compression_method;

  Basic_Settings();

  static std::string get_shared_name_suffix();
};


struct Osm_Base_Settings
{
  File_Properties* const NODES;
  File_Properties* const NODE_TAGS_LOCAL;
  File_Properties* const NODE_TAGS_GLOBAL;
  File_Properties* const NODE_KEYS;
  File_Properties* const NODES_TAGGED;
  File_Properties* const WAYS;
  File_Properties* const WAY_TAGS_LOCAL;
  File_Properties* const WAY_TAGS_GLOBAL;
  File_Properties* const WAY_KEYS;
  File_Properties* const RELATIONS;
  File_Properties* const RELATION_ROLES;
  File_Properties* const RELATION_TAGS_LOCAL;
  File_Properties* const RELATION_TAGS_GLOBAL;
  File_Properties* const RELATION_KEYS;

  const std::string shared_name;
  const uint max_num_processes;
  const uint purge_timeout;
  const uint64 total_available_space;
  const uint64 total_available_time_units;

  Osm_Base_Settings();
  ~Osm_Base_Settings();

  Osm_Base_Settings(const Osm_Base_Settings&) = delete;
  Osm_Base_Settings& operator=(const Osm_Base_Settings& a) = delete;
};


struct Area_Settings
{
  File_Properties* const AREA_BLOCKS;
  File_Properties* const AREAS;
  File_Properties* const AREA_TAGS_LOCAL;
  File_Properties* const AREA_TAGS_GLOBAL;

  const std::string shared_name;
  const uint max_num_processes;
  const uint purge_timeout;
  const uint64 total_available_space;
  const uint64 total_available_time_units;

  Area_Settings();
  ~Area_Settings();
  Area_Settings(const Area_Settings&) = delete;
  Area_Settings& operator=(const Area_Settings& a) = delete;
};


struct Meta_Settings
{
  File_Properties* const USER_DATA;
  File_Properties* const USER_INDICES;
  File_Properties* const NODES_META;
  File_Properties* const WAYS_META;
  File_Properties* const RELATIONS_META;

  Meta_Settings();
  ~Meta_Settings();
  Meta_Settings(const Meta_Settings&) = delete;
  Meta_Settings& operator=(const Meta_Settings& a) = delete;

  const std::vector< File_Properties* >& idxs() const;

private:
  std::vector< File_Properties* > const idxs_;
};


struct Attic_Settings
{
  File_Properties* const NODES;
  File_Properties* const NODES_UNDELETED;
  File_Properties* const NODE_IDX_LIST;
  File_Properties* const NODE_TAGS_LOCAL;
  File_Properties* const NODE_TAGS_GLOBAL;
  File_Properties* const NODES_META;
  File_Properties* const NODE_CHANGELOG;
  File_Properties* const WAYS;
  File_Properties* const WAYS_UNDELETED;
  File_Properties* const WAY_IDX_LIST;
  File_Properties* const WAY_TAGS_LOCAL;
  File_Properties* const WAY_TAGS_GLOBAL;
  File_Properties* const WAYS_META;
  File_Properties* const WAY_CHANGELOG;
  File_Properties* const RELATIONS;
  File_Properties* const RELATIONS_UNDELETED;
  File_Properties* const RELATION_IDX_LIST;
  File_Properties* const RELATION_TAGS_LOCAL;
  File_Properties* const RELATION_TAGS_GLOBAL;
  File_Properties* const RELATIONS_META;
  File_Properties* const RELATION_CHANGELOG;

  Attic_Settings();
  ~Attic_Settings();
  Attic_Settings(const Attic_Settings&) = delete;
  Attic_Settings& operator=(const Attic_Settings& a) = delete;

  const std::vector< File_Properties* >& idxs() const;

private:
  std::vector< File_Properties* > const idxs_;
};


struct Clone_Settings
{
  uint32 compression_method;
  uint32 map_compression_method;
  uint32 parallel_processes;

  Clone_Settings()
      : compression_method(File_Blocks_Index_Base::USE_DEFAULT),
      map_compression_method(File_Blocks_Index_Base::USE_DEFAULT),
      parallel_processes(1) {}
};


class all_settings {

public:
   static Basic_Settings basic_settings;
   const static Osm_Base_Settings osm_base_settings;
   const static Attic_Settings attic_settings;
   const static Meta_Settings meta_settings;
   const static Area_Settings area_settings;

};

inline Basic_Settings& basic_settings()
{
  return all_settings::basic_settings;
}

inline const Osm_Base_Settings& osm_base_settings()
{
  return all_settings::osm_base_settings;
}

inline const Attic_Settings& attic_settings()
{
  return all_settings::attic_settings;
}

inline const Meta_Settings& meta_settings()
{
  return all_settings::meta_settings;
}

inline const Area_Settings& area_settings()
{
  return all_settings::area_settings;
}


void show_mem_status();


class Logger
{
  public:
    enum LEVEL {
      error,
      warn,
      info,
      debug,
      trace
    };

    Logger(const std::string& db_dir);
    void annotated_log(const std::string& message);
    void annotated_log(LEVEL l, const std::string& message);
    void raw_log(const std::string& message);
    void raw_log(LEVEL l, const std::string& message);
    bool enabled(LEVEL l);

  private:
    static LEVEL int_to_level(int level);
    static LEVEL get_env_log_level();

    std::string logfile_full_name;
    static const LEVEL log_level;
};


extern const uint64 NOW;


#endif
