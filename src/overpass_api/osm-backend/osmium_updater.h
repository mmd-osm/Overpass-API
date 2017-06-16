/** Copyright 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016 Roland Olbricht et al.
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

#ifndef DE__OSM3S___OVERPASS_API__OSM_BACKEND__OSMIUM_UPDATER_H
#define DE__OSM3S___OVERPASS_API__OSM_BACKEND__OSMIUM_UPDATER_H

#include <dirent.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>

#include "node_updater.h"
#include "relation_updater.h"
#include "way_updater.h"

using namespace std;

class Dispatcher_Client;

class Osmium_Updater
{
  public:
    Osmium_Updater(Osm_Backend_Callback* callback_, const string& data_version,
		meta_modes meta, unsigned int flush_limit, unsigned int parallel_processes);
    Osmium_Updater(Osm_Backend_Callback* callback_, string db_dir, const string& data_version,
		meta_modes meta, unsigned int flush_limit, unsigned int parallel_processes);
    ~Osmium_Updater();

    void finish_updater();
    void parse_file_completely(FILE* in);
    
  private:
    Nonsynced_Transaction* transaction;
    Dispatcher_Client* dispatcher_client;
    Node_Updater* node_updater_;
    Way_Updater* way_updater_;
    Relation_Updater* relation_updater_;
    Osm_Backend_Callback* callback_;
    uint flush_limit = 4*1024*1024;
    string db_dir_;
    meta_modes meta;
    unsigned int parallel_processes;
    Cpu_Stopwatch* cpu_stopwatch;

    void flush();
};

#endif
