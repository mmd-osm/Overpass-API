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

#include <iostream>
#include <string>

#include "output.h"


class Verbose_Osm_Backend_Callback : public Osm_Backend_Callback
{
  public:
    void update_started() override { std::cerr<<"Flushing to database ."; }
    void compute_indexes_finished() override { std::cerr<<'.'; }
    void update_ids_finished() override { std::cerr<<'.'; }
    void update_coords_finished() override { std::cerr<<'.'; }
    void prepare_delete_tags_finished() override { std::cerr<<'.'; }
    void tags_local_finished() override { std::cerr<<'.'; }
    void tags_global_finished() override { std::cerr<<'.'; }
    void flush_roles_finished() override { std::cerr<<'.'; }
    void update_finished() override { std::cerr<<" done.\n"; }
    void partial_started() override { std::cerr<<"Reorganizing the database ..."; }
    void partial_finished() override { std::cerr<<" done.\n"; }

    void parser_started() override { std::cerr<<"Reading XML file ..."; }
    void node_elapsed(Node::Id_Type id) override { std::cerr<<" elapsed node "<<id.val()<<". "; }
    void nodes_finished() override { std::cerr<<" finished reading nodes. "; }
    void way_elapsed(Way::Id_Type id) override { std::cerr<<" elapsed way "<<id.val()<<". "; }
    void ways_finished() override { std::cerr<<" finished reading ways. "; }
    void relation_elapsed(Relation::Id_Type id) override { std::cerr<<" elapsed relation "<<id.val()<<". "; }
    void relations_finished() override { std::cerr<<" finished reading relations. "; }

    void parser_succeeded() override { std::cerr<<"Update complete.\n"; }

    ~Verbose_Osm_Backend_Callback() override = default;
};


Osm_Backend_Callback* get_verbatim_callback()
{
  return new Verbose_Osm_Backend_Callback;
}


class Quiet_Osm_Backend_Callback : public Osm_Backend_Callback
{
  public:
    void update_started() override {}
    void compute_indexes_finished() override {}
    void update_ids_finished() override {}
    void update_coords_finished() override {}
    void prepare_delete_tags_finished() override {}
    void tags_local_finished() override {}
    void tags_global_finished() override {}
    void flush_roles_finished() override {}
    void update_finished() override {}
    void partial_started() override {}
    void partial_finished() override {}

    void parser_started() override {}
    void node_elapsed(Node::Id_Type id) override {}
    void nodes_finished() override {}
    void way_elapsed(Way::Id_Type id) override {}
    void ways_finished() override {}
    void relation_elapsed(Relation::Id_Type id) override {}
    void relations_finished() override {}

    void parser_succeeded() override {}

    ~Quiet_Osm_Backend_Callback() override = default;
};


Osm_Backend_Callback* get_quiet_callback()
{
  return new Quiet_Osm_Backend_Callback;
}


void report_file_error(const File_Error& e)
{
  if (e.error_number)
    std::cerr<<"File error caught: "
        <<e.error_number<<' '<<strerror(e.error_number)<<' '<<e.filename<<' '<<e.origin<<'\n';
  else
    std::cerr<<"File error caught: "<<e.filename<<' '<<e.origin<<'\n';
}
