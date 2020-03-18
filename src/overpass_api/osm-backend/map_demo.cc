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

#include <bits/exception.h>
#include <osmium/index/id_set.hpp>
#include <cstring>
#include <iostream>
#include <iterator>
#include <map>
#include <regex>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "../../template_db/block_backend.h"
#include "../../template_db/transaction.h"
#include "../../template_db/types.h"
#include "../core/basic_types.h"
#include "../core/datatypes.h"
#include "../core/geometry.h"
#include "../core/index_computations.h"
#include "../core/parsed_query.h"
#include "../core/settings.h"
#include "../core/type_node.h"
#include "../core/type_relation.h"
#include "../core/type_way.h"
#include "../data/meta_collector.h"
#include "../data/tag_store.h"
#include "../dispatch/dispatcher_stub.h"
#include "../dispatch/resource_manager.h"
#include "../dispatch/scripting_core.h"
#include "../frontend/console_output.h"
#include "../frontend/web_output.h"
#include "../output_formats/output_xml.h"
#include "../statements/osm_script.h"
#include "../statements/statement.h"


osmium::index::IdSetDense<Node_Skeleton::Id_Type::Id_Type>     nodes_dense;
osmium::index::IdSetDense<Way_Skeleton::Id_Type::Id_Type>      ways_dense;
osmium::index::IdSetDense<Relation_Skeleton::Id_Type::Id_Type> relations_dense;

std::vector < std::pair<Node_Skeleton::Id_Type::Id_Type, Node_Skeleton::Id_Type::Id_Type>> node_pairs;
std::vector < std::pair<Way_Skeleton::Id_Type::Id_Type, Way_Skeleton::Id_Type::Id_Type>> way_pairs;
std::vector < std::pair<Relation_Skeleton::Id_Type::Id_Type, Relation_Skeleton::Id_Type::Id_Type>> rel_pairs;

std::set< Uint31_Index > req;
std::set< Uint32_Index > node_idx_outside_bbox;

//Bbox_Double bbox(49.8,-14.2,59.5,3.3);  // UK+IRL
//Bbox_Double bbox(17.24,-67.88,19.11,-64.98); // Puerto Rico
//Bbox_Double bbox(47.2587,-3.3134,47.4037,-3.0267); // belle-ile




void prep_map_data(Resource_Manager& rman, Bbox_Double bbox)
{

  
  try
  {    
    // based on map query: "(node(bbox);way(bn);node(w););(._;(rel(bn)->.a;rel(bw)->.a;);rel(br););out meta;"

    std::vector< uint32 > node_idxs;

    {
      // Ranges for bbox
      auto ranges = ::get_ranges_32(bbox.south, bbox.north, bbox.west, bbox.east);

      // Nodes inside bbox:     node( {{bbox}} )

      uint32 south = ilat_(bbox.south);
      uint32 north = ilat_(bbox.north);
      int32 west = ilon_(bbox.west);
      int32 east = ilon_(bbox.east);

      Uint32_Index previous_node_idx{};

      Block_Backend< Uint32_Index, Node_Skeleton > db (rman.get_transaction()->data_index(osm_base_settings().NODES));
      for (auto it(db.range_begin(ranges.begin(), ranges.end()));
          !(it == db.range_end()); ++it)
      {
        auto n = it.object();

        uint32 lat(::ilat(it.index().val(), n.ll_lower));
        int32 lon(::ilon(it.index().val(), n.ll_lower));
        if ((lat >= south) && (lat <= north) &&
            (((lon >= west) && (lon <= east))
                || ((east < west) && ((lon >= west) || (lon <= east)))))
        {
          nodes_dense.set(n.id.val());
          if (!(it.index() == previous_node_idx)) {
            node_idxs.push_back(it.index().val());
            previous_node_idx = it.index();
          }
        }
      }
    }

    // Recurse nodes to ways:   way(bn)

    {
      std::vector< uint32 > parents = calc_parents(node_idxs);
      for (std::vector< uint32 >::const_iterator it = parents.begin(); it != parents.end(); ++it)
        req.insert(Uint31_Index(*it));
    }

    // Fetch ways for nodes

    {
      Block_Backend< Uint31_Index, Way_Skeleton > db (rman.get_transaction()->data_index(osm_base_settings().WAYS));

      for (typename Block_Backend< Uint31_Index, Way_Skeleton, std::set< Uint31_Index >::const_iterator >::Discrete_Iterator
          it(db.discrete_begin(req.begin(), req.end())); !(it == db.discrete_end()); ++it)
      {

        auto w = it.object();
        for (const auto& n : w.nds()) {
          if (nodes_dense.get(n.val())) {
            ways_dense.set(w.id.val());
          }
        }
      }
    }

    // Mark all nodes in way nodes:    node(w)

    {
      Random_File< Node_Skeleton::Id_Type, Uint32_Index > current(rman.get_transaction()->random_index
          (current_skeleton_file_properties< Node_Skeleton >()));

      Block_Backend< Uint31_Index, Way_Skeleton > db (rman.get_transaction()->data_index(osm_base_settings().WAYS));

      for (typename Block_Backend< Uint31_Index, Way_Skeleton, std::set< Uint31_Index >::const_iterator >::Discrete_Iterator
          it(db.discrete_begin(req.begin(), req.end())); !(it == db.discrete_end()); ++it)
      {
        auto w = it.object();
        if (ways_dense.get(w.id.val())) {
          for (const auto& n : w.nds()) {
            bool added_new_entry = nodes_dense.check_and_set(n.val());
            if (added_new_entry)
              node_idx_outside_bbox.insert(current.get(n.val()));
          }
        }
      }
    }

    // Relations for nodes    rel(bn)->.a;

    {
      Block_Backend< Uint31_Index, Relation_Skeleton > db (rman.get_transaction()->data_index(osm_base_settings().RELATIONS));

      for (typename Block_Backend< Uint31_Index, Relation_Skeleton, std::set< Uint31_Index >::const_iterator >::Discrete_Iterator
          it(db.discrete_begin(req.begin(), req.end())); !(it == db.discrete_end()); ++it)
      {

        auto r = it.object();
        for (const auto& m : r.members()) {
          if (m.type == Relation_Entry::NODE) {
            if (nodes_dense.get(m.ref.val()))
              relations_dense.set(r.id.val());
          }
        }
      }
    }

    // Relations for ways    rel(bw)->.a;

    // TODO: check req (it is based on nodes!)

    {
      Block_Backend< Uint31_Index, Relation_Skeleton > db (rman.get_transaction()->data_index(osm_base_settings().RELATIONS));

      for (typename Block_Backend< Uint31_Index, Relation_Skeleton, std::set< Uint31_Index >::const_iterator >::Discrete_Iterator
          it(db.discrete_begin(req.begin(), req.end())); !(it == db.discrete_end()); ++it)
      {

        auto r = it.object();
        for (const auto& m : r.members()) {
          if (m.type == Relation_Entry::WAY)
          {
            if (ways_dense.get(m.ref.val()))
              relations_dense.set(r.id.val());
          }
        }
      }
    }

    // Relations for relations:    rel(br);

    {
      Block_Backend< Uint31_Index, Relation_Skeleton > db (rman.get_transaction()->data_index(osm_base_settings().RELATIONS));

      for (typename Block_Backend< Uint31_Index, Relation_Skeleton >::Flat_Iterator
          it(db.flat_begin()); !(it == db.flat_end()); ++it)
      {
        auto r = it.object();
        for (const auto& m : r.members()) {
          if (m.type == Relation_Entry::RELATION) {
            if (relations_dense.get(m.ref.val()))
              relations_dense.set(r.id.val());
          }
        }
      }
    }
  }
  catch (const File_Error & e)
  {
    std::cout<<e.origin<<' '<<e.filename<<' '<<e.error_number<<'\n';
  }
}

void calc_node_pairs()
{
  Node_Skeleton::Id_Type::Id_Type lower_bound = 0;
  Node_Skeleton::Id_Type::Id_Type upper_bound = 0;
  long nodes = 0;

  for (const auto& n : nodes_dense) {
    nodes++;
    if (nodes == 1000000) {
      node_pairs.push_back({lower_bound, upper_bound});
      lower_bound = n;
      nodes = 0;
    }
    upper_bound = n;
  }
  node_pairs.push_back({lower_bound, upper_bound});
}

void calc_way_pairs()
{
  Way_Skeleton::Id_Type::Id_Type lower_bound = 0;
  Way_Skeleton::Id_Type::Id_Type upper_bound = 0;
  long ways = 0;

  for (const auto& w : ways_dense) {
    ways++;
    if (ways == 100000) {
      way_pairs.push_back({lower_bound, upper_bound});
      lower_bound = w;
      ways = 0;
    }
    upper_bound = w;
  }
  way_pairs.push_back({lower_bound, upper_bound});
}

void calc_rel_pairs()
{
  Relation_Skeleton::Id_Type::Id_Type lower_bound = 0;
  Relation_Skeleton::Id_Type::Id_Type upper_bound = 0;
  long relations = 0;

  for (const auto& r : relations_dense) {
    relations++;
    if (relations == 100000) {
      rel_pairs.push_back({lower_bound, upper_bound});
      lower_bound = r;
      relations = 0;
    }
    upper_bound = r;
  }
  rel_pairs.push_back({lower_bound, upper_bound});
}




void next_package_node(Resource_Manager& rman, std::pair<Node_Skeleton::Id_Type::Id_Type, Node_Skeleton::Id_Type::Id_Type> idx, Bbox_Double bbox)
{
  try
  {
    // TODO: Fix ranges
    auto ranges = ::get_ranges_32(bbox.south, bbox.north, bbox.west, bbox.east);

    // Add ranges for nodes outside bbox that have been pulled in via way completion
    for (const auto r : node_idx_outside_bbox)
      ranges.insert({r, r + 1});

    std::map< Uint32_Index, std::vector< Node_Skeleton > > result;
    long cnt = 0;

    Block_Backend< Uint32_Index, Node_Skeleton > db (rman.get_transaction()->data_index(osm_base_settings().NODES));
    for (auto it(db.range_begin(ranges.begin(), ranges.end()));
        !(it == db.range_end()); ++it)
    {
        if ( it.handle().id().val() >= idx.first &&
             it.handle().id().val() <=  idx.second &&
             nodes_dense.get( it.handle().id().val() ))
        {
          result[it.index()].push_back(it.object());
          cnt++;
        }
    }

    Set into;
    into.nodes = result;

    rman.swap_set("_", into);

  }
  catch (const File_Error & e)
  {
    std::cout<<e.origin<<' '<<e.filename<<' '<<e.error_number<<'\n';
  }

}


void next_package_way(Resource_Manager& rman, std::pair<Way_Skeleton::Id_Type::Id_Type, Way_Skeleton::Id_Type::Id_Type> idx)
{

  try
  {
    // TODO: Fix ranges

    std::map< Uint31_Index, std::vector< Way_Skeleton > > result;
    long cnt = 0;

    Block_Backend< Uint31_Index, Way_Skeleton > db (rman.get_transaction()->data_index(osm_base_settings().WAYS));

    for (typename Block_Backend< Uint31_Index, Way_Skeleton, std::set< Uint31_Index >::const_iterator >::Discrete_Iterator
        it(db.discrete_begin(req.begin(), req.end())); !(it == db.discrete_end()); ++it)

    {
        if ( it.handle().id().val() >= idx.first &&
             it.handle().id().val() <=  idx.second &&
             ways_dense.get( it.handle().id().val() ))
        {
          result[it.index()].push_back(it.object());
          cnt++;
        }
    }

    Set into;
    into.ways = result;

    rman.swap_set("_", into);

  }
  catch (const File_Error & e)
  {
    std::cout<<e.origin<<' '<<e.filename<<' '<<e.error_number<<'\n';
  }

}


void next_package_rel(Resource_Manager& rman, std::pair<Relation_Skeleton::Id_Type::Id_Type, Relation_Skeleton::Id_Type::Id_Type> idx)
{

  try
  {

    // TODO: Fix ranges

    std::map< Uint31_Index, std::vector< Relation_Skeleton > > result;
    long cnt = 0;

    Block_Backend< Uint31_Index, Relation_Skeleton > db (rman.get_transaction()->data_index(osm_base_settings().RELATIONS));

    for (typename Block_Backend< Uint31_Index, Relation_Skeleton, std::set< Uint31_Index >::const_iterator >::Discrete_Iterator
        it(db.discrete_begin(req.begin(), req.end())); !(it == db.discrete_end()); ++it)

    {
        if ( it.handle().id().val() >= idx.first &&
             it.handle().id().val() <=  idx.second &&
             relations_dense.get( it.handle().id().val() ))
        {
          result[it.index()].push_back(it.object());
          cnt++;
        }
    }

    Set into;
    into.relations = result;

    rman.swap_set("_", into);

  }
  catch (const File_Error & e)
  {
    std::cout<<e.origin<<' '<<e.filename<<' '<<e.error_number<<'\n';
  }
  
}





int main(int argc, char *argv[])
{
  std::string db_dir = "";

  uint log_level = Error_Output::CONCISE;
  Debug_Level debug_level = parser_execute;
  Clone_Settings clone_settings;
  int area_level = 0;
  bool respect_timeout = true;

  Bbox_Double bbox(bbox.invalid);


  Error_Output* error_output(new Console_Output(log_level));
  Statement::set_error_output(error_output);

  int argpos = 1;
  while (argpos < argc)
  {
    if (!(strncmp(argv[argpos], "--db-dir=", 9)))
    {
      db_dir = ((std::string)argv[argpos]).substr(9);
      if ((db_dir.size() > 0) && (db_dir[db_dir.size()-1] != '/'))
        db_dir += '/';
    }
    if (!(strncmp(argv[argpos], "--bbox=", 7)))
    {
      std::string bbox_string = ((std::string)argv[argpos]).substr(7);

      std::smatch sm;

      try {
          std::regex r("(-?[0-9\\.]+),(-?[0-9\\.]+),(-?[0-9\\.]+),(-?[0-9\\.]+)");

          if (!std::regex_match(bbox_string, sm, r)) {
            error_output->add_parse_error("Invalid bbox argument", 1);
            exit(1);
          }

          if (sm.size() != 5) {
            error_output->add_parse_error("Invalid bbox argument", 1);
            exit(1);
          }

      } catch (std::regex_error&) {
        error_output->add_parse_error("Invalid bbox argument", 1);
        exit(1);
      }

      bbox.west  = atof((sm[1]).str().c_str());
      bbox.south = atof((sm[2]).str().c_str());
      bbox.east  = atof((sm[3]).str().c_str());
      bbox.north = atof((sm[4]).str().c_str());

    }
    else
    {
      std::cout<<"Unknown argument: "<<argv[argpos]<<"\n\n"
      "Accepted arguments are:\n"
      "  --db-dir=$DB_DIR: The directory where the database resides. If you set this parameter\n"
      "        then osm3s_query will read from the database without using the dispatcher management.\n"
      "  --bbox=west,south,east,north: defines bounding box for map call\n";

      return 0;
    }
    ++argpos;
  }

  if (!bbox.valid()) {
    error_output->add_parse_error("Invalid bbox argument", 1);
    exit(1);
  }


  // connect to dispatcher and get database dir
  try
  {
    Parsed_Query global_settings;
    std::string xml_raw = "[timeout:3600][out:pbf];out meta;";


    Statement::Factory stmt_factory(global_settings);
    if (!parse_and_validate(stmt_factory, global_settings, xml_raw, error_output, debug_level))
      return 0;


    Osm_Script_Statement* osm_script = 0;
    if (!get_statement_stack()->empty())
      osm_script = dynamic_cast< Osm_Script_Statement* >(get_statement_stack()->front());

    uint32 max_allowed_time = 0;
    uint64 max_allowed_space = 0;


    // open read transaction and log this.
    area_level = determine_area_level(error_output, area_level);
    Dispatcher_Stub dispatcher(db_dir, error_output, xml_raw,
                               get_uses_meta_data(), area_level, max_allowed_time, max_allowed_space,
                               global_settings);
    if (osm_script && osm_script->get_desired_timestamp())
      dispatcher.resource_manager().set_desired_timestamp(osm_script->get_desired_timestamp());

    Web_Output web_output(log_level);
    web_output.set_output_handler(global_settings.get_output_handler());
    web_output.write_payload_header("", dispatcher.get_timestamp(),
           area_level > 0 ? dispatcher.get_area_timestamp() : "", false);


    // Inject one package into default inputset
    prep_map_data(dispatcher.resource_manager(), bbox);
    calc_node_pairs();
    calc_way_pairs();
    calc_rel_pairs();

    dispatcher.resource_manager().start_cpu_timer(0);

    for (const auto & idx : node_pairs)
    {
      next_package_node(dispatcher.resource_manager(), idx, bbox);

      for (std::vector< Statement* >::const_iterator it(get_statement_stack()->begin());
           it != get_statement_stack()->end(); ++it)
        (*it)->execute(dispatcher.resource_manager());
    }

    for (const auto & idx : way_pairs)
    {
      next_package_way(dispatcher.resource_manager(), idx);

      for (std::vector< Statement* >::const_iterator it(get_statement_stack()->begin());
           it != get_statement_stack()->end(); ++it)
        (*it)->execute(dispatcher.resource_manager());
    }

    for (const auto & idx : rel_pairs)
    {
      next_package_rel(dispatcher.resource_manager(), idx);

      for (std::vector< Statement* >::const_iterator it(get_statement_stack()->begin());
           it != get_statement_stack()->end(); ++it)
        (*it)->execute(dispatcher.resource_manager());
    }

    dispatcher.resource_manager().stop_cpu_timer(0);

    web_output.write_footer();

    return 0;
  }
  catch(std::exception& e)
  {
    error_output->runtime_error(std::string("Query failed with the exception: ") + e.what());
    return 4;
  }
  catch(...)
  {
    error_output->runtime_error(std::string("Query could not be executed"));
    return 4;
  }
}




