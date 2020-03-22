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

std::set< Uint32_Index > req_node;
std::set< Uint31_Index > req_way;
std::set< Uint31_Index > req_rel;

std::set< Uint32_Index > node_idx_outside_bbox;

std::set< std::pair< Uint32_Index, Uint32_Index > > node_ranges;


// collect_members.cc:113:

inline std::set< std::pair< Uint32_Index, Uint32_Index > > calc_node_children_ranges
    (const std::set< uint32 >& way_rel_idxs)
{
  std::set< std::pair< Uint32_Index, Uint32_Index > > result;

  std::vector< std::pair< uint32, uint32 > > ranges;

  for (std::set< uint32 >::const_iterator it = way_rel_idxs.begin();
      it != way_rel_idxs.end(); ++it)
  {
    if (*it & 0x80000000)
    {
      uint32 lat = 0;
      uint32 lon = 0;
      uint32 lat_u = 0;
      uint32 lon_u = 0;
      uint32 offset = 0;

      if (*it & 0x00000001)
      {
        lat = upper_ilat(*it & 0x2aaaaaa8);
        lon = upper_ilon(*it & 0x55555554);
        offset = 2;
      }
      else if (*it & 0x00000002)
      {
        lat = upper_ilat(*it & 0x2aaaaa80);
        lon = upper_ilon(*it & 0x55555540);
        offset = 8;
      }
      else if (*it & 0x00000004)
      {
        lat = upper_ilat(*it & 0x2aaaa800);
        lon = upper_ilon(*it & 0x55555400);
        offset = 0x20;
      }
      else if (*it & 0x00000008)
      {
        lat = upper_ilat(*it & 0x2aaa8000);
        lon = upper_ilon(*it & 0x55554000);
        offset = 0x80;
      }
      else if (*it & 0x00000010)
      {
        lat = upper_ilat(*it & 0x2aa80000);
        lon = upper_ilon(*it & 0x55540000);
        offset = 0x200;
      }
      else if (*it & 0x00000020)
      {
        lat = upper_ilat(*it & 0x2a800000);
        lon = upper_ilon(*it & 0x55400000);
        offset = 0x800;
      }
      else if (*it & 0x00000040)
      {
        lat = upper_ilat(*it & 0x28000000);
        lon = upper_ilon(*it & 0x54000000);
        offset = 0x2000;
      }
      else // *it == 0x80000080
      {
        lat = 0;
        lon = 0;
        offset = 0x8000;
      }

      ranges.push_back(std::make_pair(ll_upper(lat<<16, lon<<16),
                                 ll_upper((lat+offset-1)<<16, (lon+offset-1)<<16)+1));
      ranges.push_back(std::make_pair(ll_upper(lat<<16, (lon+offset)<<16),
                                 ll_upper((lat+offset-1)<<16, (lon+2*offset-1)<<16)+1));
      ranges.push_back(std::make_pair(ll_upper((lat+offset)<<16, lon<<16),
                                 ll_upper((lat+2*offset-1)<<16, (lon+offset-1)<<16)+1));
      ranges.push_back(std::make_pair(ll_upper((lat+offset)<<16, (lon+offset)<<16),
                                 ll_upper((lat+2*offset-1)<<16, (lon+2*offset-1)<<16)+1));
      for (uint32 i = lat; i <= lat_u; ++i)
      {
        for (uint32 j = lon; j <= lon_u; ++j)
          result.insert(std::make_pair(ll_upper(i<<16, j<<16), ll_upper(i<<16, j<<16)+1));
      }
    }
    else
      ranges.push_back(std::make_pair(*it, (*it) + 1));
  }
  std::sort(ranges.begin(), ranges.end());
  uint32 pos = 0;
  for (std::vector< std::pair< uint32, uint32 > >::const_iterator it = ranges.begin();
      it != ranges.end(); ++it)
  {
    if (pos < it->first)
      pos = it->first;
    result.insert(std::make_pair(pos, it->second));
    pos = it->second;
  }
  return result;
}



void prep_map_data(Resource_Manager& rman, Bbox_Double bbox)
{

  
  try
  {    
    // based on map query: "(node(bbox);way(bn);node(w););(._;(rel(bn)->.a;rel(bw)->.a;);rel(br););out meta;"

    std::vector< uint32 > node_idxs;
    std::vector< uint32 > node_idxs_outside;

    {
      // Ranges for bbox
      node_ranges = ::get_ranges_32(bbox.south, bbox.north, bbox.west, bbox.east);

      // Nodes inside bbox:     node( {{bbox}} )

      uint32 south = ilat_(bbox.south);
      uint32 north = ilat_(bbox.north);
      int32 west = ilon_(bbox.west);
      int32 east = ilon_(bbox.east);

      Uint32_Index previous_node_idx{};

      Block_Backend< Uint32_Index, Node_Skeleton > db (rman.get_transaction()->data_index(osm_base_settings().NODES));
      for (auto it(db.range_begin(node_ranges.begin(), node_ranges.end()));
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
            req_node.insert(it.index());
            previous_node_idx = it.index();
          }
        }
      }

      // remove duplicates in node_idxs
      std::sort(node_idxs.begin(), node_idxs.end());
      node_idxs.erase(unique(node_idxs.begin(), node_idxs.end()), node_idxs.end());
    }

    // Recurse nodes to ways:   way(bn)   - RECURSE_NODE_WAY
    // collect_ways (collect_mnembers.cc1208)
    //                    --> extract_children_ids (collect all Node ids)
    //                    --> extract_parent_indices -> calls calc_parents on node_idxs.
    {
      std::vector< uint32 > parents = calc_parents(node_idxs);
      for (std::vector< uint32 >::const_iterator it = parents.begin(); it != parents.end(); ++it)
        req_way.insert(Uint31_Index(*it));

      // req_way now contains the result of extract_parent_indices(nodes)
    }


    {
      // Fetch ways for nodes  - collect_ways / collect_items_discrete
      std::set< uint32 > parents;

      uint32 prev_ll_upper = 0;
      uint32 prev_idx = 0;

      // collect_ways --> collect_items_discrete
      Block_Backend< Uint31_Index, Way_Skeleton > db (rman.get_transaction()->data_index(osm_base_settings().WAYS));

      for (typename Block_Backend< Uint31_Index, Way_Skeleton, std::set< Uint31_Index >::const_iterator >::Discrete_Iterator
          it(db.discrete_begin(req_way.begin(), req_way.end())); !(it == db.discrete_end()); ++it)
      {
        auto w = it.object();

        // check if way contains a known node id
        for (const auto& n : w.nds()) {
          if (nodes_dense.get(n.val())) {

            // mark way id as relevant
            ways_dense.set(w.id.val());
            // collect indices for ways, see collect_items_discrete
            // Logic from way_nd_indices (collect_members:cc:393):
            if ((it.index().val() & 0x80000000) && ((it.index().val() & 0x1) == 0)) // Adapt 0x3
            {
              // Treat ways with really large indices: get the node indexes from the segment indexes

                for (const auto g : w.geometry()) {
                  if (g.ll_upper != prev_ll_upper) {
                    parents.insert(g.ll_upper);
                    prev_ll_upper = g.ll_upper;
                  }
                }
            }
            else {
              if (it.index().val() != prev_idx) {
                parents.insert(it.index().val());
                prev_idx = it.index().val();
              }
            }

          }
        }
      }

      // remaining part way_nd_indices (collect_members.cc:429)

      // collect_members.cc:280 (remaining part of collect_node_req is not relevant, as map_ids is empty)
      auto node_ranges_par = calc_node_children_ranges(parents);   // node_ranges contains result of way_nd_indices now
      node_ranges.insert(node_ranges_par.begin(), node_ranges_par.end());
    }


    // Mark all nodes in way nodes:    node(w)    - RECURSE_WAY_NODE - recurse.cc:2712 - collect_members.cc:833 - way_members
    //                                                                 --> way_nd_ids (collect_members.cc:72) -> way_nd_ids (collect_members.cc:30)
    //                                                                     (-> collects all way' nodes: see ways_dense)
    //                                                                     way_nd_indices (collect_members.cc:393)
    // --> way_members (collect_members.cc:847) --> paired_items_range
    //                                              --> collect_items_range (collect_items.h:453) with all node ids on NODES
    //                                                  (collects Node_Skeletons in into.nodes, not yet needed here!)



    {
      // Random access to NODES: lookup index for node id
      Random_File< Node_Skeleton::Id_Type, Uint32_Index > current(rman.get_transaction()->random_index
          (current_skeleton_file_properties< Node_Skeleton >()));

      Block_Backend< Uint31_Index, Way_Skeleton > db (rman.get_transaction()->data_index(osm_base_settings().WAYS));

      for (typename Block_Backend< Uint31_Index, Way_Skeleton, std::set< Uint31_Index >::const_iterator >::Discrete_Iterator
          it(db.discrete_begin(req_way.begin(), req_way.end())); !(it == db.discrete_end()); ++it)
      {
        auto w = it.object();
        if (ways_dense.get(w.id.val())) {
          for (const auto& n : w.nds()) {
            bool added_new_entry = nodes_dense.check_and_set(n.val());
            if (added_new_entry) {
              auto n_idx = current.get(n.val());       // collect indices for new nodes which are outside of bounding box
              node_idx_outside_bbox.insert(n_idx);
              node_idxs_outside.push_back(n_idx.val());
            }
          }
        }
      }

      std::sort(node_idxs_outside.begin(), node_idxs_outside.end());
      node_idxs_outside.erase(unique(node_idxs_outside.begin(), node_idxs_outside.end()), node_idxs_outside.end());
    }

    // Relations for nodes    rel(bn)->.a;         // RECURSE_NODE_RELATION - recurse.cc:2774   --> collect_relations (recurse.cc:212)
    //                                                --> extract_children_ids (get all Node Ids)
    //                                                --> extract_parent_indices (collect_members.h:1008) -> collect all node idxs
    //                                                     --> calc_parents (based on nodes idx)
    //                                                --> collect_items_discrete (for RELATIONS) -> checks if Relation has node with relevant id

    {
      auto parents_node_idx = calc_parents(node_idxs);
      for (const auto & node_idx : parents_node_idx)
        req_rel.insert(Uint31_Index(node_idx));

      auto parents_node_outside_idx = calc_parents(node_idxs_outside);
      for (const auto & node_outside_idx : parents_node_outside_idx)
        req_rel.insert(Uint31_Index(node_outside_idx));

      auto parents_way_idx = calc_parents(req_way);
      for (const auto & parent_way_idx : parents_way_idx)
        req_rel.insert(parent_way_idx);
    }

    {
      Block_Backend< Uint31_Index, Relation_Skeleton > db (rman.get_transaction()->data_index(osm_base_settings().RELATIONS));

      for (typename Block_Backend< Uint31_Index, Relation_Skeleton, std::set< Uint31_Index >::const_iterator >::Discrete_Iterator
          it(db.discrete_begin(req_rel.begin(), req_rel.end())); !(it == db.discrete_end()); ++it)
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

    // Relations for ways    rel(bw)->.a;                 RECURSE_WAY_RELATION (recurse.cc:2783)
    //                                                    --> collect_relations (recuse.cc:212), based on ways
    //                                                    --> extract_children_ids (get all Way ids)
    //                                                    --> extract_parent_indices (collect_members.cc:1008) --> calc_parents ((based on way_idxs)
    //                                                    --> collect_items_discrete (for RELATIONS) -> checks if Relation has way with relevant id

    {
      Block_Backend< Uint31_Index, Relation_Skeleton > db (rman.get_transaction()->data_index(osm_base_settings().RELATIONS));

      for (typename Block_Backend< Uint31_Index, Relation_Skeleton, std::set< Uint31_Index >::const_iterator >::Discrete_Iterator
          it(db.discrete_begin(req_rel.begin(), req_rel.end())); !(it == db.discrete_end()); ++it)
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

    // Relations for relations:    rel(br);               RECURSE_RELATION_BACKWARDS (recuse.cc:2661)
    //                                                    --> collect_relations (recuse.cc:456)
    //                                                    --> extract_children_ids (get all relations ids)
    //                                                    --> collect_items_flat :: check if relation has known relation id as member

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
              req_rel.insert(it.index());
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

//    auto ranges = ::get_ranges_32(bbox.south, bbox.north, bbox.west, bbox.east);

    auto ranges = node_ranges;

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
    std::map< Uint31_Index, std::vector< Way_Skeleton > > result;
    long cnt = 0;

    Block_Backend< Uint31_Index, Way_Skeleton > db (rman.get_transaction()->data_index(osm_base_settings().WAYS));

    for (typename Block_Backend< Uint31_Index, Way_Skeleton, std::set< Uint31_Index >::const_iterator >::Discrete_Iterator
        it(db.discrete_begin(req_way.begin(), req_way.end())); !(it == db.discrete_end()); ++it)

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
    std::map< Uint31_Index, std::vector< Relation_Skeleton > > result;
    long cnt = 0;

    Block_Backend< Uint31_Index, Relation_Skeleton > db (rman.get_transaction()->data_index(osm_base_settings().RELATIONS));

    for (typename Block_Backend< Uint31_Index, Relation_Skeleton, std::set< Uint31_Index >::const_iterator >::Discrete_Iterator
        it(db.discrete_begin(req_rel.begin(), req_rel.end())); !(it == db.discrete_end()); ++it)

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




