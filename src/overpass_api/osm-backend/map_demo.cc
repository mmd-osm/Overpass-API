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

#include <fstream>
#include <iomanip>
#include <iostream>
#include <list>

#include <locale.h>
#include <stdio.h>
#include <stdlib.h>

#include "../../expat/expat_justparse_interface.h"
#include "../../template_db/random_file.h"
#include "../../template_db/transaction.h"
#include "../core/settings.h"
#include "../frontend/output.h"
#include "../data/collect_members.h"
#include "../data/bbox_filter.h"
#include "../data/way_geometry_store.h"
#include "../core/datatypes.h"
#include "../core/geometry.h"

#include <osmium/index/id_set.hpp>



int main(int argc, char* args[])
{
  if (argc < 2)
  {
    std::cout<<"Usage: "<<args[0]<<" db_dir\n";
    return 0;
  }
  
  std::string db_dir(args[1]);
  
  try
  {    
    osmium::index::IdSetDense<Node_Skeleton::Id_Type::Id_Type>     nodes_dense;
    osmium::index::IdSetDense<Way_Skeleton::Id_Type::Id_Type>      ways_dense;
    osmium::index::IdSetDense<Relation_Skeleton::Id_Type::Id_Type> relations_dense;


    // based on map query: "(node(bbox);way(bn);node(w););(._;(rel(bn)->.a;rel(bw)->.a;);rel(br););out meta;"

    Nonsynced_Transaction transaction(false, false, db_dir, "");

    Bbox_Double bbox(49.8,-14.2,59.5,3.3);  // UK+IRL


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

      Block_Backend< Uint32_Index, Node_Skeleton > db (transaction.data_index(osm_base_settings().NODES));
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

    std::set< Uint31_Index > req;

    {
      std::vector< uint32 > parents = calc_parents(node_idxs);
      for (std::vector< uint32 >::const_iterator it = parents.begin(); it != parents.end(); ++it)
        req.insert(Uint31_Index(*it));
    }

    // Fetch ways for nodes

    {
      Block_Backend< Uint31_Index, Way_Skeleton > db (transaction.data_index(osm_base_settings().WAYS));

      for (typename Block_Backend< Uint31_Index, Way_Skeleton, std::set< Uint31_Index >::const_iterator >::Discrete_Iterator
          it(db.discrete_begin(req.begin(), req.end())); !(it == db.discrete_end()); ++it)
      {

        auto w = it.object();
        for (const auto& n : w.nds) {
          if (nodes_dense.get(n.val())) {
            ways_dense.set(w.id.val());
          }
        }
      }
    }

    // Mark all nodes in way nodes:    node(w)

    {
      Block_Backend< Uint31_Index, Way_Skeleton > db (transaction.data_index(osm_base_settings().WAYS));

      for (typename Block_Backend< Uint31_Index, Way_Skeleton, std::set< Uint31_Index >::const_iterator >::Discrete_Iterator
          it(db.discrete_begin(req.begin(), req.end())); !(it == db.discrete_end()); ++it)
      {
        auto w = it.object();
        if (ways_dense.get(w.id.val())) {
          for (const auto& n : w.nds) {
              nodes_dense.set(n.val());
            }
        }
      }
    }

    // Relations for nodes    rel(bn)->.a;

    {
      Block_Backend< Uint31_Index, Relation_Skeleton > db (transaction.data_index(osm_base_settings().RELATIONS));

      for (typename Block_Backend< Uint31_Index, Relation_Skeleton, std::set< Uint31_Index >::const_iterator >::Discrete_Iterator
          it(db.discrete_begin(req.begin(), req.end())); !(it == db.discrete_end()); ++it)
      {

        auto r = it.object();
        for (const auto& m : r.members) {
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
      Block_Backend< Uint31_Index, Relation_Skeleton > db (transaction.data_index(osm_base_settings().RELATIONS));

      for (typename Block_Backend< Uint31_Index, Relation_Skeleton, std::set< Uint31_Index >::const_iterator >::Discrete_Iterator
          it(db.discrete_begin(req.begin(), req.end())); !(it == db.discrete_end()); ++it)
      {

        auto r = it.object();
        for (const auto& m : r.members) {
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
      Block_Backend< Uint31_Index, Relation_Skeleton > db (transaction.data_index(osm_base_settings().RELATIONS));

      for (typename Block_Backend< Uint31_Index, Relation_Skeleton, std::set< Uint31_Index >::const_iterator >::Discrete_Iterator
          it(db.discrete_begin(req.begin(), req.end())); !(it == db.discrete_end()); ++it)
      {

        auto r = it.object();
        for (const auto& m : r.members) {
          if (m.type == Relation_Entry::RELATION) {
            if (relations_dense.get(m.ref.val()))
              relations_dense.set(r.id.val());
          }
        }
      }
    }

    long nodes = 0;
    for (const auto& n : nodes_dense) nodes++;

    long ways = 0;
    for (const auto& w : ways_dense) ways++;

    long rels = 0;
    for (const auto& r : relations_dense) rels++;


    std::cout << nodes<< std::endl;
    std::cout << ways<< std::endl;
    std::cout << rels<< std::endl;


  }
  catch (const File_Error & e)
  {
    std::cout<<e.origin<<' '<<e.filename<<' '<<e.error_number<<'\n';
  }
  
  return 0;
}
