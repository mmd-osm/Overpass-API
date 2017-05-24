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
#include "node_updater.h"


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

    Nonsynced_Transaction transaction(false, false, db_dir, "");

    Block_Backend< Uint31_Index, Node_Skeleton > db(transaction.data_index(osm_base_settings().NODES));
    Block_Backend< Uint31_Index, Node_Skeleton >::Flat_Iterator it_nodes(db.flat_begin());

    Block_Backend< Tag_Index_Local, Node_Skeleton::Id_Type > db_tags(transaction.data_index(osm_base_settings().NODE_TAGS_LOCAL));
    Block_Backend< Tag_Index_Local, Node_Skeleton::Id_Type >::Flat_Iterator it_tags(db_tags.flat_begin());



    int elems = 0;

    std::vector< Node_Skeleton::Id_Type::Id_Type> node_ids_tag;
    std::vector< std::pair < Uint31_Index, Node_Skeleton > > node_ids_all;
    std::map< Uint31_Index, std::set< Node_Skeleton > > nodes_map_insert;

    while (!(it_nodes == db.flat_end()))
    {

      Uint31_Index current_coarse_index = it_nodes.index().val() & 0x7fffff00;

      while (!(it_tags == db_tags.flat_end()) && Uint31_Index(it_tags.index().index) < current_coarse_index)
        ++it_tags;

      while (!(it_tags == db_tags.flat_end()) && Uint31_Index(it_tags.index().index) == current_coarse_index)
      {
        node_ids_tag.emplace_back(it_tags.object().val());
        ++it_tags;
      }

      std::sort(node_ids_tag.begin(), node_ids_tag.end());
      node_ids_tag.erase(std::unique( node_ids_tag.begin(), node_ids_tag.end()), node_ids_tag.end());

      while (!(it_nodes == db.flat_end()) && Uint31_Index(it_nodes.index().val() & 0x7fffff00) == current_coarse_index)
      {
        node_ids_all.push_back( std::pair < Uint31_Index, Node_Skeleton >(it_nodes.index(), it_nodes.object()));
        ++it_nodes;
      }

      for (auto const & n : node_ids_all) {

        if (std::binary_search(node_ids_tag.begin(), node_ids_tag.end(), n.second.id.val()))
        {
           std::set< Node_Skeleton > & refs = nodes_map_insert[n.first];
           refs.insert(n.second);
           elems++;
        }
      }
      
      {
      Nonsynced_Transaction target_transaction(true, false, db_dir, "");
      Block_Backend< Uint31_Index, Node_Skeleton > db_tagged(target_transaction.data_index(osm_base_settings().NODES_TAGGED));
      db_tagged.update(std::map< Uint31_Index, std::set< Node_Skeleton > > (), nodes_map_insert);
      nodes_map_insert.clear();
      }

      node_ids_tag.clear();
      node_ids_all.clear();
    }
    
    while (!(it_tags == db_tags.flat_end()))
      ++it_tags;

  }
  catch (const File_Error & e)
  {
    std::cout<<e.origin<<' '<<e.filename<<' '<<e.error_number<<'\n';
  }
  
  return 0;
}
