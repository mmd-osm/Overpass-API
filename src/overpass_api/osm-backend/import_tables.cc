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

#include <locale.h>
#include <stdio.h>
#include <stdlib.h>

#include <csignal>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <list>
#include <map>
#include <sstream>
#include <vector>


#include <cereal/access.hpp>
#include <cereal/archives/binary.hpp>
#include <cereal/archives/json.hpp>
#include <cereal/archives/xml.hpp>
#include <cereal/types/string.hpp>
#include <cereal/types/map.hpp>
#include <cereal/types/set.hpp>
#include <cereal/types/utility.hpp>
#include <cereal/types/vector.hpp>

#include "../../expat/expat_justparse_interface.h"
#include "../../template_db/random_file.h"
#include "../../template_db/transaction.h"
#include "../../template_db/dispatcher.h"
#include "../frontend/console_output.h"
#include "../core/settings.h"
#include "../frontend/output.h"
#include "../dispatch/scripting_core.h"



// Attic

template<class Archive>
void serialize(Archive & archive,
               Attic<Relation_Skeleton> & m)
{
  archive(cereal::make_nvp("id",m.id),
          cereal::make_nvp("members",m.members()),
          cereal::make_nvp("node_idxs",m.node_idxs()),
          cereal::make_nvp("way_idxs",m.way_idxs()),
          cereal::make_nvp("timestamp",m.timestamp));
}


template<class Archive>
void serialize(Archive & archive,
               Attic<Way_Delta> & m)
{
  archive(cereal::make_nvp("id",m.id),
          cereal::make_nvp("full",m.full),
          cereal::make_nvp("nds_removed",m.nds_removed),
          cereal::make_nvp("nds_added",m.nds_added),
          cereal::make_nvp("geometry_removed",m.geometry_removed),
          cereal::make_nvp("geometry_added",m.geometry_added),
          cereal::make_nvp("timestamp",m.timestamp));
}



template<class Archive>
void serialize(Archive & archive,
          Attic<Relation_Delta> & m)
{
  archive(cereal::make_nvp("id",m.id),
          cereal::make_nvp("full",m.full),
          cereal::make_nvp("members_removed",m.members_removed),
          cereal::make_nvp("members_added",m.members_added),
          cereal::make_nvp("node_idxs_removed",m.node_idxs_removed),
          cereal::make_nvp("node_idxs_added",m.node_idxs_added),
          cereal::make_nvp("way_idxs_removed",m.way_idxs_removed),
          cereal::make_nvp("way_idxs_added",m.way_idxs_added),
          cereal::make_nvp("timestamp",m.timestamp));

}


template<class Archive>
void serialize(Archive & archive,
               Attic< Node_Skeleton > & m)
{
  archive(cereal::make_nvp("id",m.id),
          cereal::make_nvp("ll_lower",m.ll_lower),
          cereal::make_nvp("timestamp",m.timestamp));
}

template<class Archive>
void serialize(Archive & archive,
    Attic<Tag_Object_Global<Uint32_Index> >& m)
{
  archive( cereal::make_nvp("idx",m.idx),
           cereal::make_nvp("id",m.id),
           cereal::make_nvp("timestamp",m.timestamp) );
}


template<class Archive>
void save(Archive & archive,
    Attic<Uint32_Index> const & m)
{
  archive(m.val(), cereal::make_nvp("timestamp",m.timestamp));
}

template<class Archive>
void load(Archive & archive,
    Attic<Uint32_Index> & m)
{
    uint32 v;
    uint64 ts;
    archive( v, ts );
    m = std::move(Attic<Uint32_Index>(v, ts));
}

template<class Archive>
void save(Archive & archive,
               Attic<Uint64> const & m)
{
  auto val = m.val();
  archive(val, cereal::make_nvp("timestamp",m.timestamp));
}

template<class Archive>
void load(Archive & archive,
    Attic<Uint64> & m)
{
    uint64 v;
    uint64 ts;
    archive( v, ts );
    m = std::move(Attic<Uint64>(v, ts));
}


template<class Archive>
void save(Archive & archive,
               Attic<Uint40> const & m)
{
  auto val = m.val();
  archive(val, cereal::make_nvp("timestamp",m.timestamp));
}

template<class Archive>
void load(Archive & archive,
    Attic<Uint40> & m)
{
    uint64 v;
    uint64 ts;
    archive( v, ts );
    m = std::move(Attic<Uint40>(v, ts));
}



template<class Archive>
void serialize(Archive & archive,
    Attic< Tag_Object_Global< Uint64 > >& m)
{
  archive( cereal::make_nvp("idx",m.idx),
           cereal::make_nvp("id",m.id),
           cereal::make_nvp("timestamp",m.timestamp));
}



//


template<class Archive>
void serialize(Archive & archive,
               OSM_Element_Metadata_Skeleton< Node_Skeleton::Id_Type > & m)
{
  archive( cereal::make_nvp("ref",m.ref),
           cereal::make_nvp("version",m.version),
           cereal::make_nvp("timestamp",m.timestamp),
           cereal::make_nvp("user_id",m.user_id),
           cereal::make_nvp("changeset",m.changeset ));
}

template<class Archive>
void serialize(Archive & archive,
               OSM_Element_Metadata_Skeleton<Uint32_Index> & m)
{
  archive( cereal::make_nvp("ref",m.ref),
           cereal::make_nvp("version",m.version),
           cereal::make_nvp("timestamp",m.timestamp),
           cereal::make_nvp("user_id",m.user_id),
           cereal::make_nvp("changeset",m.changeset ));
}


template<class Archive>
void serialize(Archive & archive,
               Tag_Index_Global & m)
{
 archive(cereal::make_nvp("key",m.key),
         cereal::make_nvp("value",m.value));
}

template<class Archive>
void serialize(Archive & archive,
    Timestamp & m)
{
  archive( cereal::make_nvp("timestamp",m.timestamp));
}

template<class Archive>
void save(Archive & archive,
    String_Object const & m)
{
  archive(m.val());
}

template<class Archive>
void load(Archive & archive,
    String_Object & m)
{
  std::string s;
  archive( s );
  m = std::move(String_Object(s));
}


template<class Archive>
void serialize(Archive & archive,
    Tag_Object_Global< Node_Skeleton::Id_Type >& m)
{
  archive( cereal::make_nvp("idx",m.idx),
           cereal::make_nvp("id",m.id) );
}

template<class Archive>
void serialize(Archive & archive,
    Tag_Object_Global<Uint32_Index>& m)
{
  archive( cereal::make_nvp("idx",m.idx),
           cereal::make_nvp("id",m.id) );
}

template<class Archive>
void serialize(Archive & archive,
    Tag_Index_Local& m)
{
  archive(cereal::make_nvp("index",m.index),
          cereal::make_nvp("key",m.key),
          cereal::make_nvp("value",m.value));
}

template<class Archive>
void save(Archive & archive,
               const Uint64 & m)
{
  auto val = m.val();
  archive(val);
}

template<class Archive>
void load(Archive & archive,
    Uint64 & m)
{
  uint64 val;
  archive( val );
  m = std::move(Uint64(val));
}


template<class Archive>
void save(Archive & archive,
               const Uint40 & m)
{
  auto val = m.val();
  archive(val);
}

template<class Archive>
void load(Archive & archive,
    Uint40 & m)
{
  uint64 val;
  archive( val );
  m = std::move(Uint40(val));
}

template<class Archive>
void save(Archive & archive,
    Uint32_Index const & m)
{
  archive(m.val());
}

template<class Archive>
void load(Archive & archive,
    Uint32_Index & m)
{
  uint32 v;
  archive( v );
  m = std::move(Uint32_Index(v));
}

template<class Archive>
void save(Archive & archive,
    Uint31_Index const & m)
{
  archive(m.val());
}

template<class Archive>
void load(Archive & archive,
    Uint31_Index & m)
{
  uint32 v;
  archive( v );
  m = std::move(Uint31_Index(v));
}

template<class Archive>
void serialize(Archive & archive,
    Quad_Coord & m)
{
  archive(cereal::make_nvp("ll_upper",m.ll_upper),
          cereal::make_nvp("ll_lower",m.ll_lower));
}

template<class Archive>
void serialize(Archive & archive,
               Node_Skeleton & m)
{
  archive(cereal::make_nvp("id",m.id),
          cereal::make_nvp("ll_lower", m.ll_lower));
}


template<class Archive>
void serialize(Archive & archive,
               Way_Skeleton & m)
{
  archive(cereal::make_nvp("id",m.id),
          cereal::make_nvp("nds",m.nds()),
          cereal::make_nvp("geometry",m.geometry()));
}

template<class Archive>
void serialize(Archive & archive,
               Relation_Skeleton & m)
{
  archive(cereal::make_nvp("id",m.id),
          cereal::make_nvp("members",m.members()),
          cereal::make_nvp("node_idxs",m.node_idxs()),
          cereal::make_nvp("way_idxs",m.way_idxs()));
}

template<class Archive>
void serialize(Archive & archive,
               Change_Entry<Node::Id_Type> & m)
{
  archive(cereal::make_nvp("old_idx",m.old_idx),
          cereal::make_nvp("new_idx",m.new_idx),
          cereal::make_nvp("elem_id",m.elem_id));
}


template<class Archive>
void serialize(Archive & archive,
               Change_Entry<Uint32_Index> & m)
{
  archive(cereal::make_nvp("old_idx",m.old_idx),
          cereal::make_nvp("new_idx",m.new_idx),
          cereal::make_nvp("elem_id",m.elem_id));
}

template<class Archive>
void serialize(Archive & archive,
               Relation_Entry & m)
{
  archive(cereal::make_nvp("ref",m.ref),
          cereal::make_nvp("type",m.type),
          cereal::make_nvp("role", m.role));
}


template<class Archive>
void serialize(Archive & archive,
               User_Data & m)
{
  archive(cereal::make_nvp("id",m.id),
          cereal::make_nvp("name",m.name));
}


template <class Index, class Object>
void import_bin(Transaction& transaction, const File_Properties* fp) {

  std::map<Index, std::set< Object > > res;

  cereal::BinaryInputArchive iarchive(std::cin);

  Block_Backend< Index, Object > db(transaction.data_index(fp));

  uint64 total_objcount = 0;
  uint64 export_total_objcount = 0;

  bool last_entry = false;

  try {

    while (!last_entry) {
      iarchive( export_total_objcount, last_entry, res );

      for (auto const & t : res)
        total_objcount += t.second.size();

      if (total_objcount != export_total_objcount) {
        std::cerr << "total_objcount: " << total_objcount << "\n";
        std::cerr << "export_total_objcount: " << export_total_objcount << "\n";
        std::cerr << "last_entry: " << last_entry << "\n";
        throw std::runtime_error ("import_tables: mismatch total object count");
      }

      if (!res.empty()) {
        db.update(std::map< Index, std::set< Object > > (), res);
      }
      res.clear();
    }
  } catch(std::exception& ex) {
    std::cerr << "(err) Import: total object count: " << total_objcount << "\n";
    throw;
  }
}


template< typename Key, typename TIndex >
void import_map(Transaction& transaction, const File_Properties* fp) {

  cereal::BinaryInputArchive iarchive(std::cin);

  Random_File_Index& dest_idx = *transaction.random_index(fp);
  Random_File< Key, TIndex > dest_file(&dest_idx);

  while (true) {
    Key key;
    TIndex idx;
    bool eof;
    iarchive( key, idx, eof );
    if (eof)
      return;
    dest_file.put(key, idx);
  }
}

int main(int argc, char* args[])
{
  signal(SIGPIPE, SIG_IGN);

  if (argc < 3)
  {
    std::cout<<"Usage: "<<args[0]<<" db_dir step\n";
    return 0;
  }

  std::string db_dir(args[1]);
  uint32 step = atoi(args[2]);

  try
  {
    Nonsynced_Transaction transaction(true, false, db_dir, "");

    Parsed_Query global_settings;

    Error_Output* error_output(new Console_Output(1));
    Statement::set_error_output(error_output);

    // connect to dispatcher and get database dir

    //    Dispatcher_Stub dispatcher(db_dir, error_output, "-- export tables --",
    //        keep_attic, 0, 24*60*60, 1024*1024*1024, global_settings);

    //    Transaction& transaction = *dispatcher.resource_manager().get_transaction();

    if (std::string("--nodes") == args[2] || step == 1)
    {
      import_bin< Uint31_Index, Node_Skeleton >(transaction, osm_base_settings().NODES);
    }
    else if (std::string("--nodes-map") == args[2] || step == 2)
    {
      import_map<Node_Skeleton::Id_Type, Uint32_Index>(transaction, osm_base_settings().NODES);
    }
    else if (std::string("--node-tags-local") == args[2] || step == 3)
    {
      import_bin< Tag_Index_Local, Node_Skeleton::Id_Type >(transaction, osm_base_settings().NODE_TAGS_LOCAL);
    }
    else if (std::string("--node-tags-global") == args[2] || step == 4)
    {
      import_bin< Tag_Index_Global, Tag_Object_Global< Node_Skeleton::Id_Type > >(transaction, osm_base_settings().NODE_TAGS_GLOBAL);
    }
    else if (std::string("--node-keys") == args[2] || step == 5)
    {
      import_bin< Uint32_Index, String_Object >(transaction, osm_base_settings().NODE_KEYS);
    }
    else if (std::string("--ways") == args[2] || step == 6)
    {
      import_bin< Uint31_Index, Way_Skeleton >(transaction, osm_base_settings().WAYS);
    }
    else if (std::string("--ways-map") == args[2] || step == 7)
    {
      import_map< Way_Skeleton::Id_Type, Uint31_Index >(transaction, osm_base_settings().WAYS);
    }
    else if (std::string("--way-tags-local") == args[2] || step == 8)
    {
      import_bin< Tag_Index_Local, Way_Skeleton::Id_Type >(transaction, osm_base_settings().WAY_TAGS_LOCAL);
    }
    else if (std::string("--way-tags-global") == args[2] || step == 9)
    {
      import_bin< Tag_Index_Global, Tag_Object_Global< Way_Skeleton::Id_Type > >(transaction, osm_base_settings().WAY_TAGS_GLOBAL);
    }
    else if (std::string("--way-keys") == args[2] || step == 10)
    {
      import_bin< Uint32_Index, String_Object >(transaction, osm_base_settings().WAY_KEYS);
    }
    else if (std::string("--rels") == args[2] || step == 11)
    {
      import_bin< Uint31_Index, Relation_Skeleton >(transaction, osm_base_settings().RELATIONS);
    }
    else if (std::string("--rels-map") == args[2] || step == 12)
    {
      import_map< Relation_Skeleton::Id_Type, Uint31_Index >(transaction, osm_base_settings().RELATIONS);
    }
    else if (std::string("--rel-roles") == args[2] || step == 13)
    {
      import_bin< Uint32_Index, String_Object >(transaction, osm_base_settings().RELATION_ROLES);
    }
    else if (std::string("--rel-tags-local") == args[2] || step == 14)
    {
      import_bin< Tag_Index_Local, Relation_Skeleton::Id_Type >(transaction, osm_base_settings().RELATION_TAGS_LOCAL);
    }
    else if (std::string("--rel-tags-global") == args[2] || step == 15)
    {
      import_bin< Tag_Index_Global, Tag_Object_Global< Relation_Skeleton::Id_Type > >(transaction, osm_base_settings().RELATION_TAGS_GLOBAL);
    }
    else if (std::string("--rel-keys") == args[2] || step == 16)
    {
      import_bin< Uint32_Index, String_Object >(transaction, osm_base_settings().RELATION_KEYS);
    }
    else if (std::string("--nodes-meta") == args[2] || step == 17)
    {
      import_bin< Uint31_Index, OSM_Element_Metadata_Skeleton< Node_Skeleton::Id_Type > >(transaction, meta_settings().NODES_META);
    }
    else if (std::string("--ways-meta") == args[2] || step == 18)
    {
      import_bin< Uint31_Index, OSM_Element_Metadata_Skeleton< Way_Skeleton::Id_Type > >(transaction, meta_settings().WAYS_META);
    }
    else if (std::string("--rels-meta") == args[2] || step == 19)
    {
      import_bin< Uint31_Index, OSM_Element_Metadata_Skeleton< Relation_Skeleton::Id_Type > >(transaction, meta_settings().RELATIONS_META);
    }
    else if (std::string("--user") == args[2] || step == 20)
    {
      import_bin< Uint32_Index, User_Data >(transaction, meta_settings().USER_DATA);
    }
    else if (std::string("--user-idxs") == args[2] || step == 21)
    {
      import_bin< Uint32_Index, Uint31_Index >(transaction, meta_settings().USER_INDICES);
    }
    else if (std::string("--attic-nodes") == args[2] || step == 22)
    {
      import_bin< Uint31_Index, Attic< Node_Skeleton > >(transaction, attic_settings().NODES);
    }
    else if (std::string("--attic-nodes-map") == args[2] || step == 23)
    {
      import_map< Node_Skeleton::Id_Type, Uint31_Index >(transaction, attic_settings().NODES);
    }
    else if (std::string("--nodes-undelete") == args[2] || step == 24)
    {
      import_bin< Uint32_Index, Attic< Node_Skeleton::Id_Type > >(transaction, attic_settings().NODES_UNDELETED);
    }
    else if (std::string("--attic-node-idxs") == args[2] || step == 25)
    {
      import_bin< Node_Skeleton::Id_Type, Uint31_Index >(transaction, attic_settings().NODE_IDX_LIST);
    }
    else if (std::string("--attic-node-tags-local") == args[2] || step == 26)
    {
      import_bin< Tag_Index_Local, Attic< Node_Skeleton::Id_Type > >(transaction, attic_settings().NODE_TAGS_LOCAL);
    }
    else if (std::string("--attic-node-tags-global") == args[2] || step == 27)
    {
      import_bin< Tag_Index_Global, Attic< Tag_Object_Global< Node_Skeleton::Id_Type > > >(transaction, attic_settings().NODE_TAGS_GLOBAL);
    }
    else if (std::string("--attic-nodes-meta") == args[2] || step == 28)
    {
      import_bin< Uint31_Index, OSM_Element_Metadata_Skeleton< Node_Skeleton::Id_Type > >(transaction, attic_settings().NODES_META);
    }
    else if (std::string("--node-changelog") == args[2] || step == 29)
    {
      import_bin< Timestamp, Change_Entry< Node_Skeleton::Id_Type > >(transaction, attic_settings().NODE_CHANGELOG);
    }
    else if (std::string("--attic-ways-delta") == args[2] || step == 30)
    {
      import_bin< Uint31_Index, Attic< Way_Delta > >(transaction, attic_settings().WAYS);
    }
    else if (std::string("--attic-ways-map") == args[2] || step == 31)
    {
      import_map< Way_Skeleton::Id_Type, Uint31_Index >(transaction, attic_settings().WAYS);
    }
    else if (std::string("--ways-undelete") == args[2] || step == 32)
    {
      import_bin< Uint31_Index, Attic< Way_Skeleton::Id_Type > >(transaction, attic_settings().WAYS_UNDELETED);
    }
    else if (std::string("--attic-ways-idxs") == args[2] || step == 33)
    {
      import_bin< Way_Skeleton::Id_Type, Uint31_Index >(transaction, attic_settings().WAY_IDX_LIST);
    }
    else if (std::string("--attic-way-tags-local") == args[2] || step == 34)
    {
      import_bin< Tag_Index_Local, Attic< Way_Skeleton::Id_Type > >(transaction, attic_settings().WAY_TAGS_LOCAL);
    }
    else if (std::string("--attic-way-tags-global") == args[2] || step == 35)
    {
      import_bin< Tag_Index_Global, Attic< Tag_Object_Global< Way_Skeleton::Id_Type > > >(transaction, attic_settings().WAY_TAGS_GLOBAL);
    }
    else if (std::string("--attic-ways-meta") == args[2] || step == 36)
    {
      import_bin< Uint31_Index, OSM_Element_Metadata_Skeleton< Way_Skeleton::Id_Type > >(transaction, attic_settings().WAYS_META);
    }
    else if (std::string("--way-changelog") == args[2] || step == 37)
    {
      import_bin< Timestamp, Change_Entry< Way_Skeleton::Id_Type > >(transaction, attic_settings().WAY_CHANGELOG);
    }
    else if (std::string("--attic-rels-delta") == args[2] || step == 38)
    {
      import_bin< Uint31_Index, Attic< Relation_Delta > >(transaction, attic_settings().RELATIONS);
    }
    else if (std::string("--attic-rels-map") == args[2] || step == 39)
    {
      import_map< Relation_Skeleton::Id_Type, Uint31_Index >(transaction, attic_settings().RELATIONS);
    }
    else if (std::string("--rels-undelete") == args[2] || step == 40)
    {
      import_bin< Uint31_Index, Attic< Relation_Skeleton::Id_Type > >(transaction, attic_settings().RELATIONS_UNDELETED);
    }
    else if (std::string("--attic-rel-idxs") == args[2] || step == 41)
    {
      import_bin< Relation_Skeleton::Id_Type, Uint31_Index >(transaction, attic_settings().RELATION_IDX_LIST);
    }
    else if (std::string("--attic-rel-tags-local") == args[2] || step == 42)
    {
      import_bin< Tag_Index_Local, Attic< Relation_Skeleton::Id_Type > >(transaction, attic_settings().RELATION_TAGS_LOCAL);
    }
    else if (std::string("--attic-rel-tags-global") == args[2] || step == 43)
    {
      import_bin< Tag_Index_Global, Attic< Tag_Object_Global< Relation_Skeleton::Id_Type > > >(transaction, attic_settings().RELATION_TAGS_GLOBAL);
    }
    else if (std::string("--attic-rels-meta") == args[2] || step == 44)
    {
      import_bin< Uint31_Index, OSM_Element_Metadata_Skeleton< Relation_Skeleton::Id_Type > >(transaction, attic_settings().RELATIONS_META);
    }
    else if (std::string("--rel-changelog") == args[2] || step == 45)
    {
      import_bin< Timestamp, Change_Entry< Relation_Skeleton::Id_Type > >(transaction, attic_settings().RELATION_CHANGELOG);
    }
    else
      std::cout<<"Unknown target.\n";
  }
  catch (File_Error & e)
  {
    std::cerr << "Error while executing step " << step << ": " << e.origin<<' '<<e.filename<<' '<<e.error_number<<'\n';
    return 1;
  }
  catch (std::exception& ex) {
    std::cerr << "Error while executing step " << step << ": " << ex.what() << "\n";
    return 2;
  }

  return 0;
}
