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

#include "../../template_db/block_backend.h"
#include "../../template_db/random_file.h"
#include "../core/settings.h"
#include "../data/meta_collector.h"
#include "changeset.h"

#include <algorithm>
#include <cmath>
#include <sstream>
#include <vector>


//-----------------------------------------------------------------------------

class Changeset_Constraint : public Query_Constraint
{
  public:
    Changeset_Constraint(Changeset_Statement& cs) : changeset(cs.get_changeset()) {}

    bool delivers_data(Resource_Manager& rman) { return false; }

    void filter(const Statement& query, Resource_Manager& rman, Set& into, uint64 timestamp);
    virtual ~Changeset_Constraint() {}

  private:
    uint32 changeset;
};


template< typename TIndex, typename TObject >
void changeset_filter_map
    (std::map< TIndex, std::vector< TObject > >& modify,
     Resource_Manager& rman, uint32 changeset, File_Properties* file_properties)
{
  if (modify.empty())
    return;
  Meta_Collector< TIndex, typename TObject::Id_Type > meta_collector
      (modify, *rman.get_transaction(), file_properties);
  for (typename std::map< TIndex, std::vector< TObject > >::iterator it = modify.begin();
      it != modify.end(); ++it)
  {
    std::vector< TObject > local_into;
    for (typename std::vector< TObject >::const_iterator iit = it->second.begin();
        iit != it->second.end(); ++iit)
    {
      const OSM_Element_Metadata_Skeleton< typename TObject::Id_Type >* meta_skel
	  = meta_collector.get(it->first, iit->id);
      if ((meta_skel) && (meta_skel->changeset == changeset))
	local_into.push_back(*iit);
    }
    it->second.swap(local_into);
  }
}


template< typename TIndex, typename TObject >
void changeset_filter_map_attic
    (std::map< TIndex, std::vector< TObject > >& modify,
     Resource_Manager& rman, uint32 changeset,
     File_Properties* current_file_properties, File_Properties* attic_file_properties)
{
  if (modify.empty())
    return;

  Meta_Collector< TIndex, typename TObject::Id_Type > current_meta_collector
      (modify, *rman.get_transaction(), current_file_properties);
  Meta_Collector< TIndex, typename TObject::Id_Type > attic_meta_collector
      (modify, *rman.get_transaction(), attic_file_properties);

  for (typename std::map< TIndex, std::vector< TObject > >::iterator it = modify.begin();
      it != modify.end(); ++it)
  {
    std::vector< TObject > local_into;
    for (typename std::vector< TObject >::const_iterator iit = it->second.begin();
        iit != it->second.end(); ++iit)
    {
      const OSM_Element_Metadata_Skeleton< typename TObject::Id_Type >* meta_skel
	  = current_meta_collector.get(it->first, iit->id);
      if (!meta_skel || !(meta_skel->timestamp < iit->timestamp))
        meta_skel = attic_meta_collector.get(it->first, iit->id, iit->timestamp);
      if ((meta_skel) && (meta_skel->changeset == changeset))
	local_into.push_back(*iit);
    }
    it->second.swap(local_into);
  }
}


void Changeset_Constraint::filter(const Statement& query, Resource_Manager& rman, Set& into, uint64 timestamp)
{
  changeset_filter_map(into.nodes, rman, this->changeset, meta_settings().NODES_META);
  changeset_filter_map(into.ways, rman, this->changeset, meta_settings().WAYS_META);
  changeset_filter_map(into.relations, rman, this->changeset, meta_settings().RELATIONS_META);

  if (timestamp != NOW)
  {
    changeset_filter_map_attic(into.attic_nodes, rman, this->changeset,
			       meta_settings().NODES_META, attic_settings().NODES_META);
    changeset_filter_map_attic(into.attic_ways, rman, this->changeset,
			       meta_settings().WAYS_META, attic_settings().WAYS_META);
    changeset_filter_map_attic(into.attic_relations, rman, this->changeset,
			       meta_settings().RELATIONS_META, attic_settings().RELATIONS_META);
  }

  into.areas.clear();
}

//-----------------------------------------------------------------------------

Generic_Statement_Maker< Changeset_Statement > Changeset_Statement::statement_maker("changeset");

Changeset_Statement::Changeset_Statement
    (int line_number_, const std::map< std::string, std::string >& input_attributes, Parsed_Query& global_settings)
    : Statement(line_number_), changeset(0)
{
  std::map< std::string, std::string > attributes;

  attributes["changeset"] = "";

  eval_attributes_array(get_name(), attributes, input_attributes);

  std::string cs = attributes["changeset"];

  changeset = atol(cs.c_str());

  if (changeset <= 0)
  {
    std::ostringstream temp;
    temp<<"The attribute \"changeset\" must be a positive number.";
    add_static_error(temp.str());
  }
}

Changeset_Statement::~Changeset_Statement()
{
  for (std::vector< Query_Constraint* >::const_iterator it = constraints.begin();
      it != constraints.end(); ++it)
    delete *it;
}


void Changeset_Statement::execute(Resource_Manager& rman) {}

Query_Constraint* Changeset_Statement::get_query_constraint()
{
  constraints.push_back(new Changeset_Constraint(*this));
  return constraints.back();
}
