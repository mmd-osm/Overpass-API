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

#include <algorithm>
#include <cctype>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <math.h>
#include <stdlib.h>
#include <vector>

#include "../../template_db/block_backend.h"
#include "../core/index_computations.h"
#include "../data/collect_members.h"
#include "../data/tilewise_geometry.h"
#include "area_query.h"
#include "coord_query.h"
#include "make_area.h"
#include "recurse.h"


class Area_Constraint final : public Query_Constraint
{
  public:
    Area_Constraint(Area_Query_Statement& area_) : area(&area_) {}

    Query_Filter_Strategy delivers_data(Resource_Manager& rman) override;

    bool get_ranges
        (Resource_Manager& rman, std::set< std::pair< Uint32_Index, Uint32_Index > >& ranges) override;
    bool get_ranges
        (Resource_Manager& rman, std::set< std::pair< Uint31_Index, Uint31_Index > >& ranges) override;
    void filter(Resource_Manager& rman, Set& into) override;
    void filter(const Statement& query, Resource_Manager& rman, Set& into) override;
    ~Area_Constraint() override = default;
    const Statement * get_statement() const override { return area; };

  private:
    std::ostream& print_constraint( std::ostream &os ) const override {
        return os << (area != nullptr ? area->dump_ql_in_query("") : "area");
    }

    Area_Query_Statement* area;
    std::set< Uint31_Index > area_blocks_req;
};

namespace {

void copy_discrete_to_area_ranges(
    const std::set< Uint31_Index >& area_blocks_req,
    std::set< std::pair< Uint32_Index, Uint32_Index > >& nodes_req)
{
  nodes_req.clear();
  for (auto it = area_blocks_req.begin(); it != area_blocks_req.end(); ++it)
    nodes_req.insert(std::make_pair(Uint32_Index(it->val()), Uint32_Index((it->val()) + 0x100)));
}

}


bool Area_Constraint::get_ranges
    (Resource_Manager& rman, std::set< std::pair< Uint32_Index, Uint32_Index > >& ranges)
{
//  area_blocks_req.clear();
  std::set< Uint31_Index > area_blocks_req;

  if (area->areas_from_input())
  {
    const Set* input = rman.get_set(area->get_input());
    if (!input)
      return true;

    area->get_ranges(input->areas, input->area_blocks, area_blocks_req, rman);

    area->get_ranges(input->ways, input->areas, area_blocks_req, rman);

    if (rman.get_desired_timestamp() == NOW)
      way_covered_indices(area, rman, input->ways.begin(), input->ways.end()).swap(ranges);
    else
      way_covered_indices(area, rman, input->ways.begin(), input->ways.end(),
          input->attic_ways.begin(), input->attic_ways.end()).swap(ranges);
  }
  else
  {
    area->get_ranges(area_blocks_req, rman);
    ranges.clear();
  }

  std::set< std::pair< Uint32_Index, Uint32_Index > > area_ranges;
  copy_discrete_to_area_ranges(area_blocks_req, area_ranges);
  range_union(ranges, area_ranges).swap(ranges);

  return true;
}


bool Area_Constraint::get_ranges
    (Resource_Manager& rman, std::set< std::pair< Uint31_Index, Uint31_Index > >& ranges)
{
  std::set< std::pair< Uint32_Index, Uint32_Index > > node_ranges;
  this->get_ranges(rman, node_ranges);
  ranges = calc_parents(node_ranges);
  return true;
}


void Area_Constraint::filter(Resource_Manager& rman, Set& into)
{
  std::set< std::pair< Uint31_Index, Uint31_Index > > ranges;
  get_ranges(rman, ranges);

  // pre-process ways to reduce the load of the expensive filter
  filter_ways_by_ranges(into.ways, ranges);
  if (!into.attic_ways.empty())
    filter_ways_by_ranges(into.attic_ways, ranges);

  // pre-filter relations
  filter_relations_by_ranges(into.relations, ranges);
  if (!into.attic_relations.empty())
    filter_relations_by_ranges(into.attic_relations, ranges);

  //TODO: filter areas
}

namespace {

template< typename Node_Skeleton >
std::map< Uint32_Index, std::vector< Node_Skeleton > > nodes_contained_in(
    const Set* potential_areas, bool accept_border, const Statement& stmt, Resource_Manager& rman,
    const std::map< Uint32_Index, std::vector< Node_Skeleton > >& nodes)
{
  if (!potential_areas)
    return std::map< Uint32_Index, std::vector< Node_Skeleton > >();
  
  Tilewise_Const_Area_Iterator tai(potential_areas->ways, potential_areas->attic_ways, stmt, rman);
  std::map< Uint32_Index, std::vector< Node_Skeleton > > result;
  
  for (auto iit = nodes.begin();
      iit != nodes.end(); ++iit)
  {
    while (!tai.is_end() && tai.get_idx().val() < iit->first.val())
      tai.next();
    if (tai.is_end() || iit->first.val() < tai.get_idx().val())
      continue;
    
    std::vector< Node_Skeleton >& result_block = result[iit->first];

    for (auto it = iit->second.begin(); it != iit->second.end(); ++it)
    {
      Tilewise_Area_Iterator::Relative_Position relpos = tai.rel_position(iit->first.val(), it->ll_lower, true);
//       std::cout<<"Id "<<it->id.val()<<' '<<relpos<<'\n';
      if ((accept_border && relpos != Tilewise_Area_Iterator::outside)
          || relpos == Tilewise_Area_Iterator::inside)
        result_block.push_back(*it);
    }
  }
  
//   while (!tai.is_end())
//   {
//     const std::map< const Way_Skeleton*, Tilewise_Area_Iterator::Index_Block >& way_blocks = tai.get_obj();
//     for (std::map< const Way_Skeleton*, Tilewise_Area_Iterator::Index_Block >::const_iterator bit = way_blocks.begin();
//         bit != way_blocks.end(); ++bit)
//     {
//       std::cout<<"Index "<<std::hex<<tai.get_idx().val()
//           <<" ("<<std::dec<<lat(tai.get_idx().val(), 0u)<<' '<<lon(tai.get_idx().val(), 0u)<<") "
//           <<std::dec<<bit->first->id.val()<<": "<<bit->second.sw_is_inside;
//       for (std::vector< Tilewise_Area_Iterator::Entry >::const_iterator it = bit->second.segments.begin();
//           it != bit->second.segments.end(); ++it)
//         std::cout<<" ("<<it->ilat_west<<' '<<it->ilon_west<<' '<<it->ilat_east<<' '<<it->ilon_east<<')';
//       std::cout<<'\n';
//     }
//     for (uint i = 0; i < 16; ++i)
//     {
//       std::cout<<"    ";
//       for (uint j = 0; j < 16; ++j)
//         std::cout<<tai.rel_position(tai.get_idx().val(), ll_lower(0xf000 - i*0x1000, j*0x1000));
//       std::cout<<'\n';
//     }
//     tai.next();
//   }
  
  return result;
}


std::map< Uint31_Index, std::vector< Way_Skeleton > > ways_contained_in(
    const Set* potential_areas, const Statement& stmt, Resource_Manager& rman,
    const std::map< Uint31_Index, std::vector< Way_Skeleton > >& ways)
{
  if (!potential_areas)
    return std::map< Uint31_Index, std::vector< Way_Skeleton > >();

  Tilewise_Const_Area_Iterator tai(potential_areas->ways, potential_areas->attic_ways, stmt, rman);
  std::map< Uint31_Index, std::vector< Way_Skeleton > > result;

  Tilewise_Way_Iterator twi(ways, std::map< Uint31_Index, std::vector< Attic< Way_Skeleton > > >(), stmt, rman);
  while (!twi.is_end())
  {
    while (!tai.is_end() && tai.get_idx() < twi.get_idx())
      tai.next();
    if (tai.is_end() || twi.get_idx() < tai.get_idx())
    {
      twi.next();
      continue;
    }

    const std::map< Tilewise_Way_Iterator::Status_Ref< Way_Skeleton >*, Tilewise_Way_Iterator::Index_Block >& obj = twi.get_current_obj();
    for (auto it = obj.begin();
        it != obj.end(); ++it)
    {
      if (it->first->status == Tilewise_Area_Iterator::outside)
      {
        it->first->status = tai.rel_position(it->second.segments, true);
        if (it->first->status != Tilewise_Area_Iterator::outside)
          result[it->first->idx].push_back(*it->first->skel);
      }
    }
    
    twi.next();
  }

  return result;
}


std::map< Uint31_Index, std::vector< Attic< Way_Skeleton > > > ways_contained_in(
    const Set* potential_areas, const Statement& stmt, Resource_Manager& rman,
    const std::map< Uint31_Index, std::vector< Attic< Way_Skeleton > > >& ways)
{
  if (!potential_areas)
    return std::map< Uint31_Index, std::vector< Attic< Way_Skeleton > > >();
  
  Tilewise_Const_Area_Iterator tai(potential_areas->ways, potential_areas->attic_ways, stmt, rman);
  std::map< Uint31_Index, std::vector< Attic< Way_Skeleton > > > result;

  Tilewise_Way_Iterator twi(std::map< Uint31_Index, std::vector< Way_Skeleton > >(), ways, stmt, rman);
  while (!twi.is_end())
  {
    while (!tai.is_end() && tai.get_idx() < twi.get_idx())
      tai.next();
    if (tai.is_end() || twi.get_idx() < tai.get_idx())
    {
      twi.next();
      continue;
    }
    
    const std::map< Tilewise_Way_Iterator::Status_Ref< Attic< Way_Skeleton > >*, Tilewise_Way_Iterator::Index_Block >& obj = twi.get_attic_obj();
    for (auto
        it = obj.begin(); it != obj.end(); ++it)
    {
      if (it->first->status == Tilewise_Area_Iterator::outside)
      {
        it->first->status = tai.rel_position(it->second.segments, true);
        if (it->first->status != Tilewise_Area_Iterator::outside)
          result[it->first->idx].push_back(*it->first->skel);
      }
    }
    
    twi.next();
  }
  
  return result;
}

}

void Area_Constraint::filter(const Statement& query, Resource_Manager& rman, Set& into)
{
  std::set< Uint31_Index > area_blocks_req;
  const Set* input = rman.get_set(area->get_input());
  if (area->areas_from_input())
  {
    if (input) {
      area->get_ranges(input->areas, input->area_blocks, area_blocks_req, rman);

      area->get_ranges(input->ways, input->areas, area_blocks_req, rman);
    }
  }
  else
    area->get_ranges(area_blocks_req, rman);

  //Process nodes
  {
    std::map< Uint32_Index, std::vector< Node_Skeleton > > nodes_in_wr_areas
        = nodes_contained_in(input, true, query, rman, into.nodes);
    indexed_set_difference(into.nodes, nodes_in_wr_areas);
    area->collect_nodes(into.nodes, area_blocks_req, true, rman);
    indexed_set_union(into.nodes, nodes_in_wr_areas);
  } 

  //Process ways
  {
    std::map< Uint31_Index, std::vector< Way_Skeleton > > ways_in_wr_areas
        = ways_contained_in(input, query, rman, into.ways);
    indexed_set_difference(into.ways, ways_in_wr_areas);
    area->collect_ways(Way_Geometry_Store(into.ways, query, rman),
        into.ways, area_blocks_req, false, query, rman);
    indexed_set_union(into.ways, ways_in_wr_areas);
  }

  //Process relations

  // Retrieve all nodes referred by the relations.
  std::set< std::pair< Uint32_Index, Uint32_Index > > node_ranges;
  get_ranges(rman, node_ranges);
  std::map< Uint32_Index, std::vector< Node_Skeleton > > node_members
      = relation_node_members(&query, rman, into.relations, &node_ranges);

  // filter for those nodes that are in one of the areas
  {
    std::map< Uint32_Index, std::vector< Node_Skeleton > > nodes_in_wr_areas
        = nodes_contained_in(input, false, query, rman, node_members);
    indexed_set_difference(node_members, nodes_in_wr_areas);
    area->collect_nodes(node_members, area_blocks_req, false, rman);
    indexed_set_union(node_members, nodes_in_wr_areas);
  } 

  // Retrieve all ways referred by the relations.
  std::set< std::pair< Uint31_Index, Uint31_Index > > way_ranges;
  get_ranges(rman, way_ranges);
  std::map< Uint31_Index, std::vector< Way_Skeleton > > way_members_
      = relation_way_members(&query, rman, into.relations, &way_ranges);

  // Filter for those ways that are in one of the areas
  {
    std::map< Uint31_Index, std::vector< Way_Skeleton > > ways_in_wr_areas
        = ways_contained_in(input, query, rman, way_members_);
    indexed_set_difference(way_members_, ways_in_wr_areas);
    area->collect_ways(Way_Geometry_Store(way_members_, query, rman),
        way_members_, area_blocks_req, false, query, rman);
    indexed_set_union(way_members_, ways_in_wr_areas);
  }

  filter_relations_expensive(order_by_id(node_members, Order_By_Node_Id()),
			     order_by_id(way_members_, Order_By_Way_Id()),
			     into.relations);

  //Process nodes
  if (!into.attic_nodes.empty())
  {
    std::map< Uint32_Index, std::vector< Attic< Node_Skeleton > > > nodes_in_wr_areas
        = nodes_contained_in(input, true, query, rman, into.attic_nodes);
    indexed_set_difference(into.attic_nodes, nodes_in_wr_areas);
    area->collect_nodes(into.attic_nodes, area_blocks_req, true, rman);
    indexed_set_union(into.attic_nodes, nodes_in_wr_areas);
  } 

  //Process ways
  if (!into.attic_ways.empty())
  {
    std::map< Uint31_Index, std::vector< Attic< Way_Skeleton > > > ways_in_wr_areas
        = ways_contained_in(input, query, rman, into.attic_ways);
    indexed_set_difference(into.attic_ways, ways_in_wr_areas);
    area->collect_ways(Way_Geometry_Store(into.attic_ways, query, rman),
        into.attic_ways, area_blocks_req, false, query, rman);
    indexed_set_union(into.attic_ways, ways_in_wr_areas);
  }

  //Process relations
  if (!into.attic_relations.empty())
  {
    // Retrieve all nodes referred by the relations.
    std::set< std::pair< Uint32_Index, Uint32_Index > > node_ranges;
    get_ranges(rman, node_ranges);
    std::map< Uint32_Index, std::vector< Attic< Node_Skeleton > > > node_members
        = relation_node_members(&query, rman, into.attic_relations, &node_ranges);

    // filter for those nodes that are in one of the areas
    {
      std::map< Uint32_Index, std::vector< Attic< Node_Skeleton > > > nodes_in_wr_areas
          = nodes_contained_in(input, false, query, rman, node_members);
      indexed_set_difference(node_members, nodes_in_wr_areas);
      area->collect_nodes(node_members, area_blocks_req, false, rman);
      indexed_set_union(node_members, nodes_in_wr_areas);
    } 

    // Retrieve all ways referred by the relations.
    std::set< std::pair< Uint31_Index, Uint31_Index > > way_ranges;
    get_ranges(rman, way_ranges);
    std::map< Uint31_Index, std::vector< Attic< Way_Skeleton > > > way_members_
        = relation_way_members(&query, rman, into.attic_relations, &way_ranges);

    // Filter for those ways that are in one of the areas
    {
      std::map< Uint31_Index, std::vector< Attic< Way_Skeleton > > > ways_in_wr_areas
          = ways_contained_in(input, query, rman, way_members_);
      indexed_set_difference(way_members_, ways_in_wr_areas);
      area->collect_ways(Way_Geometry_Store(way_members_, query, rman),
          way_members_, area_blocks_req, false, query, rman);
      indexed_set_union(way_members_, ways_in_wr_areas);
    }

    filter_relations_expensive(order_attic_by_id(node_members, Order_By_Node_Id()),
			       order_attic_by_id(way_members_, Order_By_Way_Id()),
			       into.attic_relations);
  }

  //TODO: filter areas
}


//-----------------------------------------------------------------------------

int Area_Query_Statement::area_stmt_ref_counter_ = 0;


Area_Query_Statement::Statement_Maker Area_Query_Statement::statement_maker;
Area_Query_Statement::Criterion_Maker Area_Query_Statement::criterion_maker;


Statement* Area_Query_Statement::Criterion_Maker::create_criterion(const Token_Node_Ptr& input_tree,
    const std::string& type, const std::string& into,
    Statement::Factory& stmt_factory, Parsed_Query& global_settings, Error_Output* error_output)
{
  Token_Node_Ptr tree_it = input_tree;
  uint line_nr = tree_it->line_col.first;
  std::string from = "_";
  std::string ref;

  if (tree_it->token == ":" && tree_it->rhs)
  {
    ref = tree_it.rhs()->token;
    tree_it = tree_it.lhs();
  }

  if (tree_it->token == "." && tree_it->rhs)
    from = tree_it.rhs()->token;

  std::map< std::string, std::string > attributes;
  attributes["from"] = from;
  attributes["into"] = into;
  attributes["ref"] = ref;

  if (type == "derived" || type == "area") {
    error_output->add_parse_error("Area filter only permitted for nodes, ways, or relations.", line_nr);
  }

  return new Area_Query_Statement(line_nr, attributes, global_settings);
}


Area_Query_Statement::Area_Query_Statement
    (int line_number_, const std::map< std::string, std::string >& input_attributes, Parsed_Query& global_settings)
    : Output_Statement(line_number_), area_blocks_req_filled(false)
{
  ++area_stmt_ref_counter_;

  std::map< std::string, std::string > attributes;

  attributes["from"] = "_";
  attributes["into"] = "_";
  attributes["ref"] = "";

  eval_attributes_array(get_name(), attributes, input_attributes);

  input = attributes["from"];
  set_output(attributes["into"]);
  submitted_id = atoll(attributes["ref"].c_str());
  if (submitted_id <= 0 && !attributes["ref"].empty())
  {
    std::ostringstream temp;
    temp<<"For the attribute \"ref\" of the element \"area-query\""
    <<" the only allowed values are positive integers.";
    add_static_error(temp.str());
  }
  else if (submitted_id > 0) {
    area_id.push_back(Area_Skeleton::Id_Type(submitted_id));
    area_id_db.push_back(Area_Skeleton::Id_Type(submitted_id));
  }
}

Area_Query_Statement::~Area_Query_Statement()
{
  if (area_stmt_ref_counter_ > 0)
    --area_stmt_ref_counter_;

  for (std::vector< Query_Constraint* >::const_iterator it = constraints.begin();
      it != constraints.end(); ++it)
    delete *it;
}


unsigned int Area_Query_Statement::count_ranges(Resource_Manager& rman)
{
  if (!area_blocks_req_filled)
    fill_ranges(rman);

  return area_blocks_req.size();
}


void Area_Query_Statement::get_ranges(std::set< Uint31_Index >& area_blocks_req, Resource_Manager& rman)
{
  if (!area_blocks_req_filled)
    fill_ranges(rman);

  area_blocks_req = this->area_blocks_req;
}


void Area_Query_Statement::fill_ranges(Resource_Manager& rman)
{
  Block_Backend< Uint31_Index, Area_Skeleton > area_locations_db
      (rman.get_area_transaction()->data_index(area_settings().AREAS));

  for (const auto & it : area_locations_db.as_flat())
  {
    if (binary_search(area_id.begin(), area_id.end(), it.handle().id()))
    {
      for (auto it2(it.object().used_indices().begin());
          it2 != it.object().used_indices().end(); ++it2)
        area_blocks_req.insert(Uint31_Index(*it2));
    }
  }
  area_blocks_req_filled = true;
}


void Area_Query_Statement::get_ranges
    (const std::map< Uint31_Index, std::vector< Area_Skeleton > >& input_areas,
     const std::map< Uint31_Index, std::vector< Area_Block > >& input_area_blocks,
     std::set< Uint31_Index >& area_blocks_req,
     Resource_Manager& rman)
{
  area_id.clear();
  area_id_db.clear();
  area_id_adhoc.clear();

  // Collect all area ids which have been created using ad-hoc area creation

  for (auto it = input_area_blocks.begin();
       it != input_area_blocks.end(); ++it)
  {
    for (auto it2 = it->second.begin(); it2 != it->second.end(); ++it2)
    {
      area_id_adhoc.push_back(it2->id);
    }
  }

  sort(area_id_adhoc.begin(), area_id_adhoc.end());
  // we're expecting multiple Area_Blocks for the same pivot id here, hence duplicate removal
  area_id_adhoc.erase(std::unique(area_id_adhoc.begin(), area_id_adhoc.end()), area_id_adhoc.end());

  for (auto it = input_areas.begin();
       it != input_areas.end(); ++it)
  {
    for (auto it2 = it->second.begin(); it2 != it->second.end(); ++it2)
    {
      area_id.push_back(it2->id);

      for (auto it3(it2->used_indices().begin());
          it3 != it2->used_indices().end(); ++it3)
        area_blocks_req.insert(Uint31_Index(*it3));
    }
  }

  sort(area_id.begin(), area_id.end());

  std::set_difference(area_id.begin(), area_id.end(), area_id_adhoc.begin(), area_id_adhoc.end(),
                      std::inserter(area_id_db, area_id_db.begin()));
}

void Area_Query_Statement::get_ranges
    (const std::map< Uint31_Index, std::vector< Way_Skeleton > >& input_ways,
     const std::map< Uint31_Index, std::vector< Area_Skeleton > >& input_areas,
     std::set< Uint31_Index >& area_blocks_req,
     Resource_Manager& rman)
{
  way_areas_id.clear();
  for (auto it = input_ways.begin();
       it != input_ways.end(); ++it)
  {
    for (auto it2 = it->second.begin(); it2 != it->second.end(); ++it2)
    {
      if (!it2->nds().empty() && it2->nds().front() == it2->nds().back())
        way_areas_id.push_back(it2->id);
    }
  }
  std::sort(way_areas_id.begin(), way_areas_id.end());
  
  area_id.clear();
  for (auto it = input_areas.begin();
       it != input_areas.end(); ++it)
  {
    for (auto it2 = it->second.begin(); it2 != it->second.end(); ++it2)
    {
      area_id.push_back(it2->id);

      for (auto it3(it2->used_indices().begin());
          it3 != it2->used_indices().end(); ++it3)
        area_blocks_req.insert(Uint31_Index(*it3));
    }
  }
  std::sort(area_id.begin(), area_id.end());
}


Query_Filter_Strategy Area_Constraint::delivers_data(Resource_Manager& rman)
{
  if (!area->areas_from_input())
    return (area->count_ranges(rman) < 12) ? prefer_ranges : ids_useful;
  else
  {
    const Set* input = rman.get_set(area->get_input());
    if (!input)
      return prefer_ranges;

    // Count the indicies of the input areas
    int counter = 0;

    for (auto it = input->areas.begin();
         it != input->areas.end(); ++it)
    {
      for (auto it2 = it->second.begin(); it2 != it->second.end(); ++it2)
        counter += it2->used_indices().size();
    }

    return (counter <= 12) ? prefer_ranges : ids_useful;
  }
}

namespace {

template< typename Node_Skeleton >
uint32 check_nodes(const std::map< Area_Skeleton::Id_Type, std::vector< Area_Block > > & areas,
                   const std::map< Uint32_Index, std::vector< Node_Skeleton > >& nodes,
                   typename std::map< Uint32_Index, std::vector< Node_Skeleton > >::iterator & nodes_it,
                   IdSetHybrid< Node::Id_Type::Id_Type> & nodes_inside,
                   uint32 current_idx,
                   bool add_border
                 )
{
  uint32 loop_count = 0;

  while (nodes_it != nodes.end() && nodes_it->first.val() < current_idx)
  {
    ++nodes_it;
  }
  while (nodes_it != nodes.end() &&
      (nodes_it->first.val() & 0xffffff00) == current_idx)
  {
    for (typename std::vector< Node_Skeleton >::const_iterator iit = nodes_it->second.begin();
        iit != nodes_it->second.end(); ++iit)
    {
      uint32 ilat((::lat(nodes_it->first.val(), iit->ll_lower)
          + 91.0)*10000000+0.5);
      int32 ilon(::lon(nodes_it->first.val(), iit->ll_lower)*10000000
          + (::lon(nodes_it->first.val(), iit->ll_lower) > 0 ? 0.5 : -0.5));
      for (auto it = areas.begin();
           it != areas.end(); ++it)
      {
        int inside = 0;
        for (auto it2 = it->second.begin(); it2 != it->second.end();
             ++it2)
        {
          ++loop_count;

          int check(Coord_Query_Statement::check_area_block(current_idx, *it2, ilat, ilon));
          if (check == Coord_Query_Statement::HIT && add_border)
          {
            inside = 1;
            break;
          }
          else if (check != 0)
            inside ^= check;
        }
        if (inside)
        {
          nodes_inside.set((*iit).id.val());
          break;
        }
      }
    }
    ++nodes_it;
  }

  return loop_count;

}

template< typename Node_Skeleton >
void filter_by_nodes_inside( std::map< Uint32_Index, std::vector< Node_Skeleton > >& nodes,
                             IdSetHybrid< Node::Id_Type::Id_Type> & nodes_inside)
{
  nodes_inside.sort_unique();

  std::map< Uint32_Index, std::vector< Node_Skeleton > > result;
  for (const auto & node : nodes) {

    std::vector< Node_Skeleton > nds;

    for (const auto & n : node.second) {
      if (nodes_inside.get(n.id.val())) {
        nds.push_back(n);
      }
    }

    if (!nds.empty()) {
      result[node.first].swap(nds);
    }
  }

  nodes.swap(result);
}

}


template< typename Node_Skeleton >
void Area_Query_Statement::collect_nodes_db
    (std::map< Uint32_Index, std::vector< Node_Skeleton > >& nodes,
     const std::set< Uint31_Index >& req,
     IdSetHybrid< Node::Id_Type::Id_Type> & nodes_inside,
     bool add_border,
     Resource_Manager& rman)
{
  if (area_id_db.empty())
    return;

  Block_Backend< Uint31_Index, Area_Block > area_blocks_db
      (rman.get_area_transaction()->data_index(area_settings().AREA_BLOCKS));
  Block_Backend< Uint31_Index, Area_Block >::Discrete_Iterator
      area_it(area_blocks_db.discrete_begin(req.begin(), req.end()));

  auto nodes_it = nodes.begin();

  uint32 loop_count = 0;
  uint32 current_idx = 0;
  while (!(area_it == area_blocks_db.discrete_end()))
  {
    current_idx = area_it.index().val();
    if (loop_count > 1024*1024)
    {
      rman.health_check(*this);
      loop_count = 0;
    }

    std::map< Area_Skeleton::Id_Type, std::vector< Area_Block > > areas;

    while ((!(area_it == area_blocks_db.discrete_end())) &&
        (area_it.index().val() == current_idx))
    {
      if (binary_search(area_id_db.begin(), area_id_db.end(), area_it.handle().id()))
        areas[area_it.object().id].push_back(area_it.object());
      ++area_it;
    }

    // in case nodes are inside, set flag for node id in "nodes_inside"
    loop_count += check_nodes(areas, nodes, nodes_it, nodes_inside, current_idx, add_border);

  }
  while (nodes_it != nodes.end())
  {
    ++nodes_it;
  }

}

template< typename Node_Skeleton >
void Area_Query_Statement::collect_nodes_adhoc
    (std::map< Uint32_Index, std::vector< Node_Skeleton > >& nodes,
     const std::set< Uint31_Index >& req,
     IdSetHybrid< Node::Id_Type::Id_Type> & nodes_inside,
     bool add_border,
     Resource_Manager& rman)
{
  if (area_id_adhoc.empty())
    return;

  const Set * inputset = rman.get_set(get_input());

  if (!inputset)
    return;

  if (inputset->area_blocks.empty())
    return;

  auto nodes_it = nodes.begin();

  uint32 loop_count = 0;
  uint32 current_idx = 0;

  for (const auto & area_blocks_per_index : inputset->area_blocks)
  {
    current_idx = area_blocks_per_index.first.val();

    if (loop_count > 1024*1024)
    {
      rman.health_check(*this);
      loop_count = 0;
    }

    std::map< Area_Skeleton::Id_Type, std::vector< Area_Block > > areas;

    for (const auto & block : area_blocks_per_index.second)
    {
      if (binary_search(area_id_adhoc.begin(), area_id_adhoc.end(), block.id))
        areas[block.id].push_back(block);
    }

    // in case nodes are inside, set flag for node id in "nodes_inside"
    loop_count += check_nodes(areas, nodes, nodes_it, nodes_inside, current_idx, add_border);
  }
  while (nodes_it != nodes.end())
  {
    ++nodes_it;
  }
}

template< typename Node_Skeleton >
void Area_Query_Statement::collect_nodes
    (std::map< Uint32_Index, std::vector< Node_Skeleton > >& nodes,
     const std::set< Uint31_Index >& req, bool add_border,
     Resource_Manager& rman)
{

  IdSetHybrid< Node::Id_Type::Id_Type> nodes_inside;

  // check for on-the-fly Area blocks first
  collect_nodes_adhoc(nodes, req, nodes_inside, add_border, rman);

  collect_nodes_db(nodes, req, nodes_inside, add_border, rman);

  // filter out ways according to ways_inside flag
  filter_by_nodes_inside(nodes, nodes_inside);

}



const int HIT = 1;
const int INTERSECT = 8;


inline int ordered_intersects_inner
    (uint32 lat_a0, uint32 lon_a0, uint32 lat_a1, uint32 lon_a1,
     uint32 lat_b0, uint32 lon_b0, uint32 lat_b1, uint32 lon_b1)
{
  double det = ((double(lat_a1) - lat_a0)*(double(lon_b1) - lon_b0) - (double(lat_b1) - lat_b0)*(double(lon_a1) - lon_a0));
  if (det != 0)
  {
    double lon =
	double(lat_b0 - lat_a0
	    + double(lon_a0)/(double(lon_a1) - lon_a0)*(double(lat_a1) - lat_a0)
	    - double(lon_b0)/(double(lon_b1) - lon_b0)*(double(lat_b1) - lat_b0))
	/det*(double(lon_a1) - lon_a0)*(double(lon_b1) - lon_b0);
    if (lon_a0 < lon && lon < lon_a1 && lon_b0 <= lon && lon <= lon_b1)
      return (((lat_a0 != lat_b0 || lon_a0 != lon_b0) && (lat_a1 != lat_b1 || lon_a1 != lon_b1)) ?
          INTERSECT : 0);
  }
  else if ((fabs(lat_a0 - (double(lon_a0) - lon_b0)/(double(lon_b1) - lon_b0)
                *(double(lat_b1) - lat_b0) - lat_b0) <= 1)
	|| (fabs(lat_a1 - (double(lon_a1) - lon_b0)/(double(lon_b1) - lon_b0)
                *(double(lat_b1) - lat_b0) - lat_b0) <= 1))
    return HIT;

  return 0;
}


inline int ordered_a_intersects_inner
    (uint32 lat_a0, uint32 lon_a0, uint32 lat_a1, uint32 lon_a1,
     uint32 lat_b0, uint32 lon_b0, uint32 lat_b1, uint32 lon_b1)
{
  if (lon_b0 < lon_b1)
  {
    if (lon_a0 < lon_b1 && lon_b0 < lon_a1)
      return ordered_intersects_inner(lat_a0, lon_a0, lat_a1, lon_a1, lat_b0, lon_b0, lat_b1, lon_b1);
  }
  else if (lon_b1 < lon_b0)
  {
    if (lon_a0 < lon_b0 && lon_b1 < lon_a1)
      return ordered_intersects_inner(lat_a0, lon_a0, lat_a1, lon_a1, lat_b1, lon_b1, lat_b0, lon_b0);
  }
  else // lon_b0 == lon_b1
  {
    if (lon_a0 < lon_b0 && lon_b0 < lon_a1)
    {
      double lat = (double(lon_b0) - lon_a0)/(double(lon_a1) - lon_a0)*(double(lat_a1) - lat_a0) + lat_a0;
      return (((lat_b0 < lat && lat < lat_b1) || (lat_b1 < lat && lat < lat_b0)) ?
          INTERSECT : 0);
    }
  }

  return 0;
}


inline int longitude_a_intersects_inner
    (uint32 lat_a0, uint32 lon_a, uint32 lat_a1,
     uint32 lat_b0, uint32 lon_b0, uint32 lat_b1, uint32 lon_b1)
{
  if (lon_b0 < lon_b1)
  {
    if (lon_a < lon_b1 && lon_b0 < lon_a)
    {
      double lat = (double(lon_a) - lon_b0)/(double(lon_b1) - lon_b0)*(double(lat_b1) - lat_b0) + lat_b0;
      return (((lat_a0 < lat && lat < lat_a1) || (lat_a1 < lat && lat < lat_a0)) ? INTERSECT : 0);
    }
  }
  else if (lon_b1 < lon_b0)
  {
    if (lon_a < lon_b0 && lon_b1 < lon_a)
    {
      double lat = (double(lon_a) - lon_b0)/(double(lon_b1) - lon_b0)*(double(lat_b1) - lat_b0) + lat_b0;
      return (((lat_a0 < lat && lat < lat_a1) || (lat_a1 < lat && lat < lat_a0)) ? INTERSECT : 0);
    }
  }
  else // lon_b0 == lon_b1
  {
    if (lon_a != lon_b0)
      return 0;
    if (lat_a0 < lat_a1)
    {
      if (lat_b0 < lat_b1)
        return ((lat_a0 < lat_b1 && lat_b0 < lat_a1) ? HIT : 0);
      else
        return ((lat_a0 < lat_b0 && lat_b1 < lat_a1) ? HIT : 0);
    }
    else
    {
      if (lat_b0 < lat_b1)
        return ((lat_a1 < lat_b1 && lat_b0 < lat_a0) ? HIT : 0);
      else
        return ((lat_a1 < lat_b0 && lat_b1 < lat_a0) ? HIT : 0);
    }
  }

  return 0;
}


int intersects_inner(const Area_Block& string_a, const Area_Block& string_b)
{
// TODO: why is this uint32, uint32, rather than uint32, int32???

//  std::vector< std::pair< uint32, uint32 > > coords_a;
//  for (std::vector< uint64 >::const_iterator it = string_a.coors.begin(); it != string_a.coors.end(); ++it)
//    coords_a.push_back(std::make_pair(::ilat(uint32((*it>>32)&0xff), uint32(*it & 0xffffffffull)),
//				 ::ilon(uint32((*it>>32)&0xff), uint32(*it & 0xffffffffull))));
//
//  std::vector< std::pair< uint32, uint32 > > coords_b;
//  for (std::vector< uint64 >::const_iterator it = string_b.coors.begin(); it != string_b.coors.end(); ++it)
//    coords_b.push_back(std::make_pair(::ilat(uint32((*it>>32)&0xff), uint32(*it & 0xffffffffull)),
//				 ::ilon(uint32((*it>>32)&0xff), uint32(*it & 0xffffffffull))));

  const std::vector< std::pair< uint32, int32 > >& coords_a = string_a.get_ilat_ilon_pairs();
  const std::vector< std::pair< uint32, int32 > >& coords_b = string_b.get_ilat_ilon_pairs();

  // TODO: why is this uint32, uint32, rather than uint32, int32???

  for (std::vector< std::pair< uint32, uint32 > >::size_type i = 0; i < coords_a.size()-1; ++i)
  {
    if (coords_a[i].second < coords_a[i+1].second)
    {
      for (std::vector< std::pair< uint32, uint32 > >::size_type j = 0; j < coords_b.size()-1; ++j)
      {
	int result = ordered_a_intersects_inner
	    (coords_a[i].first, coords_a[i].second, coords_a[i+1].first, coords_a[i+1].second,
	     coords_b[j].first, coords_b[j].second, coords_b[j+1].first, coords_b[j+1].second);
        if (result)
	  return result;
      }
    }
    else if (coords_a[i+1].second < coords_a[i].second)
    {
      for (std::vector< std::pair< uint32, uint32 > >::size_type j = 0; j < coords_b.size()-1; ++j)
      {
	int result = ordered_a_intersects_inner
	    (coords_a[i+1].first, coords_a[i+1].second, coords_a[i].first, coords_a[i].second,
	     coords_b[j].first, coords_b[j].second, coords_b[j+1].first, coords_b[j+1].second);
        if (result)
          return result;
      }
    }
    else
      for (std::vector< std::pair< uint32, uint32 > >::size_type j = 0; j < coords_b.size()-1; ++j)
      {
	int result = longitude_a_intersects_inner
	    (coords_a[i].first, coords_a[i].second, coords_a[i+1].first,
	     coords_b[j].first, coords_b[j].second, coords_b[j+1].first, coords_b[j+1].second);
        if (result)
          return result;
      }
  }

  return 0;
}


void has_inner_points(const Area_Block& string_a, const Area_Block& string_b, int& inside)
{
  // TODO: why is this uint32, uint32, rather than uint32, int32???

//  std::vector< std::pair< uint32, uint32 > > coords_a;
//  for (std::vector< uint64 >::const_iterator it = string_a.coors.begin(); it != string_a.coors.end(); ++it)
//    coords_a.push_back(std::make_pair(::ilat(uint32((*it>>32)&0xff), uint32(*it & 0xffffffffull)),
//                                 ::ilon(uint32((*it>>32)&0xff), uint32(*it & 0xffffffffull))));

  const std::vector< std::pair< uint32, int32 > >& coords_a = string_a.get_ilat_ilon_pairs();

  // Check additionally the middle of the segment to also get segments
  // that run through the area
  for (std::vector< std::pair< uint32, uint32 > >::size_type i = 0; i < coords_a.size()-1; ++i)
  {
    uint32 ilat = (coords_a[i].first + coords_a[i+1].first)/2;
    uint32 ilon = (coords_a[i].second + coords_a[i+1].second)/2 + 0x80000000u;
    int check = Coord_Query_Statement::check_area_block(0, string_b, ilat, ilon);
    if (check & Coord_Query_Statement::HIT)
      inside = check;
    else if (check)
      inside ^= check;
  }
}

namespace {

int check_nodes_for_ways(const std::map< Area_Skeleton::Id_Type, std::vector< Area_Block > > & areas,
                std::map< Way::Id_Type, bool > & ways_inside,
                std::map< uint32, std::vector< std::pair< uint32, Way::Id_Type > > >::const_iterator & nodes_it,
                const std::map< uint32, std::vector< std::pair< uint32, Way::Id_Type > > > & way_coords_to_id,
                const uint32 current_idx)
{
  int loop_count = 0;

  while (nodes_it != way_coords_to_id.end() && nodes_it->first < current_idx)
    ++nodes_it;
  while (nodes_it != way_coords_to_id.end() &&
      (nodes_it->first & 0xffffff00) == current_idx)
  {
    std::vector< std::pair< uint32, Way::Id_Type > > into;
    for (auto iit = nodes_it->second.begin();
        iit != nodes_it->second.end(); ++iit)
    {
      uint32 ilat = ::ilat(nodes_it->first, iit->first);
      int32 ilon = ::ilon(nodes_it->first, iit->first);
      for (auto it = areas.begin();
           it != areas.end(); ++it)
      {
        int inside = 0;
        for (auto it2 = it->second.begin(); it2 != it->second.end();
             ++it2)
        {
          ++loop_count;

          int check(Coord_Query_Statement::check_area_block(current_idx, *it2, ilat, ilon));
          if (check == Coord_Query_Statement::HIT)
          {
            inside = Coord_Query_Statement::HIT;
            break;
          }
          else
            inside ^= check;
        }
        if (inside & (Coord_Query_Statement::TOGGLE_EAST | Coord_Query_Statement::TOGGLE_WEST))
          ways_inside[iit->second] = true;
      }
    }
    ++nodes_it;

  }
  return loop_count;
}


void check_segments_for_ways(const std::map< Area_Skeleton::Id_Type, std::vector< Area_Block > > & areas,
                    const std::map< Uint31_Index, std::vector< Area_Block > > & way_segments,
                    std::map< Way::Id_Type, bool > & ways_inside,
                    uint32 current_idx,
                    bool add_border)
{
  // check segments
  const auto & area_blocks_it = way_segments.find(Uint31_Index(current_idx));

  if (area_blocks_it == way_segments.end())
    return;

  const auto & area_blocks = area_blocks_it->second;

  for (auto sit = area_blocks.begin();
       sit != area_blocks.end(); ++sit)
  {
    std::map< Area::Id_Type, int > area_status;
    for (auto it = areas.begin();
         it != areas.end(); ++it)
    {
      if (ways_inside[Way::Id_Type(sit->id)])
        break;
      for (auto it2 = it->second.begin(); it2 != it->second.end();
           ++it2)
      {
        // If an area segment intersects this way segment in the inner of the way,
        // the way is contained in the area.
        // The endpoints are properly handled via the point-in-area test
        // Check additionally the middle of the segment to also get segments
        // that run through the area
        int intersect = intersects_inner(*sit, *it2);
        if (intersect == Coord_Query_Statement::INTERSECT)
        {
          ways_inside[Way::Id_Type(sit->id)] = true;
          break;
        }
        else if (intersect == Coord_Query_Statement::HIT)
        {
          if (add_border)
            ways_inside[Way::Id_Type(sit->id)] = true;
          else
            area_status[it2->id] = Coord_Query_Statement::HIT;
          break;
        }
        has_inner_points(*sit, *it2, area_status[it2->id]);
      }
    }
    for (std::map< Area::Id_Type, int >::const_iterator it = area_status.begin(); it != area_status.end(); ++it)
    {
      if ((it->second && (!(it->second & Coord_Query_Statement::HIT))) ||
          (it->second && add_border))
        ways_inside[Way::Id_Type(sit->id)] = true;
    }
  }

}

template <typename Way_Skeleton>
void filter_by_ways_inside(std::map< Uint31_Index, std::vector< Way_Skeleton > >& ways,
                           std::map< Way::Id_Type, bool > & ways_inside)

{
  std::map< Uint31_Index, std::vector< Way_Skeleton > > result;

  // Mark ways as found that intersect the area border
  for (auto it = ways.begin();
       it != ways.end(); ++it)
  {
    std::vector< Way_Skeleton > cur_result;
    for (auto it2 = it->second.begin(); it2 != it->second.end(); ++it2)
    {
      if (ways_inside[it2->id])
      {
        cur_result.push_back(*it2);
        it2->id = Way::Id_Type(0u);
      }
    }
    result[it->first].swap(cur_result);
  }

  result.swap(ways);
}

}


void Area_Query_Statement::collect_ways_db
      (const std::set< Uint31_Index >& req,
       const std::map< Uint31_Index, std::vector< Area_Block > > & way_segments,
       const std::map< uint32, std::vector< std::pair< uint32, Way::Id_Type > > > & way_coords_to_id,
       std::map< Way::Id_Type, bool > & ways_inside,
       bool add_border,
       Resource_Manager& rman)
{

  if (area_id_db.empty())
    return;

  auto nodes_it = way_coords_to_id.begin();

  Block_Backend< Uint31_Index, Area_Block > area_blocks_db
      (rman.get_area_transaction()->data_index(area_settings().AREA_BLOCKS));
  Block_Backend< Uint31_Index, Area_Block >::Discrete_Iterator
      area_it(area_blocks_db.discrete_begin(req.begin(), req.end()));

  // Fill node_status with the area related status of each node and segment
  uint32 loop_count = 0;
  uint32 current_idx = 0;
  while (!(area_it == area_blocks_db.discrete_end()))
  {
    current_idx = area_it.index().val();
    if (loop_count > 64*1024)
    {
      rman.health_check(*this);
      loop_count = 0;
    }

    std::map< Area_Skeleton::Id_Type, std::vector< Area_Block > > areas;
    while ((!(area_it == area_blocks_db.discrete_end())) &&
        (area_it.index().val() == current_idx))
    {
      if (binary_search(area_id_db.begin(), area_id_db.end(), area_it.handle().id()))
        areas[area_it.object().id].push_back(area_it.object());
      ++area_it;
    }

    // check nodes
    loop_count += check_nodes_for_ways(areas, ways_inside, nodes_it, way_coords_to_id, current_idx);

    // check segments
    check_segments_for_ways(areas, way_segments, ways_inside, current_idx, add_border);
  }
}

void Area_Query_Statement::collect_ways_adhoc
      (const std::map< Uint31_Index, std::vector< Area_Block > > & way_segments,
       const std::map< uint32, std::vector< std::pair< uint32, Way::Id_Type > > > & way_coords_to_id,
       std::map< Way::Id_Type, bool > & ways_inside,
       bool add_border,
       Resource_Manager& rman)
{
  if (area_id_adhoc.empty())
    return;

  const Set * inputset = rman.get_set(get_input());

  if (!inputset)
    return;

  if (inputset->area_blocks.empty())
    return;

  auto nodes_it = way_coords_to_id.begin();

  // Fill node_status with the area related status of each node and segment
  uint32 loop_count = 0;
  uint32 current_idx = 0;

  for (const auto & area_blocks_per_index : inputset->area_blocks)
  {
    current_idx = area_blocks_per_index.first.val();

    if (loop_count > 1024*1024)
    {
      rman.health_check(*this);
      loop_count = 0;
    }

    std::map< Area_Skeleton::Id_Type, std::vector< Area_Block > > areas;

    for (const auto & block : area_blocks_per_index.second)
    {
      if (binary_search(area_id_adhoc.begin(), area_id_adhoc.end(), block.id))
        areas[block.id].push_back(block);
    }

    // check nodes
    loop_count += check_nodes_for_ways(areas, ways_inside, nodes_it, way_coords_to_id, current_idx);

    // check segments
    check_segments_for_ways(areas, way_segments, ways_inside, current_idx, add_border);
  }
}

template< typename Way_Skeleton >
void Area_Query_Statement::collect_ways
      (const Way_Geometry_Store& way_geometries,
       std::map< Uint31_Index, std::vector< Way_Skeleton > >& ways,
       const std::set< Uint31_Index >& req, bool add_border,
       const Statement& query, Resource_Manager& rman)
{

  std::map< Uint31_Index, std::vector< Area_Block > > way_segments;
  for (auto it = ways.begin(); it != ways.end(); ++it)
  {
    for (auto it2 = it->second.begin(); it2 != it->second.end(); ++it2)
      add_way_to_area_blocks(way_geometries.get_geometry(*it2), it2->id.val(), way_segments);
  }

  std::map< uint32, std::vector< std::pair< uint32, Way::Id_Type > > > way_coords_to_id;
  for (auto it = ways.begin(); it != ways.end(); ++it)
  {
    for (auto it2 = it->second.begin(); it2 != it->second.end(); ++it2)
    {
      std::vector< Quad_Coord > coords = way_geometries.get_geometry(*it2);
      for (std::vector< Quad_Coord >::const_iterator it3 = coords.begin(); it3 != coords.end(); ++it3)
        way_coords_to_id[it3->ll_upper].push_back(std::make_pair(it3->ll_lower, it2->id));
    }
  }

  std::map< Way::Id_Type, bool > ways_inside;

  // check for ad hoc Area blocks first
  // method updates ways_inside to indicate which way ids are inside either the ad-hoc or (further down) the db based areas
  collect_ways_adhoc(way_segments, way_coords_to_id, ways_inside, add_border, rman);

  collect_ways_db(req, way_segments, way_coords_to_id, ways_inside, add_border, rman);

  // filter out ways according to ways_inside flag
  filter_by_ways_inside(ways, ways_inside);
}


void Area_Query_Statement::execute(Resource_Manager& rman)
{
  Set into;

  Area_Constraint constraint(*this);
  std::set< std::pair< Uint32_Index, Uint32_Index > > ranges;
  constraint.get_ranges(rman, ranges);
  get_elements_by_id_from_db< Uint32_Index, Node_Skeleton >
      (into.nodes, into.attic_nodes,
       std::vector< Node::Id_Type >(), false, ranges, 0, *this, rman,
       *osm_base_settings().NODES, *attic_settings().NODES);
  constraint.filter(rman, into);
  filter_attic_elements(rman, rman.get_desired_timestamp(), into.nodes, into.attic_nodes);
  constraint.filter(*this, rman, into);

  transfer_output(rman, into);
  rman.health_check(*this);
}


Query_Constraint* Area_Query_Statement::get_query_constraint()
{
  constraints.push_back(new Area_Constraint(*this));
  return constraints.back();
}

bool Area_Query_Statement::accept(Statement_Visitor& visitor) { return visitor.visit(*this); }
bool Area_Query_Statement::accept(const Statement_Visitor& visitor) const  { return visitor.visit(*this); }
