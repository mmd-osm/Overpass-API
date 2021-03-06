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

#include "../../template_db/block_backend.h"
#include "../../template_db/random_file.h"
#include "../core/settings.h"
#include "../data/collect_members.h"
#include "around.h"
#include "recurse.h"


#include <algorithm>
#include <cmath>
#include <fstream>
#include <sstream>
#include <tuple>
#include <vector>


//-----------------------------------------------------------------------------

namespace {

template < class TIndex, class TObject >
std::set< std::pair< TIndex, TIndex > > ranges(const std::map< TIndex, std::vector< TObject > >& elems)
{
  std::set< std::pair< TIndex, TIndex > > result;
  if (elems.empty())
    return result;
  std::pair< TIndex, TIndex > range = std::make_pair(elems.begin()->first, inc(elems.begin()->first));
  for (typename std::map< TIndex, std::vector< TObject > >::const_iterator
      it = elems.begin(); it != elems.end(); ++it)
  {
    if (!(range.second < it->first))
      range.second = inc(it->first);
    else
    {
      result.insert(range);
      range = std::make_pair(it->first, inc(it->first));
    }
  }
  result.insert(range);

  return result;
}


std::set< std::pair< Uint32_Index, Uint32_Index > > ranges(double lat, double lon)
{
  Uint32_Index idx = ::ll_upper_(lat, lon);
  std::set< std::pair< Uint32_Index, Uint32_Index > > result;
  result.insert(std::make_pair(idx, inc(idx)));
  return result;
}

template < class TIndex >
std::set< std::pair< TIndex, TIndex > > condense_ranges(const std::set< std::pair< TIndex, TIndex > >& temp_ranges)
{
  std::set< std::pair< TIndex, TIndex > > result;
  if (temp_ranges.empty())
    return result;

  typename std::set< std::pair< TIndex, TIndex > >::const_iterator it = temp_ranges.begin();
  TIndex last_first = it->first;
  TIndex last_second = it->second;
  ++it;
  for (; it != temp_ranges.end(); ++it)
  {
    if (last_second < it->first)
    {
      result.insert(std::make_pair(last_first, last_second));
      last_first = it->first;
    }
    if (last_second < it->second)
      last_second = it->second;
  }
  result.insert(std::make_pair(last_first, last_second));

  return result;
}


template < class TIndex >
std::set< std::pair< TIndex, TIndex > > set_union_(std::set< std::pair< TIndex, TIndex > >&& a,
					           std::set< std::pair< TIndex, TIndex > >&& b)
{
  std::set< std::pair< TIndex, TIndex > > temp;

  if (a.size() > b.size()) {
    temp = std::move(a);
    temp.insert(b.begin(), b.end());
  } else {
    temp = std::move(b);
    temp.insert(a.begin(), a.end());
  }

  return condense_ranges(temp);
}

std::set< std::pair< Uint32_Index, Uint32_Index > > blockwise_split
    (const std::set< std::pair< Uint32_Index, Uint32_Index > >& idxs)
{
  std::set< std::pair< Uint32_Index, Uint32_Index > > result;

  for (std::set< std::pair< Uint32_Index, Uint32_Index > >::const_iterator it = idxs.begin();
      it != idxs.end(); ++it)
  {
    uint32 start = it->first.val();
    while (start < it->second.val())
    {
      uint32 end;
      if ((start & 0x3) != 0 || it->second.val() < start + 0x4)
	end = start + 1;
      else if ((start & 0x3c) != 0 || it->second.val() < start + 0x40)
	end = start + 0x4;
      else if ((start & 0x3c0) != 0 || it->second.val() < start + 0x400)
	end = start + 0x40;
      else if ((start & 0x3c00) != 0 || it->second.val() < start + 0x4000)
	end = start + 0x400;
      else if ((start & 0x3c000) != 0 || it->second.val() < start + 0x40000)
	end = start + 0x4000;
      else if ((start & 0x3c0000) != 0 || it->second.val() < start + 0x400000)
	end = start + 0x40000;
      else if ((start & 0x3c00000) != 0 || it->second.val() < start + 0x4000000)
	end = start + 0x400000;
      else
	end = start + 0x4000000;

      result.insert(std::make_pair(Uint32_Index(start), Uint32_Index(end)));
      start = end;
    }
  }

  return result;
}

std::set< std::pair< Uint32_Index, Uint32_Index > > calc_ranges_
    (double south, double north, double west, double east)
{
  std::vector< std::pair< uint32, uint32 > > ranges = calc_ranges(south, north, west, east);

  std::set< std::pair< Uint32_Index, Uint32_Index > > result;
  for (std::vector< std::pair< uint32, uint32 > >::const_iterator it = ranges.begin();
      it != ranges.end(); ++it)
    result.insert(std::make_pair(Uint32_Index(it->first), Uint32_Index(it->second)));

  return result;
}

std::set< std::pair< Uint32_Index, Uint32_Index > > expand
    (const std::set< std::pair< Uint32_Index, Uint32_Index > >& idxs, double radius)
{
  std::set< std::pair< Uint32_Index, Uint32_Index > > blockwise_idxs = blockwise_split(idxs);

  std::set< std::pair< Uint32_Index, Uint32_Index > > result;
  for (std::set< std::pair< Uint32_Index, Uint32_Index > >::const_iterator it = blockwise_idxs.begin();
      it != blockwise_idxs.end(); ++it)
  {
    double south = ::lat(it->first.val(), 0) - radius*(90.0/10/1000/1000);
    double north = ::lat(dec(it->second).val(), 0xffffffff) + radius*(90.0/10/1000/1000);
    double lon_factor = cos((-north < south ? north : south)*(acos(0)/90.0));
    double west = ::lon(it->first.val(), 0) - radius*(90.0/10/1000/1000)/lon_factor;
    double east = ::lon(dec(it->second).val(), 0xffffffff)
        + radius*(90.0/10/1000/1000)/lon_factor;

    auto ranges = calc_ranges_(south, north, west, east);

    result.insert(ranges.begin(), ranges.end());
  }

  result = condense_ranges(result);

  return result;
}

std::set< std::pair< Uint32_Index, Uint32_Index > > children
    (const std::set< std::pair< Uint31_Index, Uint31_Index > >& way_rel_idxs)
{
  std::set< std::pair< Uint32_Index, Uint32_Index > > result;

  std::vector< std::pair< uint32, uint32 > > ranges;

  for (std::set< std::pair< Uint31_Index, Uint31_Index > >::const_iterator it = way_rel_idxs.begin();
      it != way_rel_idxs.end(); ++it)
  {
    for (Uint31_Index idx = it->first; idx < it->second; idx = inc(idx))
    {
      if (idx.val() & 0x80000000)
      {
	uint32 lat = 0;
	uint32 lon = 0;
	uint32 offset = 0;

	if (idx.val() & 0x00000001)
	{
	  lat = upper_ilat(idx.val() & 0x2aaaaaa8);
	  lon = upper_ilon(idx.val() & 0x55555554);
	  offset = 2;
	}
	else if (idx.val() & 0x00000002)
	{
	  lat = upper_ilat(idx.val() & 0x2aaaaa80);
	  lon = upper_ilon(idx.val() & 0x55555540);
	  offset = 8;
	}
	else if (idx.val() & 0x00000004)
	{
	  lat = upper_ilat(idx.val() & 0x2aaaa800);
	  lon = upper_ilon(idx.val() & 0x55555400);
	  offset = 0x20;
	}
	else if (idx.val() & 0x00000008)
	{
	  lat = upper_ilat(idx.val() & 0x2aaa8000);
	  lon = upper_ilon(idx.val() & 0x55554000);
	  offset = 0x80;
	}
	else if (idx.val() & 0x00000010)
	{
	  lat = upper_ilat(idx.val() & 0x2aa80000);
	  lon = upper_ilon(idx.val() & 0x55540000);
	  offset = 0x200;
	}
	else if (idx.val() & 0x00000020)
	{
	  lat = upper_ilat(idx.val() & 0x2a800000);
	  lon = upper_ilon(idx.val() & 0x55400000);
	  offset = 0x800;
	}
	else if (idx.val() & 0x00000040)
	{
	  lat = upper_ilat(idx.val() & 0x28000000);
	  lon = upper_ilon(idx.val() & 0x54000000);
	  offset = 0x2000;
	}
	else // idx.val() == 0x80000080
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
      }
      else
	ranges.push_back(std::make_pair(idx.val(), idx.val() + 1));
    }
  }

  sort(ranges.begin(), ranges.end());
  uint32 pos = 0;
  for (std::vector< std::pair< uint32, uint32 > >::const_iterator it = ranges.begin();
      it != ranges.end(); ++it)
  {
    if (pos < it->first)
      pos = it->first;
    result.insert(std::make_pair(Uint32_Index(pos), Uint32_Index(it->second)));
  }

  return result;
}

}

//-----------------------------------------------------------------------------

Prepared_BBox::Prepared_BBox()
{
  min_lat =  100.0;
  max_lat = -100.0;
  min_lon =  200.0;
  max_lon = -200.0;
}

inline void Prepared_BBox::merge(Prepared_BBox & bbox)
{
  min_lat = std::min(min_lat, bbox.min_lat);
  max_lat = std::max(max_lat, bbox.max_lat);
  min_lon = std::min(min_lon, bbox.min_lon);
  max_lon = std::max(max_lon, bbox.max_lon);
}

inline bool Prepared_BBox::intersects(const Prepared_BBox & bbox) const
{
  bool intersects;
  // skip bbox based test when crossing date line
  if (!(min_lat <= max_lat &&
        min_lon <= max_lon))
    return true;

  intersects = !( bbox.max_lat + 1e-8 < min_lat ||
                  bbox.min_lat - 1e-8 > max_lat ||
                  bbox.max_lon + 1e-8 < min_lon ||
                  bbox.min_lon - 1e-8 > max_lon );
  return intersects;
}

inline bool Prepared_BBox::intersects(const std::vector < Prepared_BBox > & bboxes) const
{
 for (std::vector< Prepared_BBox >::const_iterator it = bboxes.begin(); it != bboxes.end(); ++it)
  if (intersects(*it))
     return true;
 return false;
}

std::ostream& operator << (std::ostream &o, const Prepared_BBox &b)
{
  o << std::fixed << std::setprecision(7)
    << " min_lat: " << b.min_lat
    << " min_lon: " << b.min_lon
    << " max_lat: " << b.max_lat
    << " max_lon: " << b.max_lon
    << std::endl;
  return o;
}

namespace {

inline Prepared_BBox lat_lon_bbox(double lat, double lon)
{
  Prepared_BBox bbox;
  bbox.min_lat = bbox.max_lat = lat;
  bbox.min_lon = bbox.max_lon = lon;
  return bbox;
}

inline Prepared_BBox lat_lon_bbox(double lat1, double lon1, double lat2, double lon2)
{
  Prepared_BBox bbox_result = ::lat_lon_bbox(lat1, lon1);
  Prepared_BBox bbox_second = ::lat_lon_bbox(lat2, lon2);
  bbox_result.merge(bbox_second);
  return bbox_result;
}

inline Prepared_BBox way_geometry_bbox(const std::vector< Quad_Coord >& way_geometry)
{
  Prepared_BBox bbox;
  std::vector< Quad_Coord >::const_iterator nit = way_geometry.begin();
  if (nit == way_geometry.end())
    return bbox;

  for (std::vector< Quad_Coord >::const_iterator it = way_geometry.begin(); it != way_geometry.end(); ++it)
  {
    double lat(::lat(it->ll_upper, it->ll_lower));
    double lon(::lon(it->ll_upper, it->ll_lower));
    Prepared_BBox bbox_node = lat_lon_bbox(lat, lon);
    bbox.merge(bbox_node);
  }
  return bbox;
}

inline Prepared_BBox calc_distance_bbox(double lat, double lon, double dist)
{
// see: http://janmatuschek.de/LatitudeLongitudeBoundingCoordinates

  Prepared_BBox bbox;

  if (dist == 0)
  {
    bbox.min_lat = bbox.max_lat = lat;
    bbox.min_lon = bbox.max_lon = lon;
    return bbox;
  }

  double min_lon_rad, max_lon_rad;

  double dist_rad = dist / (10.0 * 1000.0 * 1000.0 / acos(0));

  double lat_rad = lat * acos(0) / 90.0;
  double lon_rad = lon * acos(0) / 90.0;
  double min_lat_rad = lat_rad - dist_rad;
  double max_lat_rad = lat_rad + dist_rad;

  if (min_lat_rad > (-90.0 * acos(0) / 90.0)
      && max_lat_rad < (90.0 * acos(0) / 90.0))
  {
    double lon_delta_rad = asin(sin(dist_rad) / cos(lat_rad));
    min_lon_rad = lon_rad - lon_delta_rad;
    if (min_lon_rad < (-180.0 * acos(0) / 90.0))
      min_lon_rad += 2.0 * 2.0 * acos(0);
    max_lon_rad = lon_rad + lon_delta_rad;
    if (max_lon_rad > (180.0 * acos(0) / 90.0))
      max_lon_rad -= 2.0 * 2.0 * acos(0);
  }
  else
  {  // pole within dist
    min_lat_rad = std::max(min_lat_rad, -90.0 * acos(0) / 90.0);
    max_lat_rad = std::min(max_lat_rad, 90.0 * acos(0) / 90.0);
    min_lon_rad = -180.0 * acos(0) / 90.0;
    max_lon_rad = 180.0 * acos(0) / 90.0;
  }

  bbox.min_lat = min_lat_rad * 90.0 / acos(0);
  bbox.max_lat = max_lat_rad * 90.0 / acos(0);
  bbox.min_lon = min_lon_rad * 90.0 / acos(0);
  bbox.max_lon = max_lon_rad * 90.0 / acos(0);
  return bbox;
}

}

//-----------------------------------------------------------------------------

class Around_Constraint final : public Query_Constraint
{
  public:
    Around_Constraint(Around_Statement& around_) : around(&around_), ranges_used(false) {}

    Query_Filter_Strategy delivers_data(Resource_Manager& rman)
    { return (around->get_radius() < 2000) ? prefer_ranges : ids_useful; }

    bool get_ranges
        (Resource_Manager& rman, std::set< std::pair< Uint32_Index, Uint32_Index > >& ranges);
    bool get_ranges
        (Resource_Manager& rman, std::set< std::pair< Uint31_Index, Uint31_Index > >& ranges);
    void filter(Resource_Manager& rman, Set& into);
    void filter(const Statement& query, Resource_Manager& rman, Set& into);
    virtual ~Around_Constraint() {}
  private:
    std::ostream& print_constraint( std::ostream &os ) const override {
      return os << (around != nullptr ? around->dump_ql_in_query("") : "around");
    }

    Around_Statement* around;
    bool ranges_used;
};


bool Around_Constraint::get_ranges
    (Resource_Manager& rman, std::set< std::pair< Uint32_Index, Uint32_Index > >& ranges)
{
  ranges_used = true;

  const Set* input = rman.get_set(around->get_source_name());
  ranges = around->calc_ranges(input ? *input : Set(), rman);
  return true;
}


bool Around_Constraint::get_ranges
    (Resource_Manager& rman, std::set< std::pair< Uint31_Index, Uint31_Index > >& ranges)
{
  std::set< std::pair< Uint32_Index, Uint32_Index > > node_ranges;
  this->get_ranges(rman, node_ranges);
  ranges = calc_parents(node_ranges);
  return true;
}


void Around_Constraint::filter(Resource_Manager& rman, Set& into)
{
  {
    std::set< std::pair< Uint32_Index, Uint32_Index > > ranges;
    get_ranges(rman, ranges);

    std::set< std::pair< Uint32_Index, Uint32_Index > >::const_iterator ranges_it = ranges.begin();
    std::map< Uint32_Index, std::vector< Node_Skeleton > >::iterator nit = into.nodes.begin();
    for (; nit != into.nodes.end() && ranges_it != ranges.end(); )
    {
      if (!(nit->first < ranges_it->second))
	  ++ranges_it;
      else if (!(nit->first < ranges_it->first))
	  ++nit;
      else
      {
	  nit->second.clear();
	  ++nit;
      }
    }
    for (; nit != into.nodes.end(); ++nit)
      nit->second.clear();

    ranges_it = ranges.begin();
    std::map< Uint32_Index, std::vector< Attic< Node_Skeleton > > >::iterator it = into.attic_nodes.begin();
    for (; it != into.attic_nodes.end() && ranges_it != ranges.end(); )
    {
      if (!(it->first < ranges_it->second))
	  ++ranges_it;
      else if (!(it->first < ranges_it->first))
	  ++it;
      else
      {
	  it->second.clear();
	  ++it;
      }
    }
    for (; it != into.attic_nodes.end(); ++it)
      it->second.clear();
  }

  std::set< std::pair< Uint31_Index, Uint31_Index > > ranges;
  get_ranges(rman, ranges);

  // pre-process ways to reduce the load of the expensive filter
  // pre-filter ways
  filter_ways_by_ranges(into.ways, ranges);
  filter_ways_by_ranges(into.attic_ways, ranges);

  // pre-filter relations
  filter_relations_by_ranges(into.relations, ranges);
  filter_relations_by_ranges(into.attic_relations, ranges);

  ranges_used = false;

  //TODO: areas
}


template< typename Node_Skeleton >
void filter_nodes_expensive(const Around_Statement& around,
                            std::map< Uint32_Index, std::vector< Node_Skeleton > >& nodes)
{
  for (typename std::map< Uint32_Index, std::vector< Node_Skeleton > >::iterator it = nodes.begin();
      it != nodes.end(); ++it)
  {
    std::vector< Node_Skeleton > local_into;
    for (typename std::vector< Node_Skeleton >::const_iterator iit = it->second.begin();
        iit != it->second.end(); ++iit)
    {
      double lat(::lat(it->first.val(), iit->ll_lower));
      double lon(::lon(it->first.val(), iit->ll_lower));
      if (around.matches_bboxes(lat, lon) && around.is_inside(lat, lon))
	local_into.push_back(*iit);
    }
    it->second.swap(local_into);
  }
}


template< typename Way_Skeleton >
void filter_ways_expensive(const Around_Statement& around,
                           const Way_Geometry_Store& way_geometries,
                           std::map< Uint31_Index, std::vector< Way_Skeleton > >& ways)
{
  for (typename std::map< Uint31_Index, std::vector< Way_Skeleton > >::iterator it = ways.begin();
      it != ways.end(); ++it)
  {
    std::vector< Way_Skeleton > local_into;
    for (typename std::vector< Way_Skeleton >::const_iterator iit = it->second.begin();
        iit != it->second.end(); ++iit)
    {
      const std::vector< Quad_Coord >& way_geometry = way_geometries.get_geometry(*iit);
      if (around.matches_bboxes(::way_geometry_bbox(way_geometry)) &&
          around.is_inside(way_geometry))
	local_into.push_back(*iit);
    }
    it->second.swap(local_into);
  }
}


template< typename Relation_Skeleton >
void filter_relations_expensive(const Around_Statement& around,
                                const std::vector< std::pair< Uint32_Index, const Node_Skeleton* > > node_members_by_id,
                                const std::vector< std::pair< Uint31_Index, const Way_Skeleton* > > way_members_by_id,
                                const Way_Geometry_Store& way_geometries,
                                std::map< Uint31_Index, std::vector< Relation_Skeleton > >& relations)
{
  for (typename std::map< Uint31_Index, std::vector< Relation_Skeleton > >::iterator it = relations.begin();
      it != relations.end(); ++it)
  {
    std::vector< Relation_Skeleton > local_into;
    for (typename std::vector< Relation_Skeleton >::const_iterator iit = it->second.begin();
        iit != it->second.end(); ++iit)
    {
      for (std::vector< Relation_Entry >::const_iterator nit = iit->members().begin();
	  nit != iit->members().end(); ++nit)
      {
	if (nit->type == Relation_Entry::NODE)
	{
          const std::pair< Uint32_Index, const Node_Skeleton* >* second_nd =
              binary_search_for_pair_id(node_members_by_id, nit->ref);
	  if (!second_nd)
	    continue;
	  double lat(::lat(second_nd->first.val(), second_nd->second->ll_lower));
	  double lon(::lon(second_nd->first.val(), second_nd->second->ll_lower));


	  if (around.matches_bboxes(lat, lon) &&
	      around.is_inside(lat, lon))
	  {
	    local_into.push_back(*iit);
	    break;
	  }
	}
	else if (nit->type == Relation_Entry::WAY)
	{
          const std::pair< Uint31_Index, const Way_Skeleton* >* second_nd =
              binary_search_for_pair_id(way_members_by_id, nit->ref32());
	  if (!second_nd)
	    continue;
	  const std::vector< Quad_Coord >& way_geometry = way_geometries.get_geometry(*second_nd->second);
	  if (around.matches_bboxes(::way_geometry_bbox(way_geometry)) &&
	      around.is_inside(way_geometry))
	  {
	    local_into.push_back(*iit);
	    break;
	  }
	}
      }
    }
    it->second.swap(local_into);
  }
}


void Around_Constraint::filter(const Statement& query, Resource_Manager& rman, Set& into)
{
  const Set* input = rman.get_set(around->get_source_name());
  around->calc_lat_lons(input ? *input : Set(), *around, rman);

  filter_nodes_expensive(*around, into.nodes);
  filter_ways_expensive(*around, Way_Geometry_Store(into.ways, query, rman), into.ways);

  {
    //Process relations

    // Retrieve all node and way members referred by the relations.
    std::set< std::pair< Uint32_Index, Uint32_Index > > node_ranges;
    get_ranges(rman, node_ranges);

    std::map< Uint32_Index, std::vector< Node_Skeleton > > node_members
        = relation_node_members(&query, rman, into.relations, &node_ranges);
    std::vector< std::pair< Uint32_Index, const Node_Skeleton* > > node_members_by_id
        = order_by_id(node_members, Order_By_Node_Id());

    // Retrieve all ways referred by the relations.
    std::set< std::pair< Uint31_Index, Uint31_Index > > way_ranges;
    get_ranges(rman, way_ranges);

    std::map< Uint31_Index, std::vector< Way_Skeleton > > way_members_
        = relation_way_members(&query, rman, into.relations, &way_ranges);
    std::vector< std::pair< Uint31_Index, const Way_Skeleton* > > way_members_by_id
        = order_by_id(way_members_, Order_By_Way_Id());

    // Retrieve all nodes referred by the ways.
    filter_relations_expensive(*around, node_members_by_id, way_members_by_id,
        Way_Geometry_Store(way_members_, query, rman), into.relations);
  }

  if (!into.attic_nodes.empty())
    filter_nodes_expensive(*around, into.attic_nodes);

  if (!into.attic_ways.empty())
    filter_ways_expensive(*around, Way_Geometry_Store(into.attic_ways, query, rman), into.attic_ways);

  if (!into.attic_relations.empty())
  {
    //Process relations
    std::set< std::pair< Uint32_Index, Uint32_Index > > node_ranges;
    get_ranges(rman, node_ranges);

    std::map< Uint32_Index, std::vector< Attic< Node_Skeleton > > > node_members
        = relation_node_members(&query, rman, into.attic_relations, &node_ranges);
    std::vector< std::pair< Uint32_Index, const Node_Skeleton* > > node_members_by_id
        = order_attic_by_id(node_members, Order_By_Node_Id());

    // Retrieve all ways referred by the relations.
    std::set< std::pair< Uint31_Index, Uint31_Index > > way_ranges;
    get_ranges(rman, way_ranges);

    std::map< Uint31_Index, std::vector< Attic< Way_Skeleton > > > way_members_
        = relation_way_members(&query, rman, into.attic_relations, &way_ranges);
    std::vector< std::pair< Uint31_Index, const Way_Skeleton* > > way_members_by_id
        = order_attic_by_id(way_members_, Order_By_Way_Id());

    filter_relations_expensive(*around, node_members_by_id, way_members_by_id,
        Way_Geometry_Store(way_members_, query, rman), into.attic_relations);
  }

  //TODO: areas
}

//-----------------------------------------------------------------------------

Around_Statement::Statement_Maker Around_Statement::statement_maker;
Around_Statement::Criterion_Maker Around_Statement::criterion_maker;


Statement* Around_Statement::Criterion_Maker::create_criterion(const Token_Node_Ptr& input_tree,
    const std::string& type, const std::string& into,
    Statement::Factory& stmt_factory, Parsed_Query& global_settings, Error_Output* error_output)
{
  Token_Node_Ptr tree_it = input_tree;
  uint line_nr = tree_it->line_col.first;
  std::vector< std::pair< std::string, std::string > > coords;

  while (tree_it->token == "," && tree_it->rhs && tree_it->lhs)
  {
    std::string lon = tree_it.rhs()->token;
    tree_it = tree_it.lhs();

    if (tree_it->token != "," || !tree_it->rhs || !tree_it->lhs)
    {
      if (error_output)
        error_output->add_parse_error("around requires an odd number of arguments", line_nr);
      return 0;
    }

    coords.push_back(std::make_pair(tree_it.rhs()->token, lon));
    tree_it = tree_it.lhs();
  }

  if (type == "area") {
    error_output->add_parse_error("Around filter not supported for areas.", line_nr);
  }

  if (tree_it->token == ":" && tree_it->rhs)
  {
    std::string radius = decode_json(tree_it.rhs()->token, error_output);

    tree_it = tree_it.lhs();
    std::string from = "_";
    if (tree_it->token == "." && tree_it->rhs)
      from = tree_it.rhs()->token;

    std::map< std::string, std::string > attributes;
    attributes["from"] = from;
    attributes["into"] = into;
    attributes["radius"] = radius;
    if (coords.size() == 1)
    {
      attributes["lat"] = coords.front().first;
      attributes["lon"] = coords.front().second;
    }
    else if (!coords.empty())
    {
      std::reverse(coords.begin(),coords.end());
      for (std::vector< std::pair< std::string, std::string > >::const_iterator it = coords.begin();
          it != coords.end(); ++it)
        attributes["polyline"] += it->first + "," + it->second + ",";
      attributes["polyline"].resize(attributes["polyline"].size()-1);
    }
    return new Around_Statement(line_nr, attributes, global_settings);
  }
  else if (error_output)
    error_output->add_parse_error("around requires the radius as first argument", line_nr);

  return 0;
}


Around_Statement::Around_Statement
    (int line_number_, const std::map< std::string, std::string >& input_attributes, Parsed_Query& global_settings)
    : Output_Statement(line_number_)
{
  std::map< std::string, std::string > attributes;

  attributes["from"] = "_";
  attributes["into"] = "_";
  attributes["radius"] = "";
  attributes["lat"] = "";
  attributes["lon"] = "";
  attributes["polyline"] = "";

  eval_attributes_array(get_name(), attributes, input_attributes);

  input = attributes["from"];
  set_output(attributes["into"]);

  radius = atof(attributes["radius"].c_str());
  if ((radius < 0.0) || (attributes["radius"] == ""))
  {
    std::ostringstream temp;
    temp<<"For the attribute \"radius\" of the element \"around\""
        <<" the only allowed values are nonnegative floats.";
    add_static_error(temp.str());
  }

  double lat = 100.;
  double lon = 0;
  if (attributes["lat"] != "")
  {
    lat = atof(attributes["lat"].c_str());
    if ((lat < -90.0) || (lat > 90.0))
      add_static_error("For the attribute \"lat\" of the element \"around\""
          " the only allowed values are floats between -90.0 and 90.0 or an empty value.");
  }

  if (attributes["lon"] != "")
  {
    lon = atof(attributes["lon"].c_str());
    if ((lon < -180.0) || (lon > 180.0))
      add_static_error("For the attribute \"lon\" of the element \"around\""
          " the only allowed values are floats between -1800.0 and 180.0 or an empty value.");
  }

  if (attributes["polyline"] != "")
  {
    if (attributes["lat"] != "" || attributes["lon"] != "")
      add_static_error("In \"around\", the attribute \"polyline\" cannot be used if \"lat\" or \"lon\" are used.");

    std::string& polystring = attributes["polyline"];
    std::string::size_type from = 0;
    std::string::size_type to = polystring.find(",");
    while (to != std::string::npos)
    {
      double lat = atof(polystring.substr(from, to).c_str());
      from = to+1;
      to = polystring.find(",", from);
      if (to != std::string::npos)
      {
        points.push_back(Point_Double(lat, atof(polystring.substr(from, to).c_str())));
        from = to+1;
        to = polystring.find(",", from);
      }
      else
        points.push_back(Point_Double(lat, atof(polystring.substr(from).c_str())));
    }

    if ((points.back().lat < -90.0) || (points.back().lat > 90.0))
      add_static_error("For a latitude entry in the attribute \"polyline\" of the element \"around\""
          " the only allowed values are floats between -90.0 and 90.0 or an empty value.");
    if ((points.back().lon < -180.0) || (points.back().lon > 180.0))
      add_static_error("For a latitude entry in the attribute \"polyline\" of the element \"around\""
          " the only allowed values are floats between -1800.0 and 180.0 or an empty value.");
  }
  else if (lat < 100.)
    points.push_back(Point_Double(lat, lon));
}

Around_Statement::~Around_Statement()
{
  for (std::vector< Query_Constraint* >::const_iterator it = constraints.begin();
      it != constraints.end(); ++it)
    delete *it;
}

namespace {

std::tuple< double, double, double > cartesian(double lat, double lon)
{
  return std::make_tuple( sin(lat/90.0*acos(0)),
                          cos(lat/90.0*acos(0))*sin(lon/90.0*acos(0)),
                          cos(lat/90.0*acos(0))*cos(lon/90.0*acos(0)));
}


inline void rescale(double a, std::tuple< double, double, double >& v)
{
  std::get<0>(v) *= a;
  std::get<1>(v) *= a;
  std::get<2>(v) *= a;
}


inline std::tuple< double, double, double > sum(const std::tuple< double, double, double >& v,
                                         const std::tuple< double, double, double >& w)
{
  return std::make_tuple( std::get<0>(v) + std::get<0>(w),
                          std::get<1>(v) + std::get<1>(w),
                          std::get<2>(v) + std::get<2>(w));
}


inline double scalar_prod(const std::tuple< double, double, double >& v,
                   const std::tuple< double, double, double >& w)
{
  return (std::get<0>(v) * std::get<0>(w) +
          std::get<1>(v) * std::get<1>(w) +
          std::get<2>(v) * std::get<2>(w));
}


std::tuple< double, double, double >cross_prod(const std::tuple< double, double, double >& v,
                                               const std::tuple< double, double, double >& w)
{
  return std::make_tuple(std::get<1>(v) * std::get<2>(w) - std::get<2>(v) * std::get<1>(w),
                         std::get<2>(v) * std::get<0>(w) - std::get<0>(v) * std::get<2>(w),
                         std::get<0>(v) * std::get<1>(w) - std::get<1>(v) * std::get<0>(w));
}

}

Prepared_Segment::Prepared_Segment
  (double first_lat_, double first_lon_, double second_lat_, double second_lon_)
  : first_lat(first_lat_), first_lon(first_lon_), second_lat(second_lat_), second_lon(second_lon_)
{
  first_cartesian = cartesian(first_lat, first_lon);
  second_cartesian = cartesian(second_lat, second_lon);
  norm = cross_prod(first_cartesian, second_cartesian);
}


Prepared_Point::Prepared_Point
  (double lat_, double lon_)
  : lat(lat_), lon(lon_)
{
  cartesian = ::cartesian(lat, lon);
}

namespace {

double great_circle_line_dist(const Prepared_Segment& segment, const std::tuple< double, double, double >& cartesian)
{
  double scalar_prod_ = std::abs(scalar_prod(cartesian, segment.norm))
      /sqrt(scalar_prod(segment.norm, segment.norm));

  if (scalar_prod_ > 1)
    scalar_prod_ = 1;

  return asin(scalar_prod_)*(10*1000*1000/acos(0));
}


double great_circle_line_dist(double llat1, double llon1, double llat2, double llon2,
                              double plat, double plon)
{
  std::tuple< double, double, double > norm = cross_prod(cartesian(llat1, llon1), cartesian(llat2, llon2));

  double scalar_prod_ = std::abs(scalar_prod(cartesian(plat, plon), norm))
      /sqrt(scalar_prod(norm, norm));

  if (scalar_prod_ > 1)
    scalar_prod_ = 1;

  return asin(scalar_prod_)*(10*1000*1000/acos(0));
}


bool intersect(const Prepared_Segment& segment_a,
               const Prepared_Segment& segment_b)
{
  std::tuple< double, double, double >intersection_pt = cross_prod(segment_a.norm, segment_b.norm);
  rescale(1.0/sqrt(scalar_prod(intersection_pt, intersection_pt)), intersection_pt);

  std::tuple< double, double, double > asum = sum(segment_a.first_cartesian, segment_a.second_cartesian);
  std::tuple< double, double, double > bsum = sum(segment_b.first_cartesian, segment_b.second_cartesian);

  return (std::abs(scalar_prod(asum, intersection_pt)) >= scalar_prod(asum, segment_a.first_cartesian)
      && std::abs(scalar_prod(bsum, intersection_pt)) >= scalar_prod(bsum, segment_b.first_cartesian));
}


bool intersect(double alat1, double alon1, double alat2, double alon2,
	       double blat1, double blon1, double blat2, double blon2)
{
  std::tuple< double, double, double > a1 = cartesian(alat1, alon1);
  std::tuple< double, double, double > a2 = cartesian(alat2, alon2);
  std::tuple< double, double, double > norm_a = cross_prod(a1, a2);
  std::tuple< double, double, double > b1 = cartesian(blat1, blon1);
  std::tuple< double, double, double > b2 = cartesian(blat2, blon2);
  std::tuple< double, double, double > norm_b = cross_prod(b1, b2);

  std::tuple< double, double, double > intersection_pt = cross_prod(norm_a, norm_b);
  rescale(1.0/sqrt(scalar_prod(intersection_pt, intersection_pt)), intersection_pt);

  std::tuple< double, double, double > asum = sum(a1, a2);
  std::tuple< double, double, double > bsum = sum(b1, b2);

  return (std::abs(scalar_prod(asum, intersection_pt)) >= scalar_prod(asum, a1)
      && std::abs(scalar_prod(bsum, intersection_pt)) >= scalar_prod(bsum, b1));
}

}

std::set< std::pair< Uint32_Index, Uint32_Index > > Around_Statement::calc_ranges
    (const Set& input, Resource_Manager& rman) const
{
  if (points.size() == 1)
    return expand(ranges(points[0].lat, points[0].lon), radius);

  else if (points.size() > 1)
  {
    std::vector< uint32 > nd_idxs;
    std::map< Uint31_Index, std::vector< Way_Skeleton > > ways;
    std::pair< Uint31_Index, std::vector< Way_Skeleton > > way;

    for (std::vector< Point_Double >::const_iterator it = points.begin(); it != points.end(); ++it)
        nd_idxs.push_back(::ll_upper_(it->lat, it->lon));

    Uint31_Index idx = Way::calc_index(nd_idxs);
    way = std::make_pair(idx, std::vector< Way_Skeleton >());
    ways.insert(way);
    return expand(children(ranges(ways)), radius);
  }
  else
    return expand(set_union_
        (set_union_(ranges(input.nodes), ranges(input.attic_nodes)),
	    children(set_union_(
	        set_union_(ranges(input.ways), ranges(input.attic_ways)),
	        set_union_(ranges(input.relations), ranges(input.attic_relations))))),
        radius);
}


void add_coord(double lat, double lon, double radius,
               std::map< Uint32_Index, std::vector< Point_Double > >& radius_lat_lons,
               std::vector< std::pair< Prepared_BBox, Prepared_Point> >& simple_lat_lons)
{
  double south = lat - radius*(360.0/(40000.0*1000.0));
  double north = lat + radius*(360.0/(40000.0*1000.0));
  double scale_lat = lat > 0.0 ? north : south;
  if (std::abs(scale_lat) >= 89.9)
    scale_lat = 89.9;
  double west = lon - radius*(360.0/(40000.0*1000.0))/cos(scale_lat/90.0*acos(0));
  double east = lon + radius*(360.0/(40000.0*1000.0))/cos(scale_lat/90.0*acos(0));
  
  Prepared_BBox bbox_point = ::lat_lon_bbox(south, west, north, east);

  simple_lat_lons.push_back(std::make_pair(bbox_point, Prepared_Point(lat, lon)));
  
  std::vector< std::pair< uint32, uint32 > > uint_ranges

      (calc_ranges(south, north, west, east));
  for (std::vector< std::pair< uint32, uint32 > >::const_iterator
      it(uint_ranges.begin()); it != uint_ranges.end(); ++it)
  {
    for (uint32 idx = Uint32_Index(it->first).val();
        idx < Uint32_Index(it->second).val(); ++idx)
      radius_lat_lons[idx].push_back(Point_Double(lat, lon));
  }
}


void add_node(Uint32_Index idx, const Node_Skeleton& node, double radius,
              std::map< Uint32_Index, std::vector< Point_Double > >& radius_lat_lons,
              std::vector< std::pair< Prepared_BBox, Prepared_Point> >& simple_lat_lons,
              std::vector< Prepared_BBox >& node_bboxes)

{
  double lat = ::lat(idx.val(), node.ll_lower);
  double lon = ::lon(idx.val(), node.ll_lower);
  add_coord(lat, lon, radius, radius_lat_lons, simple_lat_lons);
  node_bboxes.push_back(::calc_distance_bbox(lat, lon, radius));
}


void add_way(const std::vector< Quad_Coord >& way_geometry, double radius,
             std::map< Uint32_Index, std::vector< Point_Double > >& radius_lat_lons,
             std::vector< std::pair< Prepared_BBox, Prepared_Point> >& simple_lat_lons,
             std::vector< std::pair< Prepared_BBox, Prepared_Segment> >& simple_segments,
             std::vector< Prepared_BBox >& way_bboxes)
{
  // add nodes
  Prepared_BBox way_bbox;
  
  for (std::vector< Quad_Coord >::const_iterator nit = way_geometry.begin(); nit != way_geometry.end(); ++nit)
  {
    double lat = ::lat(nit->ll_upper, nit->ll_lower);
    double lon = ::lon(nit->ll_upper, nit->ll_lower);
    add_coord(lat, lon, radius, radius_lat_lons, simple_lat_lons);
    Prepared_BBox node_bbox = ::calc_distance_bbox(lat, lon, radius);
    way_bbox.merge(node_bbox);
  }
  way_bboxes.push_back(way_bbox);

  // add segments

  std::vector< Quad_Coord >::const_iterator nit = way_geometry.begin();
  if (nit == way_geometry.end())
    return;

  double first_lat(::lat(nit->ll_upper, nit->ll_lower));
  double first_lon(::lon(nit->ll_upper, nit->ll_lower));

  for (++nit; nit != way_geometry.end(); ++nit)
  {
    double second_lat(::lat(nit->ll_upper, nit->ll_lower));
    double second_lon(::lon(nit->ll_upper, nit->ll_lower));
    
    simple_segments.push_back(std::make_pair(way_bbox, Prepared_Segment(first_lat, first_lon, second_lat, second_lon)));

    first_lat = second_lat;
    first_lon = second_lon;
  }
}

void add_way(const std::vector< Point_Double >& points, double radius,
             std::map< Uint32_Index, std::vector< Point_Double > >& radius_lat_lons,
             std::vector< std::pair< Prepared_BBox, Prepared_Point> >& simple_lat_lons,
             std::vector< std::pair< Prepared_BBox, Prepared_Segment> >& simple_segments,
             std::vector< Prepared_BBox >& way_bboxes)
{
  // add nodes
  Prepared_BBox way_bbox;

  for (std::vector< Point_Double >::const_iterator nit = points.begin(); nit != points.end(); ++nit)
  {
    double lat = nit->lat;
    double lon = nit->lon;
    add_coord(lat, lon, radius, radius_lat_lons, simple_lat_lons);
    Prepared_BBox node_bbox = ::calc_distance_bbox(lat, lon, radius);
    way_bbox.merge(node_bbox);
  }
  way_bboxes.push_back(way_bbox);

  // add segments

  std::vector< Point_Double >::const_iterator nit = points.begin();
  if (nit == points.end())
    return;

  double first_lat(nit->lat);
  double first_lon(nit->lon);

  for (++nit; nit != points.end(); ++nit)
  {
    double second_lat(nit->lat);
    double second_lon(nit->lon);

    simple_segments.push_back(std::make_pair(way_bbox, Prepared_Segment(first_lat, first_lon, second_lat, second_lon)));

    first_lat = second_lat;
    first_lon = second_lon;
  }
}


struct Relation_Member_Collection
{
  Relation_Member_Collection(const std::map< Uint31_Index, std::vector< Relation_Skeleton > >& relations,
			     const Statement& query, Resource_Manager& rman,
			     std::set< std::pair< Uint32_Index, Uint32_Index > >* node_ranges,
			     std::set< std::pair< Uint31_Index, Uint31_Index > >* way_ranges)
      : query_(query),
	way_members(relation_way_members(&query, rman, relations, way_ranges)),
        node_members(relation_node_members(&query, rman, relations, node_ranges))
  {
    // Retrieve all nodes referred by the ways.

    // Order node ids by id.
    for (std::map< Uint32_Index, std::vector< Node_Skeleton > >::iterator it = node_members.begin();
        it != node_members.end(); ++it)
    {
      for (std::vector< Node_Skeleton >::const_iterator iit = it->second.begin();
          iit != it->second.end(); ++iit)
        node_members_by_id.push_back(std::make_pair(it->first, &*iit));
    }
    Order_By_Node_Id order_by_node_id;
    sort(node_members_by_id.begin(), node_members_by_id.end(), order_by_node_id);

    // Retrieve all ways referred by the relations.

    // Order way ids by id.
    for (std::map< Uint31_Index, std::vector< Way_Skeleton > >::iterator it = way_members.begin();
        it != way_members.end(); ++it)
    {
      for (std::vector< Way_Skeleton >::const_iterator iit = it->second.begin();
          iit != it->second.end(); ++iit)
        way_members_by_id.push_back(std::make_pair(it->first, &*iit));
    }
    Order_By_Way_Id order_by_way_id;
    sort(way_members_by_id.begin(), way_members_by_id.end(), order_by_way_id);
  }

  const std::pair< Uint32_Index, const Node_Skeleton* >* get_node_by_id(Node::Id_Type id) const
  {
    const std::pair< Uint32_Index, const Node_Skeleton* >* node =
        binary_search_for_pair_id(node_members_by_id, id);

    return node;
  }

  const std::pair< Uint31_Index, const Way_Skeleton* >* get_way_by_id(Uint32_Index id) const
  {
    const std::pair< Uint31_Index, const Way_Skeleton* >* way =
        binary_search_for_pair_id(way_members_by_id, id);

    return way;
  }

  const Statement& query_;
  std::map< Uint31_Index, std::vector< Way_Skeleton > > way_members;
  std::map< Uint32_Index, std::vector< Node_Skeleton > > node_members;
  std::vector< std::pair< Uint31_Index, const Way_Skeleton* > > way_members_by_id;
  std::vector< std::pair< Uint32_Index, const Node_Skeleton* > > node_members_by_id;
};


template< typename Node_Skeleton >
void Around_Statement::add_nodes(const std::map< Uint32_Index, std::vector< Node_Skeleton > >& nodes)
{
  for (typename std::map< Uint32_Index, std::vector< Node_Skeleton > >::const_iterator iit(nodes.begin());
      iit != nodes.end(); ++iit)
  {
    for (typename std::vector< Node_Skeleton >::const_iterator nit(iit->second.begin());
        nit != iit->second.end(); ++nit)
      add_node(iit->first, *nit, radius, radius_lat_lons, simple_lat_lons, node_bboxes);
  }
}


template< typename Way_Skeleton >
void Around_Statement::add_ways(const std::map< Uint31_Index, std::vector< Way_Skeleton > >& ways,
				const Way_Geometry_Store& way_geometries)
{
  for (typename std::map< Uint31_Index, std::vector< Way_Skeleton > >::const_iterator it = ways.begin();
      it != ways.end(); ++it)
  {
    for (typename std::vector< Way_Skeleton >::const_iterator iit = it->second.begin();
        iit != it->second.end(); ++iit)
      add_way(way_geometries.get_geometry(*iit), radius,
          radius_lat_lons, simple_lat_lons, simple_segments, way_bboxes);
  }
}


void Around_Statement::calc_lat_lons(const Set& input, Statement& query, Resource_Manager& rman)
{
  radius_lat_lons.clear();
  simple_lat_lons.clear();

  simple_segments.clear();
  
  node_bboxes.clear();
  way_bboxes.clear();


  if (points.size() == 1)
  {
    add_coord(points[0].lat, points[0].lon, radius, radius_lat_lons, simple_lat_lons);
    node_bboxes.push_back(::calc_distance_bbox(points[0].lat, points[0].lon, radius));
    return;
  }
  else if (points.size() > 1)
  {
    add_way(points, radius, radius_lat_lons, simple_lat_lons, simple_segments, way_bboxes);
    return;
  }

  add_nodes(input.nodes);
  add_ways(input.ways, Way_Geometry_Store(input.ways, query, rman));

  // Retrieve all node and way members referred by the relations.
  add_nodes(relation_node_members(&query, rman, input.relations));

  // Retrieve all ways referred by the relations.
  std::map< Uint31_Index, std::vector< Way_Skeleton > > way_members
      = relation_way_members(&query, rman, input.relations);
  add_ways(way_members, Way_Geometry_Store(way_members, query, rman));

  if (rman.get_desired_timestamp() != NOW)
  {
    add_nodes(input.attic_nodes);
    add_ways(input.attic_ways, Way_Geometry_Store(input.attic_ways, query, rman));

    // Retrieve all node and way members referred by the relations.
    add_nodes(relation_node_members(&query, rman, input.attic_relations));

    // Retrieve all ways referred by the relations.
    std::map< Uint31_Index, std::vector< Attic< Way_Skeleton > > > way_members
        = relation_way_members(&query, rman, input.attic_relations);
    add_ways(way_members, Way_Geometry_Store(way_members, query, rman));
  }
}

bool Around_Statement::matches_bboxes(double lat, double lon) const
{
  Prepared_BBox bbox = ::lat_lon_bbox(lat, lon);
  return bbox.intersects(node_bboxes) ||
         bbox.intersects(way_bboxes);

}

bool Around_Statement::matches_bboxes(const Prepared_BBox & bbox) const
{
  return bbox.intersects(node_bboxes) ||
         bbox.intersects(way_bboxes);
}


bool Around_Statement::is_inside(double lat, double lon) const
{
  std::map< Uint32_Index, std::vector< Point_Double > >::const_iterator mit
      = radius_lat_lons.find(::ll_upper_(lat, lon));
  if (mit != radius_lat_lons.end())
  {
    for (std::vector< Point_Double >::const_iterator cit = mit->second.begin();
        cit != mit->second.end(); ++cit)
    {
      if ((radius > 0 && great_circle_dist(cit->lat, cit->lon, lat, lon) <= radius)
          || (std::abs(cit->lat - lat) < 1e-7 && std::abs(cit->lon - lon) < 1e-7))
        return true;
    }
  }
  
  std::tuple< double, double, double > coord_cartesian = cartesian(lat, lon);
  Prepared_BBox bbox_lat_lon = ::lat_lon_bbox(lat, lon);

  for (std::vector< std::pair< Prepared_BBox, Prepared_Segment> >::const_iterator

      it = simple_segments.begin(); it != simple_segments.end(); ++it)
  {
    if (bbox_lat_lon.intersects(it->first) &&
        great_circle_line_dist(it->second, coord_cartesian) <= radius)
    {
      double gcdist = great_circle_dist
          (it->second.first_lat, it->second.first_lon, it->second.second_lat, it->second.second_lon);
      double limit = sqrt(gcdist*gcdist + radius*radius);
      if (great_circle_dist(lat, lon, it->second.first_lat, it->second.first_lon) <= limit &&
          great_circle_dist(lat, lon, it->second.second_lat, it->second.second_lon) <= limit)
	return true;
    }
  }

  return false;
}

bool Around_Statement::is_inside
    (double first_lat, double first_lon, double second_lat, double second_lon) const
{
  Prepared_Segment segment(first_lat, first_lon, second_lat, second_lon);
  Prepared_BBox bbox_segment = ::lat_lon_bbox(first_lat, first_lon, second_lat, second_lon);
  
  for (std::vector< std::pair< Prepared_BBox, Prepared_Point> >::const_iterator cit = simple_lat_lons.begin();

      cit != simple_lat_lons.end(); ++cit)
  {
    if (bbox_segment.intersects(cit->first) &&
        great_circle_line_dist(segment, cit->second.cartesian) <= radius)
    {
      double gcdist = great_circle_dist(first_lat, first_lon, second_lat, second_lon);
      double limit = sqrt(gcdist*gcdist + radius*radius);
      if (great_circle_dist(cit->second.lat, cit->second.lon, first_lat, first_lon) <= limit &&
	  great_circle_dist(cit->second.lat, cit->second.lon, second_lat, second_lon) <= limit)
        return true;
    }
  }

  for (std::vector< std::pair< Prepared_BBox, Prepared_Segment> >::const_iterator
      cit = simple_segments.begin(); cit != simple_segments.end(); ++cit)
  {
    if (bbox_segment.intersects(cit->first) &&
        intersect(cit->second, segment))
      return true;
  }

  return false;
}


bool Around_Statement::is_inside(const std::vector< Quad_Coord >& way_geometry) const
{
  std::vector< Quad_Coord >::const_iterator nit = way_geometry.begin();
  if (nit == way_geometry.end())
    return false;

  // Pre-check if a node is inside
  for (std::vector< Quad_Coord >::const_iterator it = way_geometry.begin(); it != way_geometry.end(); ++it)
  {
    double second_lat(::lat(it->ll_upper, it->ll_lower));
    double second_lon(::lon(it->ll_upper, it->ll_lower));

    if (is_inside(second_lat, second_lon))
      return true;
  }

  double first_lat(::lat(nit->ll_upper, nit->ll_lower));
  double first_lon(::lon(nit->ll_upper, nit->ll_lower));

  for (++nit; nit != way_geometry.end(); ++nit)
  {
    double second_lat(::lat(nit->ll_upper, nit->ll_lower));
    double second_lon(::lon(nit->ll_upper, nit->ll_lower));

    if (is_inside(first_lat, first_lon, second_lat, second_lon))
      return true;

    first_lat = second_lat;
    first_lon = second_lon;
  }
  return false;
}


void Around_Statement::execute(Resource_Manager& rman)
{
  Set into;

  Around_Constraint constraint(*this);
  std::set< std::pair< Uint32_Index, Uint32_Index > > ranges;
  constraint.get_ranges(rman, ranges);
  get_elements_by_id_from_db< Uint32_Index, Node_Skeleton >
      (into.nodes, into.attic_nodes,
       std::vector< Node::Id_Type >(), false, ranges, 0, *this, rman,
       *osm_base_settings().NODES, *attic_settings().NODES);
  constraint.filter(*this, rman, into);
  filter_attic_elements(rman, rman.get_desired_timestamp(), into.nodes, into.attic_nodes);

  transfer_output(rman, into);
  rman.health_check(*this);
}


Query_Constraint* Around_Statement::get_query_constraint()
{
  constraints.push_back(new Around_Constraint(*this));
  return constraints.back();
}
