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

#ifndef DE__OSM3S___OVERPASS_API__DATA__WAY_GEOMETRY_STORE_H
#define DE__OSM3S___OVERPASS_API__DATA__WAY_GEOMETRY_STORE_H

#include "../core/datatypes.h"
#include "../statements/statement.h"
// #include "abstract_processing.h"
// #include "filenames.h"

#include <map>
#include <memory>
#include <vector>


class Way_Geometry_Store
{
public:
  Way_Geometry_Store(const std::map< Uint31_Index, std::vector< Way_Skeleton > >& ways,
                     const Statement& query, Resource_Manager& rman,
                     bool lazy_loading = false);
  Way_Geometry_Store(const std::map< Uint31_Index, std::vector< Attic< Way_Skeleton > > >& ways,
                     const Statement& query, Resource_Manager& rman,
                     bool lazy_loading = false);

  // return the empty std::vector if the way is not found
  std::vector< Quad_Coord > get_geometry(const Way_Skeleton& way) const;

  void prefetch(Uint31_Index);
  void prefetch_attic(Uint31_Index);

private:
  void way_members_to_nodes(std::map< Uint32_Index, std::vector< Node_Skeleton > >& way_members_);

  const std::map< Uint31_Index, std::vector< Way_Skeleton > > * ways;
  const std::map< Uint31_Index, std::vector< Attic< Way_Skeleton > > > * attic_ways;
  const Statement * query;
  Resource_Manager * rman;

  std::vector< Node_Base > nodes;
  std::vector< Uint31_Index > ranges;
  std::unique_ptr<Uint31_Index> current_index;
};


class Way_Bbox_Geometry_Store : public Way_Geometry_Store
{
public:
  Way_Bbox_Geometry_Store(const std::map< Uint31_Index, std::vector< Way_Skeleton > >& ways,
                     const Statement& query, Resource_Manager& rman,
                     double south_, double north_, double west_, double east_,
                     bool lazy_loading = false);
  Way_Bbox_Geometry_Store(const std::map< Uint31_Index, std::vector< Attic< Way_Skeleton > > >& ways,
                     const Statement& query, Resource_Manager& rman,
                     double south_, double north_, double west_, double east_,
                     bool lazy_loading = false);

  // return the empty std::vector if the way is not found
  std::vector< Quad_Coord > get_geometry(const Way_Skeleton& way) const;

private:
  Bbox_Double bbox_d;
  uint32 south;
  uint32 north;
  int32 west;
  int32 east;

  bool matches_bbox(uint32 ll_upper, uint32 ll_lower) const;
};


#endif
