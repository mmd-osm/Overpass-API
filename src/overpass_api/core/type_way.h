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

#ifndef DE__OSM3S___OVERPASS_API__CORE__TYPE_WAY_H
#define DE__OSM3S___OVERPASS_API__CORE__TYPE_WAY_H

#include "basic_types.h"
#include "index_computations.h"
#include "type_node.h"

#include <cstring>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>


struct Way
{
  typedef Uint32_Index Id_Type;

  Id_Type id;
  uint32 index;
  std::vector< Node::Id_Type > nds;
//   std::vector< Uint31_Index > segment_idxs;
  std::vector< Quad_Coord > geometry;
  std::vector< std::pair< std::string, std::string > > tags;

  Way() : id(0u), index(0) {}

  Way(uint32 id_)
  : id(id_), index(0)
  {}

  Way(uint32 id_, uint32 index_, const std::vector< Node::Id_Type >& nds_)
  : id(id_), index(index_), nds(nds_) {}

  static uint32 calc_index(const std::vector< uint32 >& nd_idxs)
  {
    return ::calc_index(nd_idxs);
  }

  static bool indicates_geometry(Uint31_Index index)
  {
    return ((index.val() & 0x80000000) != 0 && ((index.val() & 0x1) == 0));
  }
};


struct Way_Comparator_By_Id {
  bool operator() (const Way* a, const Way* b)
  {
    return (a->id < b->id);
  }
};


struct Way_Equal_Id {
  bool operator() (const Way* a, const Way* b)
  {
    return (a->id == b->id);
  }
};


struct Way_Delta;

template <class T, class Object>
struct Way_Skeleton_Handle_Methods;

class Way_Skeleton_Data final : public SharedData
{
public:
  Way_Skeleton_Data() { }

  Way_Skeleton_Data(const Way_Skeleton_Data &other)
     : SharedData(other),
       nds(other.nds),
       geometry(other.geometry) {}

  ~Way_Skeleton_Data() {}

  std::vector< Node::Id_Type > nds;
  std::vector< Quad_Coord > geometry;
};

struct Way_Skeleton
{
  typedef Way::Id_Type Id_Type;
  typedef Way_Delta Delta;

  Id_Type id;

  Way_Skeleton() : id(0u) { d = new Way_Skeleton_Data; }

  Way_Skeleton(Way::Id_Type id_) : id(id_) { d = new Way_Skeleton_Data; }

  Way_Skeleton(void* data) : id(*(Id_Type*)data)
  {
    d = new Way_Skeleton_Data;

    d->nds.reserve(*((uint16*)data + 2));
    for (int i(0); i < *((uint16*)data + 2); ++i)
      d->nds.push_back(*(uint64*)((uint16*)data + 4 + 4*i));
    uint16* start_ptr = (uint16*)data + 4 + 4*d->nds.size();
    d->geometry.reserve(*((uint16*)data + 3));
    for (int i(0); i < *((uint16*)data + 3); ++i)
      d->geometry.push_back(Quad_Coord(*(uint32*)(start_ptr + 4*i), *(uint32*)(start_ptr + 4*i + 2)));

  }

  Way_Skeleton(const Way& way)
      : id(way.id) {

    d = new Way_Skeleton_Data;
    d->nds = way.nds;
    d->geometry = way.geometry;
  }

  Way_Skeleton(Id_Type id_,  std::vector< Node::Id_Type >&& nds_)
      : id(id_) {

    d = new Way_Skeleton_Data;
    d->nds = std::move(nds_);
  }


  Way_Skeleton(Id_Type id_, const std::vector< Node::Id_Type >& nds_, const std::vector< Quad_Coord >& geometry_)
      : id(id_) {

    d = new Way_Skeleton_Data;
    d->nds = nds_;
    d->geometry = geometry_;
  }

  const std::vector< Node::Id_Type > & nds() const { return d->nds; }
  std::vector< Node::Id_Type > & nds() { return d->nds; }

  const std::vector< Quad_Coord > & geometry() const { return d->geometry; }
  std::vector< Quad_Coord > & geometry() { return d->geometry; }

  uint32 size_of() const
  {
    return 8 + 8*d->nds.size() + 8*d->geometry.size();
  }

  static uint32 size_of(const void* data)
  {
    return (8 + 8 * *((uint16*)data + 2) + 8 * *((uint16*)data + 3));
  }

  void to_data(void* data) const
  {
    *(Id_Type*)data = id.val();
    *((uint16*)data + 2) = d->nds.size();
    *((uint16*)data + 3) = d->geometry.size();
    for (uint i(0); i < d->nds.size(); ++i)
      *(uint64*)((uint16*)data + 4 + 4*i) = d->nds[i].val();
    uint16* start_ptr = (uint16*)data + 4 + 4*d->nds.size();
    for (uint i(0); i < d->geometry.size(); ++i)
    {
      *(uint32*)(start_ptr + 4*i) = d->geometry[i].ll_upper;
      *(uint32*)(start_ptr + 4*i + 2) = d->geometry[i].ll_lower;
    }
  }

  bool operator<(const Way_Skeleton& a) const
  {
    return this->id < a.id;
  }

  bool operator==(const Way_Skeleton& a) const
  {
    return this->id == a.id;
  }

  template <class T, class Object>
  using Handle_Methods = Way_Skeleton_Handle_Methods<T, Object>;

private:
  SharedDataPointer<Way_Skeleton_Data> d;
};

template <typename Id_Type >
struct Way_Skeleton_Id_Functor {
  Way_Skeleton_Id_Functor() {};

  using reference_type = Way_Skeleton;

  Id_Type operator()(const void* data) const
   {
     return *(Id_Type*)data;
   }
};


template <class T, class Object>
struct Way_Skeleton_Handle_Methods
{
  typename Object::Id_Type inline id() const {
     return (static_cast<const T*>(this)->apply_func(Way_Skeleton_Id_Functor<typename Object::Id_Type>()));
  }
};


struct Way_Delta
{
  typedef Way_Skeleton::Id_Type Id_Type;

  Id_Type id;
  bool full;
  std::vector< uint > nds_removed;
  std::vector< std::pair< uint, Node::Id_Type > > nds_added;
  std::vector< uint > geometry_removed;
  std::vector< std::pair< uint, Quad_Coord > > geometry_added;

  Way_Delta() : id(0u), full(false) {}

  Way_Delta(void* data) : id(*(Id_Type*)data), full(false)
  {
    if (*((uint32*)data + 1) == 0xffffffff)
    {
      full = true;
      nds_removed.clear();
      nds_added.resize(*((uint32*)data + 2));
      geometry_removed.clear();
      geometry_added.resize(*((uint32*)data + 3), std::make_pair(0, Quad_Coord()));

      uint8* ptr = ((uint8*)data) + 16;

      for (uint i(0); i < nds_added.size(); ++i)
      {
        nds_added[i].first = i;
        nds_added[i].second = *(uint64*)((uint32*)ptr);
        ptr += 8;
      }

      for (uint i = 0; i < geometry_added.size(); ++i)
      {
        geometry_added[i].first = i;
        geometry_added[i].second = *((Quad_Coord*)ptr);
        ptr += 8;
      }
    }
    else
    {
      nds_removed.resize(*((uint32*)data + 1));
      nds_added.resize(*((uint32*)data + 2));
      geometry_removed.resize(*((uint32*)data + 3));
      geometry_added.resize(*((uint32*)data + 4), std::make_pair(0, Quad_Coord()));

      uint8* ptr = ((uint8*)data) + 20;

      for (uint i(0); i < nds_removed.size(); ++i)
      {
        nds_removed[i] = *((uint32*)ptr);
        ptr += 4;
      }

      for (uint i(0); i < nds_added.size(); ++i)
      {
        nds_added[i].first = *((uint32*)ptr);
        nds_added[i].second = *(uint64*)((uint32*)(ptr + 4));
        ptr += 12;
      }

      for (uint i = 0; i < geometry_removed.size(); ++i)
      {
        geometry_removed[i] = *((uint32*)ptr);
        ptr += 4;
      }

      for (uint i = 0; i < geometry_added.size(); ++i)
      {
        geometry_added[i].first = *((uint32*)ptr);
        geometry_added[i].second = *((Quad_Coord*)(ptr + 4));
        ptr += 12;
      }
    }
  }

  Way_Delta(const Way_Skeleton& reference, const Way_Skeleton& skel)
    : id(skel.id), full(false)
  {
    if (!(id == skel.id))
      full = true;
    else
    {
      make_delta(skel.nds(), reference.nds(), nds_removed, nds_added);
      make_delta(skel.geometry(), reference.geometry(), geometry_removed, geometry_added);
    }

    if (nds_added.size() >= skel.nds().size()/2)
    {
      nds_removed.clear();
      nds_added.clear();
      geometry_removed.clear();
      geometry_added.clear();
      full = true;
    }

    if (full)
    {
      copy_elems(skel.nds(), nds_added);
      copy_elems(skel.geometry(), geometry_added);
    }
  }

  Way_Skeleton expand(const Way_Skeleton& reference) const
  {
    Way_Skeleton result(id);

    if (full)
    {
      result.nds().reserve(nds_added.size());
      for (uint i = 0; i < nds_added.size(); ++i)
        result.nds().push_back(nds_added[i].second);

      result.geometry().reserve(geometry_added.size());
      for (uint i = 0; i < geometry_added.size(); ++i)
        result.geometry().push_back(geometry_added[i].second);
    }
    else if (reference.id == id)
    {
      expand_diff(reference.nds(), nds_removed, nds_added, result.nds());
      expand_diff(reference.geometry(), geometry_removed, geometry_added, result.geometry());
      if (!result.geometry().empty() && result.nds().size() != result.geometry().size())
      {
	std::ostringstream out;
	out<<"Bad geometry for way "<<id.val();
	throw std::logic_error(out.str());
      }
    }
    else
      result.id = 0u;

    return result;
  }

  Way_Skeleton expand_fast(Way_Skeleton& reference) const
  {
    Way_Skeleton result(id);

    if (full)
    {
      result.nds().reserve(nds_added.size());
      for (uint i = 0; i < nds_added.size(); ++i)
        result.nds().push_back(nds_added[i].second);

      result.geometry().reserve(geometry_added.size());
      for (uint i = 0; i < geometry_added.size(); ++i)
        result.geometry().push_back(geometry_added[i].second);
    }
    else if (reference.id == id)
    {
      expand_diff_fast(reference.nds(), nds_removed, nds_added, result.nds());
      expand_diff_fast(reference.geometry(), geometry_removed, geometry_added, result.geometry());
      if (!result.geometry().empty() && result.nds().size() != result.geometry().size())
      {
        std::ostringstream out;
        out<<"Bad geometry for way "<<id.val();
        throw std::logic_error(out.str());
      }
    }
    else
      result.id = 0u;

    return result;
  }

  uint32 size_of() const
  {
    if (full)
      return 16 + 8*nds_added.size() + 8*geometry_added.size();
    else
      return 20 + 4*nds_removed.size() + 12*nds_added.size()
          + 4*geometry_removed.size() + 12*geometry_added.size();
  }

  static uint32 size_of(const void* data)
  {
    if (*((uint32*)data + 1) == 0xffffffff)
      return 16 + 8 * *((uint32*)data + 2) + 8 * *((uint32*)data + 3);
    else
      return 20 + 4 * *((uint32*)data + 1) + 12 * *((uint32*)data + 2)
          + 4 * *((uint32*)data + 3) + 12 * *((uint32*)data + 4);
  }

  void to_data(void* data) const
  {
    *(Id_Type*)data = id.val();
    if (full)
    {
      *((uint32*)data + 1) = 0xffffffff;
      *((uint32*)data + 2) = nds_added.size();
      *((uint32*)data + 3) = geometry_added.size();

      uint8* ptr = ((uint8*)data) + 16;

      for (uint i = 0; i < nds_added.size(); ++i)
      {
        *(uint64*)((uint32*)ptr) = nds_added[i].second.val();
        ptr += 8;
      }

      for (uint i = 0; i < geometry_added.size(); ++i)
      {
        *((Quad_Coord*)ptr) = geometry_added[i].second;
        ptr += 8;
      }
    }
    else
    {
      *((uint32*)data + 1) = nds_removed.size();
      *((uint32*)data + 2) = nds_added.size();
      *((uint32*)data + 3) = geometry_removed.size();
      *((uint32*)data + 4) = geometry_added.size();

      uint8* ptr = ((uint8*)data) + 20;

      for (uint i = 0; i < nds_removed.size(); ++i)
      {
        *((uint32*)ptr) = nds_removed[i];
        ptr += 4;
      }

      for (uint i = 0; i < nds_added.size(); ++i)
      {
        *((uint32*)ptr) = nds_added[i].first;
        *(uint64*)((uint32*)(ptr + 4)) = nds_added[i].second.val();
        ptr += 12;
      }

      for (uint i = 0; i < geometry_removed.size(); ++i)
      {
        *((uint32*)ptr) = geometry_removed[i];
        ptr += 4;
      }

      for (uint i = 0; i < geometry_added.size(); ++i)
      {
        *((uint32*)ptr) = geometry_added[i].first;
        *((Quad_Coord*)(ptr + 4)) = geometry_added[i].second;
        ptr += 12;
      }
    }
  }

  bool operator<(const Way_Delta& a) const
  {
    return this->id < a.id;
  }

  bool operator==(const Way_Delta& a) const
  {
    return this->id == a.id;
  }
};


inline std::vector< Uint31_Index > calc_segment_idxs(const std::vector< uint32 >& nd_idxs)
{
  std::vector< Uint31_Index > result;
  std::vector< uint32 > segment_nd_idxs(2, 0);
  for (std::vector< uint32 >::size_type i = 1; i < nd_idxs.size(); ++i)
  {
    segment_nd_idxs[0] = nd_idxs[i-1];
    segment_nd_idxs[1] = nd_idxs[i];
    Uint31_Index segment_index = Way::calc_index(segment_nd_idxs);
    if ((segment_index.val() & 0x80000000) != 0)
      result.push_back(segment_index);
  }
  sort(result.begin(), result.end());
  result.erase(unique(result.begin(), result.end()), result.end());

  return result;
}


#endif
