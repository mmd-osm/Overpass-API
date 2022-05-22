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

  Way() noexcept : id(0u), index(0) {}

  Way(uint32 id_) noexcept
  : id(id_), index(0)
  {}

  Way(uint32 id_, uint32 index_, const std::vector< Node::Id_Type >& nds_)
  : id(id_), index(index_), nds(nds_) {}

  static uint32 calc_index(const std::vector< uint32 >& nd_idxs)
  {
    return ::calc_index(nd_idxs);
  }

  static bool indicates_geometry(Uint31_Index index) noexcept
  {
    return ((index.val() & 0x80000000) != 0 && ((index.val() & 0x1) == 0));
  }
};


struct Way_Comparator_By_Id {
  bool operator() (const Way* a, const Way* b) noexcept
  {
    return (a->id < b->id);
  }
};


struct Way_Equal_Id {
  bool operator() (const Way* a, const Way* b) noexcept
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
  Way_Skeleton_Data() = default;

  Way_Skeleton_Data(const Way_Skeleton_Data &other) = default;

  ~Way_Skeleton_Data() = default;

  std::vector< Node::Id_Type > nds;
  std::vector< Quad_Coord > geometry;
};

struct Way_Skeleton
{
  typedef Way::Id_Type Id_Type;
  typedef Way_Delta Delta;

  Id_Type id;

  Way_Skeleton() : id(0u), d(new Way_Skeleton_Data) { }

  Way_Skeleton(Way::Id_Type id_) : id(id_),  d(new Way_Skeleton_Data) { }

  Way_Skeleton(const void* data) : id(unalignedLoad<Id_Type>(data)),  d(new Way_Skeleton_Data)
  {
    d->nds.reserve(*((uint16*)data + 2));

    auto* start_ptr = (uint16*) decompress_ids(d->nds, *((uint16*)data + 2), *((uint16*)data + 4), ((uint8*)data + 10));

    const auto geometry_count = unalignedLoad<uint16>((uint16*)data + 3);

    d->geometry.reserve(geometry_count);
    for (int i(0); i < geometry_count; ++i)
      d->geometry.push_back(Quad_Coord(unalignedLoad<uint32>(start_ptr + 4*i), unalignedLoad<uint32>(start_ptr + 4*i + 2)));

  }

  Way_Skeleton(const Way& way)
      : id(way.id),  d(new Way_Skeleton_Data) {

    d->nds = way.nds;
    d->geometry = way.geometry;
  }

  Way_Skeleton(Id_Type id_,  std::vector< Node::Id_Type >&& nds_)
      : id(id_),  d(new Way_Skeleton_Data) {

    d->nds = std::move(nds_);
  }


  Way_Skeleton(Id_Type id_, const std::vector< Node::Id_Type >& nds_, const std::vector< Quad_Coord >& geometry_)
      : id(id_),  d(new Way_Skeleton_Data) {

    d->nds = nds_;
    d->geometry = geometry_;
  }

  const std::vector< Node::Id_Type > & nds() const { return d->nds; }
  std::vector< Node::Id_Type > & nds() { return d->nds; }

  const std::vector< Quad_Coord > & geometry() const { return d->geometry; }
  std::vector< Quad_Coord > & geometry() { return d->geometry; }

  uint32 size_of() const
  {
    uint32 compress_size = calculate_ids_compressed_size(d->nds);
    return 8 + 2 + compress_size + 8*d->geometry.size();
  }

  static uint32 size_of(const void* data) noexcept
  {
    return (8 + 2 +
            8 * unalignedLoad<uint16>((uint16*)data + 3) +      // geometry size elements, 8 byte per element
            unalignedLoad<uint16>((uint16*)data + 4));          // nds_compressed_size (in bytes)
  }

  void to_data(void* data) const
  {
    unalignedStore(data, id.val());
    unalignedStore(((uint16*)data + 2), (uint16) d->nds.size());
    unalignedStore(((uint16*)data + 3), (uint16) d->geometry.size());

    auto* start_ptr = (uint16*) compress_ids(d->nds, (uint8*)data + 10);
    auto nds_compressed_size = (uint16) ((uint8*)start_ptr - ((uint8*)data + 10));
    unalignedStore(((uint16*)data + 4), (uint16) nds_compressed_size);

    for (uint i(0); i < d->geometry.size(); ++i)
    {
      unalignedStore(start_ptr + 4*i,     d->geometry[i].ll_upper);
      unalignedStore(start_ptr + 4*i + 2, d->geometry[i].ll_lower);
    }
  }

  bool operator<(const Way_Skeleton& a) const noexcept
  {
    return this->id < a.id;
  }

  bool operator==(const Way_Skeleton& a) const noexcept
  {
    return this->id == a.id;
  }

  template <class T, class Object>
  using Handle_Methods = Way_Skeleton_Handle_Methods<T, Object>;

private:
  SharedDataPointer<Way_Skeleton_Data> d;
};


template <class T, class Object>
struct Way_Skeleton_Handle_Methods
{
  typename Object::Id_Type inline id() const {
     return (static_cast<const T*>(this)->apply_func(Way_Skeleton_Id_Functor<typename Object::Id_Type>()));
  }

  Object inline get_element() const {
    return (static_cast<const T*>(this)->apply_func(Generic_Element_Functor<Object>()));
  }

  void inline add_element(std::vector< Object > & v) const {
    static_cast<const T*>(this)->apply_func(Generic_Add_Element_Functor<Object>(v));
  }

  uint16 inline get_nds_size() const {
    return (static_cast<const T*>(this)->apply_func(Way_Skeleton_Nds_Size_Functor()));
  }

private:
  template <typename Id_Type >
  struct Way_Skeleton_Id_Functor {
    Way_Skeleton_Id_Functor() = default;

    using reference_type = Way_Skeleton;

    Id_Type operator()(const void* data) const
     {
       return unalignedLoad<Id_Type>(data);
     }
  };

  struct Way_Skeleton_Nds_Size_Functor {
    Way_Skeleton_Nds_Size_Functor() = default;

    using reference_type = Way_Skeleton;

    uint16 operator()(const void* data) const
     {
       return unalignedLoad<uint16>((uint16*)data + 2);
     }
  };
};

inline std::ostream & operator<<(std::ostream &os, const Way_Skeleton & p)
{
  return os << "Way(" << p.id << ", " << p.nds().size() << " elm)";
}



template <class T, class Object>
struct Way_Delta_Handle_Methods;

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

  Way_Delta(const void* data) : id(unalignedLoad<Id_Type>(data)), full(false)
  {
    if (unalignedLoad<uint32>((uint32*)data + 1) == 0xffffffff)
    {
      full = true;
      nds_removed.clear();
      nds_added.resize(unalignedLoad<uint32>((uint32*)data + 2));
      geometry_removed.clear();
      geometry_added.resize(unalignedLoad<uint32>((uint32*)data + 3), std::make_pair(0, Quad_Coord()));

      uint8* ptr = ((uint8*)data) + 16;

      for (uint i(0); i < nds_added.size(); ++i)
      {
        nds_added[i].first = i;
        nds_added[i].second = unalignedLoad<uint64>(ptr);
        ptr += 8;
      }

      for (uint i = 0; i < geometry_added.size(); ++i)
      {
        geometry_added[i].first = i;
        geometry_added[i].second = unalignedLoad<Quad_Coord>(ptr);
        ptr += 8;
      }
    }
    else
    {
      nds_removed.resize(unalignedLoad<uint32>((uint32*)data + 1));
      nds_added.resize(unalignedLoad<uint32>((uint32*)data + 2));
      geometry_removed.resize(unalignedLoad<uint32>((uint32*)data + 3));
      geometry_added.resize(unalignedLoad<uint32>((uint32*)data + 4), std::make_pair(0, Quad_Coord()));

      uint8* ptr = ((uint8*)data) + 20;

      for (uint i(0); i < nds_removed.size(); ++i)
      {
        nds_removed[i] = unalignedLoad<uint32>(ptr);
        ptr += 4;
      }

      for (uint i(0); i < nds_added.size(); ++i)
      {
        nds_added[i].first = unalignedLoad<uint32>(ptr);
        nds_added[i].second = unalignedLoad<uint64>(ptr + 4);
        ptr += 12;
      }

      for (uint i = 0; i < geometry_removed.size(); ++i)
      {
        geometry_removed[i] = unalignedLoad<uint32>(ptr);
        ptr += 4;
      }

      for (uint i = 0; i < geometry_added.size(); ++i)
      {
        geometry_added[i].first = unalignedLoad<uint32>(ptr);
        geometry_added[i].second = unalignedLoad<Quad_Coord>(ptr + 4);
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

  static uint32 size_of(const void* data) noexcept
  {
    if (unalignedLoad<uint32>((uint32*)data + 1) == 0xffffffff)
      return 16 + 8 * unalignedLoad<uint32>((uint32*)data + 2) +
                  8 * unalignedLoad<uint32>((uint32*)data + 3);
    else
      return 20 + 4 * unalignedLoad<uint32>((uint32*)data + 1) +
                 12 * unalignedLoad<uint32>((uint32*)data + 2) +
                  4 * unalignedLoad<uint32>((uint32*)data + 3) +
                 12 * unalignedLoad<uint32>((uint32*)data + 4);
  }

  void to_data(void* data) const
  {
    unalignedStore(data, id.val());
    if (full)
    {
      unalignedStore((uint32*)data + 1, (uint32) 0xffffffff);
      unalignedStore((uint32*)data + 2, (uint32) nds_added.size());
      unalignedStore((uint32*)data + 3, (uint32) geometry_added.size());

      uint8* ptr = ((uint8*)data) + 16;

      for (uint i = 0; i < nds_added.size(); ++i)
      {
        unalignedStore(ptr, (uint64) nds_added[i].second.val());
        ptr += 8;
      }

      for (uint i = 0; i < geometry_added.size(); ++i)
      {
        unalignedStore(ptr, (Quad_Coord) geometry_added[i].second);
        ptr += 8;
      }
    }
    else
    {
      unalignedStore((uint32*)data + 1, (uint32) nds_removed.size());
      unalignedStore((uint32*)data + 2, (uint32) nds_added.size());
      unalignedStore((uint32*)data + 3, (uint32) geometry_removed.size());
      unalignedStore((uint32*)data + 4, (uint32) geometry_added.size());

      uint8* ptr = ((uint8*)data) + 20;

      for (uint i = 0; i < nds_removed.size(); ++i)
      {
        unalignedStore(ptr, (uint32) nds_removed[i]);
        ptr += 4;
      }

      for (uint i = 0; i < nds_added.size(); ++i)
      {
        unalignedStore(ptr, (uint32) nds_added[i].first);
        unalignedStore(ptr + 4, (uint64) nds_added[i].second.val());
        ptr += 12;
      }

      for (uint i = 0; i < geometry_removed.size(); ++i)
      {
        unalignedStore(ptr, (uint32) geometry_removed[i]);
        ptr += 4;
      }

      for (uint i = 0; i < geometry_added.size(); ++i)
      {
        unalignedStore(ptr, (uint32) geometry_added[i].first);
        unalignedStore(ptr + 4, (Quad_Coord) geometry_added[i].second);
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

  template <class T, class Object>
  using Handle_Methods = Way_Delta_Handle_Methods<T, Object>;

};

template <class T, class Object>
struct Way_Delta_Handle_Methods
{
  typename Object::Id_Type inline id() const {
     return (static_cast<const T*>(this)->apply_func(Way_Delta_Id_Functor<typename Object::Id_Type>()));
  }

private:

  template <typename Id_Type >
  struct Way_Delta_Id_Functor {
    Way_Delta_Id_Functor() = default;

    using reference_type = Way_Delta;

    Id_Type operator()(const void* data) const
     {
       return unalignedLoad<Id_Type>(data);
     }
  };
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
