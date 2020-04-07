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

#ifndef DE__OSM3S___OVERPASS_API__CORE__TYPE_RELATION_H
#define DE__OSM3S___OVERPASS_API__CORE__TYPE_RELATION_H

#include "basic_types.h"
#include "index_computations.h"

#include <cstring>
#include <map>
#include <set>
#include <string>
#include <vector>


struct Relation_Entry
{
  typedef Uint64 Ref_Type;

  Relation_Entry() : ref(0ull), type(0), role(0) {}

  Uint64 ref;
  uint32 type;
  uint32 role;
  const static uint32 NODE = 1;
  const static uint32 WAY = 2;
  const static uint32 RELATION = 3;

  bool operator==(const Relation_Entry& a) const
  {
    return (a.ref == this->ref && a.type == this->type && a.role == this->role);
  }

  Uint32_Index ref32() const { return Uint32_Index(ref.val()); }
};


struct Relation
{
  typedef Uint32_Index Id_Type;

  Id_Type id;
  uint32 index;
  std::vector< Relation_Entry > members;
  std::vector< Uint31_Index > node_idxs;
  std::vector< Uint31_Index > way_idxs;
  std::vector< std::pair< std::string, std::string > > tags;

  Relation() : id(0u) {}

  Relation(Id_Type id_) : id(id_) {}

  Relation(Id_Type id_, uint32 index_, const std::vector< Relation_Entry >& members_)
  : id(id_), index(index_), members(members_) {}

  static uint32 calc_index(const std::vector< uint32 >& memb_idxs)
  {
    return ::calc_index(memb_idxs);
  }

  static bool indicates_geometry(Uint31_Index index)
  {
    return ((index.val() & 0x80000000) != 0 && ((index.val() & 0x1) == 0));
  }
};


struct Relation_Comparator_By_Id {
  bool operator() (const Relation* a, const Relation* b)
  {
    return (a->id < b->id);
  }
};


struct Relation_Equal_Id {
  bool operator() (const Relation* a, const Relation* b)
  {
    return (a->id == b->id);
  }
};


struct Relation_Delta;

template <class T, class Object>
struct Relation_Skeleton_Handle_Methods;

class Relation_Skeleton_Data final : public SharedData
{
public:
  Relation_Skeleton_Data() { }

  Relation_Skeleton_Data(const Relation_Skeleton_Data &other)
     : SharedData(other),
       members(other.members),
       node_idxs(other.node_idxs),
       way_idxs(other.way_idxs) {}

  ~Relation_Skeleton_Data() {}

  std::vector< Relation_Entry > members;
  std::vector< Uint31_Index > node_idxs;
  std::vector< Uint31_Index > way_idxs;
};


struct Relation_Skeleton
{
  typedef Relation::Id_Type Id_Type;
  typedef Relation_Delta Delta;

  Id_Type id;

  Relation_Skeleton() : id(0u) { d = new Relation_Skeleton_Data; }

  Relation_Skeleton(Relation::Id_Type id_) : id(id_) { d = new Relation_Skeleton_Data;}

  Relation_Skeleton(void* data) : id(*(Id_Type*)data)
  {
    d = new Relation_Skeleton_Data;

    d->members.resize(*((uint32*)data + 1));
    d->node_idxs.resize(*((uint32*)data + 2), 0u);
    d->way_idxs.resize(*((uint32*)data + 3), 0u);
    for (uint i(0); i < *((uint32*)data + 1); ++i)
    {
      d->members[i].ref = *(uint64*)((uint32*)data + 4 + 3*i);
      d->members[i].role = *((uint32*)data + 6 + 3*i) & 0xffffff;
      d->members[i].type = *((uint8*)data + 27 + 12*i);
    }
    uint32* start_ptr = (uint32*)data + 4 + 3* d->members.size();
    for (uint i = 0; i < d->node_idxs.size(); ++i)
      d->node_idxs[i] = *(start_ptr + i);
    start_ptr = (uint32*)data + 4 + 3* d->members.size() + d->node_idxs.size();
    for (uint i = 0; i < d->way_idxs.size(); ++i)
      d->way_idxs[i] = *(start_ptr + i);
  }

  Relation_Skeleton(const Relation& rel)
      : id(rel.id) {

    d = new Relation_Skeleton_Data;
    d->members = rel.members;
    d->node_idxs = rel.node_idxs;
    d->way_idxs = rel.way_idxs;

  }

  Relation_Skeleton(Id_Type id_, const std::vector< Relation_Entry >& members_,
		    const std::vector< Uint31_Index >& node_idxs_,
		    const std::vector< Uint31_Index >& way_idxs_)
      : id(id_) {

    d = new Relation_Skeleton_Data;
    d->members = members_;
    d->node_idxs = node_idxs_;
    d->way_idxs = way_idxs_;

  }

  uint32 size_of() const
  {
    return 16 + 12* d->members.size() + 4* d->node_idxs.size() + 4* d->way_idxs.size();
  }

  static uint32 size_of(const void* data)
  {
    return 16 + 12 * *((uint32*)data + 1) + 4* *((uint32*)data + 2) + 4* *((uint32*)data + 3);
  }

  void to_data(void* data) const
  {
    *(Id_Type*)data = id.val();
    *((uint32*)data + 1) = d->members.size();
    *((uint32*)data + 2) = d->node_idxs.size();
    *((uint32*)data + 3) = d->way_idxs.size();
    for (uint i = 0; i < d->members.size(); ++i)
    {
      *(uint64*)((uint32*)data + 4 + 3*i) = d->members[i].ref.val();
      *((uint32*)data + 6 + 3*i) = d->members[i].role & 0xffffff;
      *((uint8*)data + 27 + 12*i) = d->members[i].type;
    }
    Uint31_Index* start_ptr = (Uint31_Index*)data + 4 + 3* d->members.size();
    for (uint i = 0; i < d->node_idxs.size(); ++i)
      *(start_ptr + i) = d->node_idxs[i];
    start_ptr = (Uint31_Index*)data + 4 + 3* d->members.size() + d->node_idxs.size();
    for (uint i = 0; i < d->way_idxs.size(); ++i)
      *(start_ptr + i) = d->way_idxs[i];
  }

  const std::vector< Relation_Entry > & members() const { return d->members; }
  std::vector< Relation_Entry > & members() { return d->members; }

  const std::vector< Uint31_Index > & node_idxs() const { return d->node_idxs; }
  std::vector< Uint31_Index > & node_idxs() { return d->node_idxs; }

  const std::vector< Uint31_Index > & way_idxs() const { return d->way_idxs; }
  std::vector< Uint31_Index > & way_idxs() { return d->way_idxs; }


  bool operator<(const Relation_Skeleton& a) const
  {
    return this->id < a.id;
  }

  bool operator==(const Relation_Skeleton& a) const
  {
    return this->id == a.id;
  }

  template <class T, class Object>
  using Handle_Methods = Relation_Skeleton_Handle_Methods<T, Object>;

private:
  SharedDataPointer<Relation_Skeleton_Data> d;
};


template <typename Id_Type >
struct Relation_Skeleton_Id_Functor {
  Relation_Skeleton_Id_Functor() {};

  using reference_type = Relation_Skeleton;

  Id_Type operator()(const void* data) const
   {
     return *(Id_Type*)data;
   }
};


template <class T, class Object>
struct Relation_Skeleton_Handle_Methods
{
  typename Object::Id_Type inline id() const {
     return (static_cast<const T*>(this)->apply_func(Relation_Skeleton_Id_Functor<typename Object::Id_Type>()));
  }

};

struct Relation_Delta
{
  typedef Relation_Skeleton::Id_Type Id_Type;

  Id_Type id;
  bool full;
  std::vector< uint > members_removed;
  std::vector< std::pair< uint, Relation_Entry > > members_added;
  std::vector< uint > node_idxs_removed;
  std::vector< std::pair< uint, Uint31_Index > > node_idxs_added;
  std::vector< uint > way_idxs_removed;
  std::vector< std::pair< uint, Uint31_Index > > way_idxs_added;

  Relation_Delta() : id(0u), full(false) {}

  Relation_Delta(void* data) : id(*(Id_Type*)data), full(false)
  {
    if (*((uint32*)data + 1) == 0xffffffff)
    {
      full = true;
      members_removed.clear();
      members_added.resize(*((uint32*)data + 2));
      node_idxs_removed.clear();
      node_idxs_added.resize(*((uint32*)data + 3), std::make_pair(0, 0u));
      way_idxs_removed.clear();
      way_idxs_added.resize(*((uint32*)data + 4), std::make_pair(0, 0u));

      uint8* ptr = ((uint8*)data) + 20;

      for (uint i(0); i < members_added.size(); ++i)
      {
        members_added[i].first = i;
        members_added[i].second.ref = *(uint64*)((uint32*)ptr);
        members_added[i].second.role = *((uint32*)(ptr + 8)) & 0xffffff;
        members_added[i].second.type = *((uint8*)(ptr + 11));
        ptr += 12;
      }

      for (uint i = 0; i < node_idxs_added.size(); ++i)
      {
        node_idxs_added[i].first = i;
        node_idxs_added[i].second = *((uint32*)ptr);
        ptr += 4;
      }

      for (uint i = 0; i < way_idxs_added.size(); ++i)
      {
        way_idxs_added[i].first = i;
        way_idxs_added[i].second = *((uint32*)ptr);
        ptr += 4;
      }
    }
    else
    {
      members_removed.resize(*((uint32*)data + 1));
      members_added.resize(*((uint32*)data + 2));
      node_idxs_removed.resize(*((uint32*)data + 3));
      node_idxs_added.resize(*((uint32*)data + 4), std::make_pair(0, 0u));
      way_idxs_removed.resize(*((uint32*)data + 5));
      way_idxs_added.resize(*((uint32*)data + 6), std::make_pair(0, 0u));

      uint8* ptr = ((uint8*)data) + 28;

      for (uint i(0); i < members_removed.size(); ++i)
      {
        members_removed[i] = *((uint32*)ptr);
        ptr += 4;
      }

      for (uint i(0); i < members_added.size(); ++i)
      {
        members_added[i].first = *((uint32*)ptr);
        members_added[i].second.ref = *(uint64*)((uint32*)(ptr + 4));
        members_added[i].second.role = *((uint32*)(ptr + 12)) & 0xffffff;
        members_added[i].second.type = *((uint8*)(ptr + 15));
        ptr += 16;
      }

      for (uint i = 0; i < node_idxs_removed.size(); ++i)
      {
        node_idxs_removed[i] = *((uint32*)ptr);
        ptr += 4;
      }

      for (uint i = 0; i < node_idxs_added.size(); ++i)
      {
        node_idxs_added[i].first = *((uint32*)ptr);
        node_idxs_added[i].second = *((uint32*)(ptr + 4));
        ptr += 8;
      }

      for (uint i = 0; i < way_idxs_removed.size(); ++i)
      {
        way_idxs_removed[i] = *((uint32*)ptr);
        ptr += 4;
      }

      for (uint i = 0; i < way_idxs_added.size(); ++i)
      {
        way_idxs_added[i].first = *((uint32*)ptr);
        way_idxs_added[i].second = *((uint32*)(ptr + 4));
        ptr += 8;
      }
    }
  }

  Relation_Delta(const Relation_Skeleton& reference, const Relation_Skeleton& skel)
    : id(skel.id), full(false)
  {
    if (!(id == skel.id))
      full = true;
    else
    {
      make_delta(skel.members(), reference.members(), members_removed, members_added);
      make_delta(skel.node_idxs(), reference.node_idxs(), node_idxs_removed, node_idxs_added);
      make_delta(skel.way_idxs(), reference.way_idxs(), way_idxs_removed, way_idxs_added);
    }

    if (members_added.size() >= skel.members().size()/2)
    {
      members_removed.clear();
      members_added.clear();
      node_idxs_removed.clear();
      node_idxs_added.clear();
      way_idxs_removed.clear();
      way_idxs_added.clear();
      full = true;
    }

    if (full)
    {
      copy_elems(skel.members(), members_added);
      copy_elems(skel.node_idxs(), node_idxs_added);
      copy_elems(skel.way_idxs(), way_idxs_added);
    }
  }

  Relation_Skeleton expand(const Relation_Skeleton& reference) const
  {
    Relation_Skeleton result(id);

    if (full)
    {
      result.members().reserve(members_added.size());
      for (uint i = 0; i < members_added.size(); ++i)
        result.members().push_back(members_added[i].second);

      result.node_idxs().reserve(node_idxs_added.size());
      for (uint i = 0; i < node_idxs_added.size(); ++i)
        result.node_idxs().push_back(node_idxs_added[i].second);

      result.way_idxs().reserve(way_idxs_added.size());
      for (uint i = 0; i < way_idxs_added.size(); ++i)
        result.way_idxs().push_back(way_idxs_added[i].second);
    }
    else if (reference.id == id)
    {
      expand_diff(reference.members(), members_removed, members_added, result.members());
      expand_diff(reference.node_idxs(), node_idxs_removed, node_idxs_added, result.node_idxs());
      expand_diff(reference.way_idxs(), way_idxs_removed, way_idxs_added, result.way_idxs());
    }
    else
      result.id = 0u;

    return result;
  }

  Relation_Skeleton expand_fast(Relation_Skeleton& reference) const
  {
    Relation_Skeleton result(id);

    if (full)
    {
      result.members().reserve(members_added.size());
      for (uint i = 0; i < members_added.size(); ++i)
        result.members().push_back(members_added[i].second);

      result.node_idxs().reserve(node_idxs_added.size());
      for (uint i = 0; i < node_idxs_added.size(); ++i)
        result.node_idxs().push_back(node_idxs_added[i].second);

      result.way_idxs().reserve(way_idxs_added.size());
      for (uint i = 0; i < way_idxs_added.size(); ++i)
        result.way_idxs().push_back(way_idxs_added[i].second);
    }
    else if (reference.id == id)
    {
      expand_diff_fast(reference.members(), members_removed, members_added, result.members());
      expand_diff_fast(reference.node_idxs(), node_idxs_removed, node_idxs_added, result.node_idxs());
      expand_diff_fast(reference.way_idxs(), way_idxs_removed, way_idxs_added, result.way_idxs());
    }
    else
      result.id = 0u;

    return result;
  }

  uint32 size_of() const
  {
    if (full)
      return 20 + 12*members_added.size() + 4*node_idxs_added.size() + 4*way_idxs_added.size();
    else
      return 28 + 4*members_removed.size() + 16*members_added.size()
          + 4*node_idxs_removed.size() + 8*node_idxs_added.size()
          + 4*way_idxs_removed.size() + 8*way_idxs_added.size();
  }

  static uint32 size_of(const void* data)
  {
    if (*((uint32*)data + 1) == 0xffffffff)
      return 20 + 12 * *((uint32*)data + 2) + 4 * *((uint32*)data + 3) + 4 * *((uint32*)data + 4);
    else
      return 28 + 4 * *((uint32*)data + 1) + 16 * *((uint32*)data + 2)
          + 4 * *((uint32*)data + 3) + 8 * *((uint32*)data + 4)
          + 4 * *((uint32*)data + 5) + 8 * *((uint32*)data + 6);
  }

  void to_data(void* data) const
  {
    *(Id_Type*)data = id.val();
    if (full)
    {
      *((uint32*)data + 1) = 0xffffffff;
      *((uint32*)data + 2) = members_added.size();
      *((uint32*)data + 3) = node_idxs_added.size();
      *((uint32*)data + 4) = way_idxs_added.size();

      uint8* ptr = ((uint8*)data) + 20;

      for (uint i = 0; i < members_added.size(); ++i)
      {
        *(uint64*)((uint32*)ptr) = members_added[i].second.ref.val();
        *((uint32*)(ptr + 8)) = members_added[i].second.role & 0xffffff;
        *((uint8*)(ptr + 11)) = members_added[i].second.type;
        ptr += 12;
      }

      for (uint i = 0; i < node_idxs_added.size(); ++i)
      {
        *((Uint31_Index*)ptr) = node_idxs_added[i].second;
        ptr += 4;
      }

      for (uint i = 0; i < way_idxs_added.size(); ++i)
      {
        *((Uint31_Index*)ptr) = way_idxs_added[i].second;
        ptr += 4;
      }
    }
    else
    {
      *((uint32*)data + 1) = members_removed.size();
      *((uint32*)data + 2) = members_added.size();
      *((uint32*)data + 3) = node_idxs_removed.size();
      *((uint32*)data + 4) = node_idxs_added.size();
      *((uint32*)data + 5) = way_idxs_removed.size();
      *((uint32*)data + 6) = way_idxs_added.size();

      uint8* ptr = ((uint8*)data) + 28;

      for (uint i = 0; i < members_removed.size(); ++i)
      {
        *((uint32*)ptr) = members_removed[i];
        ptr += 4;
      }

      for (uint i = 0; i < members_added.size(); ++i)
      {
        *((uint32*)ptr) = members_added[i].first;
        *(uint64*)((uint32*)(ptr + 4)) = members_added[i].second.ref.val();
        *((uint32*)(ptr + 12)) = members_added[i].second.role & 0xffffff;
        *((uint8*)(ptr + 15)) = members_added[i].second.type;
        ptr += 16;
      }

      for (uint i = 0; i < node_idxs_removed.size(); ++i)
      {
        *((uint32*)ptr) = node_idxs_removed[i];
        ptr += 4;
      }

      for (uint i = 0; i < node_idxs_added.size(); ++i)
      {
        *((uint32*)ptr) = node_idxs_added[i].first;
        *((Uint31_Index*)(ptr + 4)) = node_idxs_added[i].second;
        ptr += 8;
      }

      for (uint i = 0; i < way_idxs_removed.size(); ++i)
      {
        *((uint32*)ptr) = way_idxs_removed[i];
        ptr += 4;
      }

      for (uint i = 0; i < way_idxs_added.size(); ++i)
      {
        *((uint32*)ptr) = way_idxs_added[i].first;
        *((Uint31_Index*)(ptr + 4)) = way_idxs_added[i].second;
        ptr += 8;
      }
    }
  }

  bool operator<(const Relation_Delta& a) const
  {
    return this->id < a.id;
  }

  bool operator==(const Relation_Delta& a) const
  {
    return this->id == a.id;
  }
};


#endif
