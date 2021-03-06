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

#ifndef DE__OSM3S___OVERPASS_API__CORE__TYPE_NODE_H
#define DE__OSM3S___OVERPASS_API__CORE__TYPE_NODE_H

#include "basic_types.h"
#include "index_computations.h"

#include <string>
#include <vector>


struct Node_Base
{
  typedef Uint40 Id_Type;

  Id_Type id;
  uint32 index;
  uint32 ll_lower_;

  bool operator<(const Node_Base& a) const noexcept
  {
    return this->id.val() < a.id.val();
  }

  bool operator==(const Node_Base& a) const noexcept
  {
    return this->id.val() == a.id.val();
  }

  Node_Base() noexcept : id(0ull), index(0), ll_lower_(0) {}

  Node_Base(Id_Type id_, double lat, double lon) noexcept
      : id(id_), index(ll_upper_(lat, lon)), ll_lower_(ll_lower(lat, lon))
  {}

  Node_Base(Id_Type id_, uint32 ll_upper_, uint32 ll_lower__) noexcept
      : id(id_), index(ll_upper_), ll_lower_(ll_lower__)
  {}

};


struct Node : public Node_Base
{
  std::vector< std::pair< std::string, std::string > > tags;

  Node() noexcept : Node_Base() {}

  Node(Id_Type id_, double lat, double lon) noexcept : Node_Base(id_, lat, lon) {}

  Node(Id_Type id_, uint32 ll_upper_, uint32 ll_lower__) noexcept : Node_Base(id_, ll_upper_, ll_lower__) {}

};


struct Node_Comparator_By_Id {
  bool operator() (const Node_Base& a, const Node_Base& b) noexcept
  {
    return (a.id.val() < b.id.val());
  }

  bool operator() (const Node_Base* a, const Node_Base* b) noexcept
  {
    return (a->id.val() < b->id.val());
  }
};


struct Node_Equal_Id {
  bool operator() (const Node_Base& a, const Node_Base& b) noexcept
  {
    return (a.id.val() == b.id.val());
  }

  bool operator() (const Node_Base* a, const Node_Base* b) noexcept
  {
    return (a->id.val() == b->id.val());
  }
};

template <class T, class Object>
struct Node_Skeleton_Handle_Methods;

struct Node_Skeleton
{
  typedef Node::Id_Type Id_Type;
  typedef Node_Skeleton Delta;

  Node::Id_Type id;
  uint32 ll_lower;

  Node_Skeleton() noexcept : id(0ull) {}

  Node_Skeleton(const void* data) noexcept
    : id(data), ll_lower(*(uint32*)((uint8*)data + Id_Type::max_size_of())) {}

  Node_Skeleton(const Node& node) noexcept
  : id(node.id), ll_lower(node.ll_lower_) {}

  Node_Skeleton(Node::Id_Type id_) noexcept
  : id(id_), ll_lower(0) {}

  Node_Skeleton(Node::Id_Type id_, uint32 ll_lower_) noexcept
  : id(id_), ll_lower(ll_lower_) {}

  uint32 size_of() const noexcept
  {
    return 4 + Id_Type::max_size_of();
  }

  static uint32 size_of(const void* data) noexcept
  {
    return 4 + Id_Type::max_size_of();
  }

  void to_data(void* data) const noexcept
  {
    id.to_data(data);
    *(uint32*)((uint8*)data + Id_Type::max_size_of()) = ll_lower;
  }

  bool operator<(const Node_Skeleton& a) const noexcept
  {
    return this->id.val() < a.id.val();
  }

  bool operator==(const Node_Skeleton& a) const noexcept
  {
    return this->id.val() == a.id.val();
  }

  template <class T, class Object>
  using Handle_Methods = Node_Skeleton_Handle_Methods<T, Object>;
};

template <typename Id_Type >
struct Node_Skeleton_Id_Functor {
  Node_Skeleton_Id_Functor() {};

  using reference_type = Node_Skeleton;

  Id_Type operator()(const void* data) const
   {
     return Id_Type(data);
   }
};

template <typename Id_Type >
struct Node_Skeleton_ll_lower_Functor {
  Node_Skeleton_ll_lower_Functor() {};

  using reference_type = Node_Skeleton;

  uint32 operator()(const void* data) const
   {
     return (*(uint32*)((uint8*)data + Id_Type::max_size_of()));
   }
};

template <typename Id_Type >
struct Node_Skeleton_Element_Functor {
  Node_Skeleton_Element_Functor() {};

  using reference_type = Node_Skeleton;

  Node_Skeleton operator()(const void* data) const
   {
     return Node_Skeleton(data);
   }
};

template <typename Id_Type >
struct Node_Skeleton_Add_Element_Functor {
  Node_Skeleton_Add_Element_Functor(std::vector< Node_Skeleton >& v_) : v(v_) {};

  using reference_type = Node_Skeleton;

  void operator()(const void* data) const
   {
     v.emplace_back(data);
   }

private:
  std::vector< Node_Skeleton > & v;
};

template <class T, class Object>
struct Node_Skeleton_Handle_Methods
{
  typename Object::Id_Type inline id() const {
     return (static_cast<const T*>(this)->apply_func(Node_Skeleton_Id_Functor<typename Object::Id_Type>()));
  }

  uint32 inline get_ll_lower() const {
     return (static_cast<const T*>(this)->apply_func(Node_Skeleton_ll_lower_Functor<typename Object::Id_Type>()));
  }

  Node_Skeleton inline get_element() const {
    return (static_cast<const T*>(this)->apply_func(Node_Skeleton_Element_Functor<typename Object::Id_Type>()));
  }

  void inline add_element(std::vector< Object > & v) const {
    static_cast<const T*>(this)->apply_func(Node_Skeleton_Add_Element_Functor<typename Object::Id_Type>(v));
  }
};


#endif
