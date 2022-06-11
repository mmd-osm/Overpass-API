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

// ------------------------------------------------------------------------------------------


#ifdef HAVE_DENSE_WAYS

namespace osm3s_experimental {

template <typename T>
class nds_type;

template <typename T>
class geometry_type;

class Way_Skeleton_Dense_Data {
  using value_type = Node::Id_Type;

private:

  // Structure to store 4 40-bit node ids in 20 bytes, avoiding extra padding bytes
  struct Val4 {
    uint8_t hi[4];
    uint32_t low[4];

    bool operator==( const Val4& rhs ) const noexcept {
      return hi == rhs.hi &&
          low == rhs.low;
    }
  };

  static_assert(sizeof(Val4) == 20, "Val4 has wrong size");

public:
  Way_Skeleton_Dense_Data() = default;

  Way_Skeleton_Dense_Data(const Way_Skeleton_Dense_Data& other) {
    if (other.r.__s.is_long) {
      r.__s.size = other.r.__s.size;
      r.__s.is_closed = other.r.__s.is_closed;
      r.__s.is_long = other.r.__s.is_long;

      int effective_size = r.__s.size - r.__s.is_closed;
      int var4_elems = val4_array_len(effective_size);

      r.__l.nds = new Val4[var4_elems];
      std::memcpy(r.__l.nds, other.r.__l.nds, sizeof(Val4) * var4_elems);

      r.__l.size_geom = other.r.__l.size_geom;
      if (r.__l.size_geom > 0) {
        r.__l.geom = new Quad_Coord[r.__l.size_geom];
        std::memcpy(r.__l.geom, other.r.__l.geom, sizeof(Quad_Coord) * r.__l.size_geom);
      }
    } else {
      r.__s = other.r.__s;
    }
    r.__s.refcnt = 1;
  };

  // using the assignment operator would lead to corruption in the
      // ref-counting
  Way_Skeleton_Dense_Data& operator=(const Way_Skeleton_Dense_Data&) = delete;

  bool operator==( const Way_Skeleton_Dense_Data& rhs ) const noexcept {

    if (r.__s.is_long != rhs.r.__s.is_long)
      return false;

    if (r.__s.is_long) {
      return r.__l.size == rhs.r.__l.size &&
          r.__l.is_closed == rhs.r.__l.is_closed &&
          r.__l.size_geom == rhs.r.__l.size_geom &&
          r.__l.nds == rhs.r.__l.nds &&
          r.__l.geom == rhs.r.__l.geom;
    }
    else {
      return r.__s.size == rhs.r.__s.size &&
          r.__s.is_closed == rhs.r.__s.is_closed &&
          r.__s.nds == rhs.r.__s.nds;
    }

  }

  Way_Skeleton_Dense_Data(const std::vector<Node::Id_Type>& ids, const std::vector<Quad_Coord>& geom) {
    if (ids.empty()) {
      return;
    }

    if (ids.size() > std::numeric_limits<std::uint16_t>::max()) {
      throw std::runtime_error("vector too large");
    }

    r.__s.is_closed = (ids.front() == ids.back() && ids.size() > 1);
    r.__s.size = ids.size();

    bool has_geometry = !(geom.empty());

    // closed ways with 5 nodes can be saved using 4 nodes only, b/c node 4
    // is the same as node 0. this comes in handy to store buildings with 4
    // distinct nodes in 24 bytes only.
    // this optimization only applies, if the way doesn't have a geometry
    // (otherwise it wouldn't fit in the union struct)

    r.__s.is_long = has_geometry ||
        (r.__s.is_closed ? (ids.size() > 5) : (ids.size() > 4));

    // effective node ids count, taking is_closed into account
    int effective_size = ids.size() - r.__s.is_closed;

    if (r.__s.is_long) {
      r.__l.nds = new Val4[val4_array_len(effective_size)];

      for (int i = 0; i < effective_size; i++) {
        setLong(r.__l.nds[i >> 2], i & 0x3, ids[i].val());
      }

      if (has_geometry) {
        r.__l.size_geom = geom.size();
        r.__l.geom = new Quad_Coord[r.__l.size_geom];
        for (unsigned int i = 0; i < geom.size(); i++) {
          r.__l.geom[i] = geom[i];
        }
      }
    } else {
      for (int i = 0; i < effective_size; i++) {
        setLong(r.__s.nds, i & 0x3, ids[i].val());
      }
    }
  }

  ~Way_Skeleton_Dense_Data() {
    delete_arrays();
  }

  // Fetch n-th node id in way (zero based)
  value_type get_nd(int a) const noexcept {
    bool fake_last_node = (r.__s.is_closed && (a == (r.__s.size - 1)));

    if (r.__s.is_long) {
      if (fake_last_node) {
        return getLong(r.__l.nds[0], 0);
      } else {
        return getLong(r.__l.nds[a >> 2], a & 0x3);
      }
    } else {
      // last node of a closed way requested -> return node 0 instead
      if (fake_last_node) {
        return getLong(r.__s.nds, 0);
      }
      return getLong(r.__s.nds, a);
    }
  }

  // Real number of nodes in way
  int size_nds() const noexcept {
    return r.__s.size;
  }

  bool empty_nds() const noexcept {
    return size_nds() == 0;
  }

  int size_geom() const noexcept {
    return (r.__s.is_long ? r.__l.size_geom : 0);
  }

  bool empty_geom() const noexcept {
    return (size_geom() == 0);
  }

  //
  bool is_closed() const noexcept {
    if (empty_nds())
      return false;
    // way is closed if
    // (1) is_closed flag indicates that first node is identical to last node
    // (2) is_closed flag is not set. However, we have at least 2 nodes,
    //     and the first one is identical to the last one
    return r.__s.is_closed || (!r.__s.is_closed && size_nds() > 1 && get_nd(0) == get_nd(size_nds() - 1));
  }

  uint8_t& ref() noexcept { return r.__s.refcnt; }

  void clean() {
    delete_arrays();
  }

  std::vector< Node::Id_Type > get_nds() const {

    std::vector< Node::Id_Type > ids;
    if (empty_nds())
      return ids;

    ids.reserve(r.__s.size);
    for (int i = 0; i < r.__s.size; i++) {
      ids.push_back(get_nd(i));
    }
    return ids;
  }

private:
  void delete_arrays() {
    if (r.__s.is_long) {
      delete[] r.__l.nds;
      delete[] r.__l.geom;
      r.__l.nds = nullptr;
      r.__l.geom = nullptr;
      r.__l.size = 0;
      r.__l.size_geom = 0;
      r.__l.is_closed = false;
    }
  }

  size_t val4_array_len(size_t x) const noexcept {
    return x / 4 + (x % 4 != 0);
  }

  void setLong(Val4& v, int ix, long val) noexcept {
    v.low[ix] = (uint32_t)val;
    v.hi[ix] = (uint8_t)(val >> 32);
  }

  uint64_t getLong(const Val4& v, uint8_t ix) const noexcept {
    return long((((uint64_t)v.hi[ix]) << 32) + (uint64_t)v.low[ix]);
  }

  struct __long {
    uint16_t size;
    uint8_t refcnt;        // used by SharedDataPointer to track number of references, don't mess with it!
    bool is_closed : 1;    // true: last node omitted in struct, as it's identical to the first node
    bool is_long : 1;
    uint16_t size_geom;
    Val4* nds = nullptr;
    Quad_Coord* geom = nullptr;
  };

  struct __short {
    uint16_t size;
    uint8_t refcnt;
    bool is_closed : 1;
    bool is_long : 1;
    Val4 nds;
  };

  struct __raw {
    uint64_t dummy[3];
  };

  struct __rep {
    union {
      __long __l;
      __short __s;
      __raw __r;
    };
  };

  __rep r{};

public:
  auto nds();
  auto nds() const;

  auto geometry();
  auto geometry() const;

  friend nds_type<Way_Skeleton_Dense_Data>;
  friend nds_type<const Way_Skeleton_Dense_Data>;

  friend geometry_type<Way_Skeleton_Dense_Data>;
  friend geometry_type<const Way_Skeleton_Dense_Data>;

};


static_assert(sizeof(Way_Skeleton_Dense_Data) == 24, "Way_Skeleton_Dense_Data has wrong size");

template <typename T>
class nds_iterator
{
public:
  typedef Node::Id_Type value_type;
  typedef value_type reference;
  typedef value_type* pointer;
  typedef std::forward_iterator_tag iterator_category;

  nds_iterator(T * o, int p) : outer(o), pos(p) {}

  struct nds_iterator_helper {

  public:
    nds_iterator_helper(value_type value) : value(value) {}

    value_type const * operator->() const
    { return &value;  }

  private:
    const value_type value;
  };

  nds_iterator& operator++() noexcept
  {
    ++pos;
    return *this;
  }

  reference operator*() const noexcept
  { return outer->operator[](pos); }

  nds_iterator_helper operator->() const noexcept
  { return nds_iterator_helper(outer->operator[](pos)); }

private:
  T * outer = nullptr;
  int pos = 0;

  template <typename U1, typename U2>
  friend bool operator==(const nds_iterator<U1>&, const nds_iterator<U2>&) noexcept;

  template <typename U1, typename U2>
  friend bool operator!=(const nds_iterator<U1>&, const nds_iterator<U2>&) noexcept;
};

template <typename T1, typename T2>
  inline bool operator==(const nds_iterator<T1>& __lhs, const nds_iterator<T2>& __rhs) noexcept
  { return __lhs.pos == __rhs.pos; }

template <typename T1, typename T2>
  inline bool operator!=(const nds_iterator<T1>& __lhs, const nds_iterator<T2>& __rhs) noexcept
  { return !(operator==(__lhs, __rhs)); }


template <typename T>
class nds_type {
public:
  using value_type = Node::Id_Type;
  using vector = std::vector< value_type >;
  using iterator = nds_iterator< nds_type< T > >;
  using const_iterator = const nds_iterator< const nds_type< T > >;

  uint size() const noexcept{ return outer->size_nds(); }
  bool empty() const noexcept { return outer->empty_nds(); }
  value_type front() const noexcept { return outer->get_nd(0); }
  value_type back() const noexcept { return outer->get_nd(size() - 1); }
  bool is_closed() const noexcept { return outer->is_closed(); }
  value_type operator[](size_t __n) const noexcept { return outer->get_nd(__n); }
  void clean() { outer->clean(); }
  inline explicit operator std::vector< Node::Id_Type >() const { return outer->get_nds(); }

  inline void reserve(int new_cap ) { }   // TODO
  inline void push_back(const value_type& __x) { }         // TODO
  inline void swap(vector& __x) noexcept { }   // TODO

  inline iterator begin() noexcept { return nds_iterator< nds_type< T > >(this, 0); }
  inline iterator end() noexcept { return nds_iterator< nds_type< T > >(this, size()); }

  inline const_iterator cbegin() const noexcept { return nds_iterator< const nds_type< T > >(this, 0); }
  inline const_iterator cend() const noexcept { return nds_iterator< const nds_type< T > >(this, size()); }

  inline void operator=(const vector& __x) {  /* v = __x; return v; */ }      // TODO

  template <typename T1>
  inline bool operator==(const nds_type<T1>& rhs ) const noexcept {
    if (size() != rhs.size())
      return false;
    for (uint i = 0; i < size(); i++) {
      if (!(operator[](i) == rhs[i]))
        return false;
    }
    return true;
  }

private:
  nds_type(T* o) : outer(o) {}

  T* outer = nullptr;
  friend class Way_Skeleton_Dense_Data;
};


template <typename T>
class geometry_type {
public:
  using value_type = Quad_Coord;
  using pointer_type = value_type *;
  using vector = std::vector< value_type >;

  uint size() const noexcept{ return outer->size_geom(); }
  bool empty() const noexcept { return outer->empty_geom(); }
  Quad_Coord& front() const noexcept { return outer->r.__l.geom[0]; }
  Quad_Coord& back() const noexcept { return outer->r.__l.geom[size() - 1]; }
  Quad_Coord& operator[](size_t __n) const noexcept { return outer->r.__l.geom[__n]; }

  inline void reserve(int new_cap ) {  }   // TODO
  inline void push_back(const value_type& __x) { }         // TODO

  inline pointer_type begin() noexcept { return outer->r.__l.geom; }
  inline pointer_type end() noexcept { return outer->r.__l.geom + size(); }

  inline pointer_type cbegin() const noexcept { return outer->r.__l.geom; }
  inline pointer_type cend() const noexcept { return outer->r.__l.geom + size(); }

  //  template<typename... _Args>
  //  inline void emplace_back(_Args&&... __args) { v.emplace_back(std::forward<_Args>(__args)...); }
  //
  //  template<typename _InputIterator >
  //  inline vector::iterator  insert(vector::const_iterator __position, _InputIterator __first, _InputIterator __last) { return v.insert(__position, __first, __last); }
  //

  inline void swap(vector& __x) noexcept { /* v.swap(__x); */ } // TODO

  inline void clear() noexcept { /* v.clear(); */ }            // TODO

  inline void  operator=(const vector& __x) { /* return v.operator=(__x); */ }
  inline explicit operator std::vector< value_type >() const {

    std::vector< value_type > res;
    res.reserve(size());
    for (uint i = 0; i < size(); i++) {
      res[i] = operator[](i);
    }
    return res;
  }

  template <typename T1>
  inline bool operator==(const geometry_type<T1>& rhs ) const noexcept {
    if (size() != rhs.size())
      return false;
    for (uint i = 0; i < size(); i++) {
      if (!(operator[](i) == rhs[i]))
        return false;
    }
    return true;
  }

private:
  geometry_type(T* o) : outer(o) {}

  T* outer = nullptr;
  friend class Way_Skeleton_Dense_Data;
};



template <typename T1, typename T2>
inline bool operator==(const geometry_type<T1>& __x, const geometry_type<T2>& __y) { return true; }    // TODO

inline auto Way_Skeleton_Dense_Data::nds() { return nds_type<Way_Skeleton_Dense_Data>(this); }
inline auto Way_Skeleton_Dense_Data::nds() const { return nds_type<const Way_Skeleton_Dense_Data>(this); }

inline auto Way_Skeleton_Dense_Data::geometry() { return geometry_type<Way_Skeleton_Dense_Data>(this); }
inline auto Way_Skeleton_Dense_Data::geometry() const { return geometry_type<const Way_Skeleton_Dense_Data>(this); }

}

#endif

// ------------------------------------------------------------------------------------------

// https://stackoverflow.com/questions/10824569/overload-operator-on-return-type
// https://www.etlcpp.com/blog/2018/01/23/c-wrapper-for-legacy-c-arrays/
// https://stackoverflow.com/questions/44098116/a-c11-wrapper-class-on-top-of-std-vector

// ------------------------------------------------------------------------------------------

#ifdef HAVE_DENSE_WAYS

 using Way_Skeleton_Data_Type = osm3s_experimental::Way_Skeleton_Dense_Data;

#else

class
#ifdef HAVE_WORD_ALIGNMENT
__attribute__ ((packed, aligned(4)))
#endif
Way_Skeleton_Data final : public SharedData
{
//  using Way_Nodes = experimental::Way_Nodes_Proxy;
//  using Way_Geometry = experimental::Way_Geometry_Proxy;

   using Way_Nodes = std::vector< Node::Id_Type >;
   using Way_Geometry = std::vector< Quad_Coord >;

public:
  Way_Skeleton_Data() = default;

  Way_Skeleton_Data(const Way_Skeleton_Data &other) = default;

  ~Way_Skeleton_Data() = default;

  Way_Skeleton_Data(const std::vector<Node::Id_Type>& nds, const std::vector<Quad_Coord>& geometry) : nds_(nds), geometry_(geometry) {}
  Way_Skeleton_Data(std::vector<Node::Id_Type>&& nds, std::vector<Quad_Coord>&& geometry) : nds_(std::move(nds)), geometry_(std::move(geometry)) {}

  Way_Nodes& nds() { return this->nds_; }
  Way_Geometry& geometry() { return this->geometry_; }

  const Way_Nodes& nds() const { return this->nds_; }
  const Way_Geometry& geometry() const { return this->geometry_; }

private:
  Way_Nodes nds_;
  Way_Geometry geometry_;
};

using Way_Skeleton_Data_Type = Way_Skeleton_Data;

#endif


template <class T, class Object>
struct Way_Skeleton_Handle_Methods;


struct Way_Delta;

struct
#ifdef HAVE_WORD_ALIGNMENT
__attribute__ ((packed, aligned(4)))
#endif
Way_Skeleton
{
  typedef Way::Id_Type Id_Type;
  typedef Way_Delta Delta;

  Id_Type id;

  Way_Skeleton() : id(0u), d(new Way_Skeleton_Data_Type) { }

  Way_Skeleton(Way::Id_Type id_) : id(id_),  d(new Way_Skeleton_Data_Type) { }

  Way_Skeleton(const void* data) : id(unalignedLoad<Id_Type>(data)),  d(nullptr)
  {
    std::vector< Node::Id_Type > nds;
    nds.reserve(*((uint16*)data + 2));

    auto* start_ptr = (uint16*) decompress_ids(nds, *((uint16*)data + 2), *((uint16*)data + 4), ((uint8*)data + 10));

    const auto geometry_count = unalignedLoad<uint16>((uint16*)data + 3);

    std::vector< Quad_Coord > geom;

    geom.reserve(geometry_count);
    for (int i(0); i < geometry_count; ++i)
      geom.push_back(Quad_Coord(unalignedLoad<uint32>(start_ptr + 4*i), unalignedLoad<uint32>(start_ptr + 4*i + 2)));

    d = new Way_Skeleton_Data_Type(nds, geom);

//    const auto p1 = static_cast<std::vector< Node::Id_Type >>(d->nds());
//    std::cerr << p1 << "\n";
  }

  Way_Skeleton(const Way& way)
      : id(way.id) {

    d = new Way_Skeleton_Data_Type(way.nds, way.geometry);
  }

  Way_Skeleton(Id_Type id_, std::vector< Node::Id_Type >&& nds_)
      : id(id_) {

    d = new Way_Skeleton_Data_Type(std::move(nds_), {});
  }


  Way_Skeleton(Id_Type id_, const std::vector< Node::Id_Type >& nds_, const std::vector< Quad_Coord >& geometry_)
      : id(id_) {

    d = new Way_Skeleton_Data_Type(nds_, geometry_);
  }

#ifdef HAVE_DENSE_WAYS
  auto nds() { return d->nds(); }
  auto nds() const { return d->nds(); }

  auto geometry() { return d->geometry(); }
  auto geometry() const { return d->geometry(); }
#else
  const auto & nds() const { return d->nds(); }
  auto & nds() { return d->nds(); }

  const auto & geometry() const { return d->geometry(); }
  auto & geometry() { return d->geometry(); }
#endif


  uint32 size_of() const
  {
    uint32 compress_size = calculate_ids_compressed_size(d->nds());
    return 8 + 2 + compress_size + 8*d->geometry().size();
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
    unalignedStore(((uint16*)data + 2), (uint16) d->nds().size());
    unalignedStore(((uint16*)data + 3), (uint16) d->geometry().size());

    auto* start_ptr = (uint16*) compress_ids(d->nds(), (uint8*)data + 10);
    auto nds_compressed_size = (uint16) ((uint8*)start_ptr - ((uint8*)data + 10));
    unalignedStore(((uint16*)data + 4), (uint16) nds_compressed_size);

    for (uint i(0); i < d->geometry().size(); ++i)
    {
      unalignedStore(start_ptr + 4*i,     d->geometry()[i].ll_upper);
      unalignedStore(start_ptr + 4*i + 2, d->geometry()[i].ll_lower);
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
  SharedDataPointer<Way_Skeleton_Data_Type> d;
};

#ifdef HAVE_WORD_ALIGNMENT
static_assert(sizeof(Way_Skeleton) == 12, "Way_Skeleton has wrong size");
#endif


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
 #ifndef HAVE_DENSE_WAYS
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
#endif
    return result;
  }

  Way_Skeleton expand_fast(Way_Skeleton& reference) const
  {
    Way_Skeleton result(id);
#ifndef HAVE_DENSE_WAYS
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
#endif
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
