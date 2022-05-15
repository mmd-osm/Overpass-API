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

#ifndef DE__OSM3S___OVERPASS_API__CORE__DATATYPES_H
#define DE__OSM3S___OVERPASS_API__CORE__DATATYPES_H

#include <cassert>
#include <cstring>
#include <iomanip>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "basic_types.h"
#include "geometry.h"
#include "type_node.h"
#include "type_way.h"
#include "type_relation.h"
#include "type_tags.h"
#include "type_area.h"


struct String_Object
{
  typedef uint32 Id_Type;

  String_Object() = default;

  String_Object(std::string s) : value(std::move(s)) {}

  String_Object(void* data)
  {
    value = std::string(((int8*)data + 2), unalignedLoad<uint16>(data));
  }

  uint32 size_of() const
  {
    return value.length() + 2;
  }

  static uint32 size_of(void* data)
  {
    return unalignedLoad<uint16>(data) + 2;
  }

  void to_data(void* data) const
  {
    unalignedStore(data, (uint16) value.length());
    memcpy(((uint8*)data + 2), value.data(), value.length());
  }

  bool operator<(const String_Object& index) const
  {
    return this->value < index.value;
  }

  bool operator==(const String_Object& index) const
  {
    return this->value == index.value;
  }

  std::string val() const
  {
    return value;
  }

  protected:
    std::string value;
};


template< typename First, typename Second >
struct Pair_Comparator_By_Id {
  bool operator() (const std::pair< First, Second >& a, const std::pair< First, Second >& b)
  {
    return (a.first < b.first);
  }
};


template< typename First, typename Second >
struct Pair_Equal_Id {
  bool operator() (const std::pair< First, Second >& a, const std::pair< First, Second >& b)
  {
    return (a.first == b.first);
  }
};


template < class T >
const T* binary_search_for_id(const std::vector< T >& vect, typename T::Id_Type id)
{
  uint32 lower(0);
  uint32 upper(vect.size());

  while (upper > lower)
  {
    uint32 pos((upper + lower)/2);
    if (id < vect[pos].id)
      upper = pos;
    else if (vect[pos].id == id)
      return &(vect[pos]);
    else
      lower = pos + 1;
  }
  return nullptr;
}


template < class TObject >
TObject* binary_ptr_search_for_id(const std::vector< TObject* >& vect, typename TObject::Id_Type id)
{
  uint32 lower(0);
  uint32 upper(vect.size());

  while (upper > lower)
  {
    uint32 pos((upper + lower)/2);
    if (id < vect[pos]->id)
      upper = pos;
    else if (vect[pos]->id == id)
      return vect[pos];
    else
      lower = pos + 1;
  }
  return nullptr;
}


template < class Id_Type, class TObject >
const TObject* binary_pair_search(const std::vector< std::pair< Id_Type, TObject> >& vect, Id_Type id)
{
  uint32 lower(0);
  uint32 upper(vect.size());

  while (upper > lower)
  {
    uint32 pos((upper + lower)/2);
    if (id < vect[pos].first)
      upper = pos;
    else if (vect[pos].first == id)
      return &vect[pos].second;
    else
      lower = pos + 1;
  }
  return nullptr;
}


template< typename T >
struct Array
{
  Array(unsigned int size) : size_(size)
  {
    if (size > 0)
      ptr = new T[size];
  }
  ~Array() { delete[] ptr; }

  Array(const Array&) = delete;
  Array& operator=(const Array&) = delete;

  const T& operator[](unsigned int i) const { return ptr[i]; }
  T& operator[](unsigned int i) { return ptr[i]; }
  unsigned int size() const { return size_; }

private:
  T* ptr = nullptr;
  unsigned int size_;
};


template< typename Base >
struct Owning_Array
{
  Owning_Array() = default;
  ~Owning_Array() = default;

  Owning_Array(const Owning_Array&) = delete;
  Owning_Array& operator=(const Owning_Array&) = delete;

  const Base* operator[](uint i) const { return content[i].get(); }
  void push_back(Base* ptr) { content.emplace_back(ptr); }
  uint size() const { return content.size(); }
private:
  std::vector< std::unique_ptr< Base > > content;
};


struct Derived_Skeleton
{
  typedef Uint64 Id_Type;

  Derived_Skeleton(const std::string& type_name_, Id_Type id_) : type_name(type_name_), id(id_) {}

  std::string type_name;
  Id_Type id;
};


struct Derived_Structure : public Derived_Skeleton
{
  Derived_Structure(const std::string& type_name_, Id_Type id_)
      : Derived_Skeleton(type_name_, id_), geometry(nullptr) {}
  Derived_Structure(const std::string& type_name_, Id_Type id_,
      const std::vector< std::pair< std::string, std::string > >& tags_, Opaque_Geometry* geometry_)
      : Derived_Skeleton(type_name_, id_), tags(tags_), geometry(geometry_) {}

  std::vector< std::pair< std::string, std::string > > tags;

  const Opaque_Geometry* get_geometry() const { return ((geometry) ? &*geometry : nullptr); }
  void acquire_geometry(Opaque_Geometry* geometry_)
  {
    geometry.reset(geometry_);
  }

  bool operator<(const Derived_Structure& a) const
  {
    return this->id.val() < a.id.val();
  }

  bool operator==(const Derived_Structure& a) const
  {
    return this->id.val() == a.id.val();
  }

private:
  std::shared_ptr< Opaque_Geometry > geometry;
};


/**
  * A dataset that is referred in the scripts by a variable.
  */
struct Set
{
  std::map< Uint32_Index, std::vector< Node_Skeleton > > nodes;
  std::map< Uint31_Index, std::vector< Way_Skeleton > > ways;
  std::map< Uint31_Index, std::vector< Relation_Skeleton > > relations;

  std::map< Uint32_Index, std::vector< Attic< Node_Skeleton > > > attic_nodes;
  std::map< Uint31_Index, std::vector< Attic< Way_Skeleton > > > attic_ways;
  std::map< Uint31_Index, std::vector< Attic< Relation_Skeleton > > > attic_relations;

  std::map< Uint31_Index, std::vector< Area_Skeleton > > areas;
  std::map< Uint31_Index, std::vector< Derived_Structure > > deriveds;

  std::map< Uint31_Index, std::vector< Area_Block > > area_blocks;

  void swap(Set& rhs)
  {
    nodes.swap(rhs.nodes);
    ways.swap(rhs.ways);
    relations.swap(rhs.relations);
    attic_nodes.swap(rhs.attic_nodes);
    attic_ways.swap(rhs.attic_ways);
    attic_relations.swap(rhs.attic_relations);
    areas.swap(rhs.areas);
    deriveds.swap(rhs.deriveds);
    area_blocks.swap(rhs.area_blocks);
  }

  void clear()
  {
    nodes.clear();
    ways.clear();
    relations.clear();
    attic_nodes.clear();
    attic_ways.clear();
    attic_relations.clear();
    areas.clear();
    deriveds.clear();
    area_blocks.clear();
  }
};


struct Error_Output
{
  virtual void add_encoding_error(const std::string& error) = 0;
  virtual void add_parse_error(const std::string& error, int line_number) = 0;
  virtual void add_static_error(const std::string& error, int line_number) = 0;
  // void add_sanity_error(const std::string& error);

  virtual void add_encoding_remark(const std::string& error) = 0;
  virtual void add_parse_remark(const std::string& error, int line_number) = 0;
  virtual void add_static_remark(const std::string& error, int line_number) = 0;
  // void add_sanity_remark(const std::string& error);

  virtual void runtime_error(const std::string& error) = 0;
  virtual void runtime_remark(const std::string& error) = 0;

  virtual void display_statement_progress
    (uint timer, const std::string& name, int progress, int line_number,
     const std::vector< std::pair< uint, uint > >& stack) = 0;

  virtual bool display_encoding_errors() = 0;
  virtual bool display_parse_errors() = 0;
  virtual bool display_static_errors() = 0;

  virtual ~Error_Output() = default;

  static const uint QUIET = 1;
  static const uint CONCISE = 2;
  static const uint PROGRESS = 3;
  static const uint ASSISTING = 4;
  static const uint VERBOSE = 5;
};


class Area_Usage_Listener
{
  public:
    virtual ~Area_Usage_Listener() = default;
    virtual void flush() = 0;
};


class Osm_Backend_Callback
{
  public:
    virtual void update_started() = 0;
    virtual void update_ids_finished() = 0;
    virtual void update_coords_finished() = 0;
    virtual void prepare_delete_tags_finished() = 0;
    virtual void tags_local_finished() = 0;
    virtual void tags_global_finished() = 0;
    virtual void flush_roles_finished() = 0;
    virtual void update_finished() = 0;
    virtual void partial_started() = 0;
    virtual void partial_finished() = 0;

    virtual void parser_started() = 0;
    virtual void node_elapsed(Node::Id_Type id) = 0;
    virtual void nodes_finished() = 0;
    virtual void way_elapsed(Way::Id_Type id) = 0;
    virtual void ways_finished() = 0;
    virtual void relation_elapsed(Relation::Id_Type id) = 0;
    virtual void relations_finished() = 0;
    virtual void parser_succeeded() = 0;

    virtual ~Osm_Backend_Callback() = default;
};


template <class T, class Object>
struct User_Data_Handle_Methods;


struct User_Data
{
  typedef uint32 Id_Type;

  Id_Type id;
  std::string name;

  User_Data() : id(0) {}

  User_Data(const void* data)
  {
    id = unalignedLoad<uint32>(data);
    name = std::string(((int8*)data + 6), unalignedLoad<uint16>((int8*)data + 4));
  }

  uint32 size_of() const
  {
    return 6 + name.length();
  }

  static uint32 size_of(void* data)
  {
    return 6 + unalignedLoad<uint16>((int8*)data + 4);
  }

  void to_data(void* data) const
  {
    unalignedStore(data, id);
    unalignedStore((int8*)data + 4, (uint16) name.length());
    memcpy(((int8*)data + 6), name.data(), name.length());
  }

  bool operator<(const User_Data& a) const
  {
    return (id < a.id);
  }

  bool operator==(const User_Data& a) const
  {
    return (id == a.id);
  }

  template <class T, class Object>
  using Handle_Methods = User_Data_Handle_Methods<T, Object>;
};

template <typename Id_Type >
struct User_Data_Id_Functor {
  User_Data_Id_Functor() = default;

  using reference_type = User_Data;

  Id_Type operator()(const void* data) const
   {
    return unalignedLoad<uint32>(data);
   }
};

struct User_Data_Name_Functor {
  User_Data_Name_Functor() {};

  using reference_type = User_Data;

  inline std::string_view operator()(const void* data) const
   {
     auto name_len = unalignedLoad<uint16>((int8*)data + 4);
     char* name =  (int8*)data + 6;
     return std::string_view(name, name_len);
   }
};

template <class T, class Object>
struct User_Data_Handle_Methods
{
  typename Object::Id_Type inline id() const {
     return (static_cast<const T*>(this)->apply_func(User_Data_Id_Functor<typename Object::Id_Type>()));
  }

  inline std::string_view get_name() const {
    return (static_cast<const T*>(this)->apply_func(User_Data_Name_Functor()));
  }
};


struct OSM_Element_Metadata
{
  OSM_Element_Metadata() : version(0), timestamp(0), changeset(0), user_id(0) {}

  uint32 version;
  timestamp_t timestamp;
  uint32 changeset;
  uint32 user_id;
  std::string user_name;

  bool operator<(const OSM_Element_Metadata&) const { return false; }
};


template <class T, class Object>
struct Metadata_Handle_Methods;

template< typename Id_Type_ >
struct OSM_Element_Metadata_Skeleton
{
  typedef Id_Type_ Id_Type;

  Id_Type ref;
  uint32 version;
  timestamp_t timestamp;
  uint32 changeset;
  uint32 user_id;

  OSM_Element_Metadata_Skeleton() : version(0), timestamp(0), changeset(0), user_id(0) {}

  OSM_Element_Metadata_Skeleton(Id_Type ref_)
    : ref(ref_), version(0), timestamp(0), changeset(0), user_id(0) {}

  OSM_Element_Metadata_Skeleton(Id_Type ref_, const OSM_Element_Metadata& meta)
    : ref(ref_),
      version(meta.version), timestamp(meta.timestamp),
      changeset(meta.changeset), user_id(meta.user_id) {}

  OSM_Element_Metadata_Skeleton(Id_Type ref_, timestamp_t timestamp_)
    : ref(ref_), version(0), timestamp(timestamp_),
      changeset(0), user_id(0) {}

  OSM_Element_Metadata_Skeleton(const void* data)
    : ref(data)
  {
    version = unalignedLoad<uint32>((int8*)data + Id_Type::max_size_of());
    timestamp = (unalignedLoad<timestamp_t>((int8*)data + Id_Type::max_size_of() + 4));
    changeset = unalignedLoad<uint32>((int8*)data + Id_Type::max_size_of() + 8);
    user_id = unalignedLoad<uint32>((int8*)data + Id_Type::max_size_of() + 12);
  }

  uint32 size_of() const
  {
    return 16 + Id_Type::max_size_of();
  }

  static uint32 size_of(const void* data)
  {
    return 16 + Id_Type::max_size_of();
  }

  void to_data(void* data) const
  {
    ref.to_data(data);
    unalignedStore((int8*)data + Id_Type::max_size_of(), version);
    unalignedStore((int8*)data + Id_Type::max_size_of() + 4, timestamp);
    unalignedStore((int8*)data + Id_Type::max_size_of() + 8, changeset);
    unalignedStore((int8*)data + Id_Type::max_size_of() + 12, user_id);
  }

  bool operator<(const OSM_Element_Metadata_Skeleton& a) const
  {
    if (ref < a.ref)
      return true;
    else if (a.ref < ref)
      return false;
    return (timestamp < a.timestamp);
  }

  bool operator==(const OSM_Element_Metadata_Skeleton& a) const
  {
    return (ref == a.ref);
  }

  template <class T, class Object>
  using Handle_Methods = Metadata_Handle_Methods<T, Object>;
};

template <typename Id_Type >
struct Metadata_Timestamp_Functor {
  Metadata_Timestamp_Functor() = default;

  using reference_type = OSM_Element_Metadata_Skeleton<Id_Type>;

  uint32 operator()(const void* data) const
   {
     return unalignedLoad<uint32>((int8*)data + Id_Type::max_size_of() + 4);
   }
};

template <typename Id_Type >
struct Metadata_Element_Functor {
  Metadata_Element_Functor() = default;

  using reference_type = OSM_Element_Metadata_Skeleton<Id_Type>;

  OSM_Element_Metadata_Skeleton< Id_Type > operator()(const void* data)
  {
    return OSM_Element_Metadata_Skeleton< Id_Type >(data);
  }
};


template <typename Id_Type >
struct Metadata_Reference_Functor {
  Metadata_Reference_Functor() = default;

  using reference_type = OSM_Element_Metadata_Skeleton<Id_Type>;

  Id_Type operator()(const void* data) const
   {
     return Id_Type(data);
   }
};

template <typename Id_Type >
struct Metadata_Changeset_Functor {
  Metadata_Changeset_Functor() = default;

  using reference_type = OSM_Element_Metadata_Skeleton<Id_Type>;

  uint32 operator()(const void* data) const
   {
     return unalignedLoad<uint32>((int8*)data + Id_Type::max_size_of() + 8);
   }
};


template <class T, class Object>
struct Metadata_Handle_Methods
{
  timestamp_t inline get_timestamp() const {
     return (static_cast<const T*>(this)->apply_func(Metadata_Timestamp_Functor<typename Object::Id_Type>()));
  }

  OSM_Element_Metadata_Skeleton< typename Object::Id_Type > inline get_element() const {
     return (static_cast<const T*>(this)->apply_func(Metadata_Element_Functor<typename Object::Id_Type>()));
  }

  typename Object::Id_Type inline get_ref() const {
     return (static_cast<const T*>(this)->apply_func(Metadata_Reference_Functor<typename Object::Id_Type>()));
  }

  uint32 inline get_changeset() const {
     return (static_cast<const T*>(this)->apply_func(Metadata_Changeset_Functor<typename Object::Id_Type>()));
  }
};


template< class TIndex, class TObject >
const std::pair< TIndex, const TObject* >* binary_search_for_pair_id
    (const std::vector< std::pair< TIndex, const TObject* > >& vect, typename TObject::Id_Type id)
{
  uint32 lower(0);
  uint32 upper(vect.size());

  while (upper > lower)
  {
    uint32 pos((upper + lower)/2);
    if (id < vect[pos].second->id)
      upper = pos;
    else if (vect[pos].second->id == id)
      return &(vect[pos]);
    else
      lower = pos + 1;
  }
  return nullptr;
}


template <class T, class Object>
struct Change_Entry_Handle_Methods;

#ifdef USE_ORIGINAL_CHANGE_ENTRY

template< typename Id_Type_ >
struct Change_Entry
{
  typedef Id_Type_ Id_Type;

  Change_Entry() = default;

  Change_Entry(const Id_Type& elem_id_, const Uint31_Index& old_idx_, const Uint31_Index& new_idx_)
      : old_idx(old_idx_), new_idx(new_idx_), elem_id(elem_id_) {}

  Uint31_Index old_idx;
  Uint31_Index new_idx;
  Id_Type elem_id;

  Change_Entry(const void* data)
    : old_idx((uint8*)data), new_idx((uint8*)data + 4), elem_id((uint8*)data + 8) {}

  uint32 size_of() const
  {
    return elem_id.size_of() + 8;
  }

  static uint32 size_of(void* data)
  {
    return Id_Type::size_of((uint8*)data + 8) + 8;
  }

  void to_data(void* data) const
  {
    old_idx.to_data((uint8*)data);
    new_idx.to_data((uint8*)data + 4);
    elem_id.to_data((uint8*)data + 8);
  }

  bool operator<(const Change_Entry& rhs) const
  {
    if (old_idx < rhs.old_idx)
      return true;
    if (rhs.old_idx < old_idx)
      return false;
    if (new_idx < rhs.new_idx)
      return true;
    if (rhs.new_idx < new_idx)
      return false;
    return (elem_id < rhs.elem_id);
  }

  bool operator==(const Change_Entry& rhs) const
  {
    return (old_idx == rhs.old_idx && new_idx == rhs.new_idx && elem_id == rhs.elem_id);
  }

  template <class T, class Object>
  using Handle_Methods = Change_Entry_Handle_Methods<T, Object>;
};

template <typename Id_Type >
struct Change_Entry_Id_Functor {
  Change_Entry_Id_Functor() = default;

  using reference_type = Change_Entry< Id_Type >;

  Id_Type operator()(const void* data) const
   {
    return Id_Type((uint8*)data + 8);
   }
};

#else

// Changelog files are fairly large (>60GB), even when compression is enabled.
// This stripped down Change_Entry version lacks old_idx and new_idx fields,
// which aren't used anywhere. Presumably they were added for debug purposes?
// Overall 15% size reduction for changelog files. node_changelog.bin no longer
// needs compression.
//
// Relevant parts in the code:
// * changed.cc uses the element id
// * {node,way,relation}_updater only write those fields but never read them
//
// Old code can be enabled using compiler flag `-DUSE_ORIGINAL_CHANGE_ENTRY=1`

template< typename Id_Type_ >
struct Change_Entry
{
  typedef Id_Type_ Id_Type;

  Change_Entry() = default;

  Change_Entry(const Id_Type& elem_id_, const Uint31_Index& , const Uint31_Index& )
      :  elem_id(elem_id_) {}

  Id_Type elem_id;

  Change_Entry(const void* data)
    : elem_id((uint8*)data) {}

  uint32 size_of() const
  {
    return elem_id.size_of();
  }

  static uint32 size_of(void* data)
  {
    return Id_Type::size_of((uint8*)data);
  }

  void to_data(void* data) const
  {
    elem_id.to_data((uint8*)data);
  }

  bool operator<(const Change_Entry& rhs) const
  {
    return (elem_id < rhs.elem_id);
  }

  bool operator==(const Change_Entry& rhs) const
  {
    return (elem_id == rhs.elem_id);
  }

  template <class T, class Object>
  using Handle_Methods = Change_Entry_Handle_Methods<T, Object>;
};

template <typename Id_Type >
struct Change_Entry_Id_Functor {
  Change_Entry_Id_Functor() = default;

  using reference_type = Change_Entry< Id_Type >;

  Id_Type operator()(const void* data) const
   {
    return Id_Type((uint8*)data);
   }
};

#endif


template <class T, class Object>
struct Change_Entry_Handle_Methods
{
  typename Object::Id_Type inline id() const {
     return (static_cast<const T*>(this)->apply_func(Change_Entry_Id_Functor<typename Object::Id_Type>()));
  }
};

// Space efficient storage of a group of Change_Entries

template <class T, class Object>
struct Change_Package_Handle_Methods;

struct Change_Package
{
  /*
   * On disk structure
   *
   * Byte offset   Length     Description
   * ----------------------------------------------
   * 0             2 Bytes    Number of elements in nds
   * 2             2 Bytes    Size of compressed nds (in bytes)
   * 4             ...        Compressed nds
   *
   */

  std::vector< Node::Id_Type > nds;

  Change_Package() = default;

  Change_Package(std::vector< Node::Id_Type> && ids) : nds(std::move(ids)) {}

  Change_Package(const std::vector< Node::Id_Type> & ids)  : nds(ids) {}

  Change_Package(std::vector< Change_Entry< Node_Skeleton::Id_Type > >::const_iterator it,
      std::vector< Change_Entry< Node_Skeleton::Id_Type > >::const_iterator end)  {

    while (it != end) {
      nds.emplace_back(it->elem_id);
      ++it;
    }
  }

  Change_Package(const void* data) {

    auto elems = unalignedLoad<uint16>((uint16*)data + 0);
    auto bytes = unalignedLoad<uint16>((uint16*)data + 1);

    nds.reserve(elems);

    decompress_ids(nds, elems, bytes, ((uint8*)data + 4));
  }


  uint32 size_of() const
  {
    uint32 compress_size = ::calculate_ids_compressed_size(nds);
    return 2 + 2 + compress_size;
  }

  static uint32 size_of(const void* data)
  {
    auto bytes = unalignedLoad<uint16>((uint16*)data + 1);
    return 2 + 2 + bytes;  // nds_compressed_size (in bytes)
  }

  void to_data(void* data) const
  {
    unalignedStore(((uint16*)data + 0), (uint16) nds.size());

    auto* end_ptr = (uint16*) compress_ids(nds, (uint8*)data + 4);
    auto nds_compressed_size = (uint16) ((uint8*)end_ptr - ((uint8*)data + 4));
    unalignedStore(((uint16*)data + 1), (uint16) nds_compressed_size);
  }

  bool operator<(const Change_Package& a) const noexcept
  {
    return this->nds < a.nds;
  }

  bool operator==(const Change_Package& a) const noexcept
  {
    return this->nds == a.nds;
  }

  static std::set<Change_Package> build_packages(const std::vector< Change_Entry< Node_Skeleton::Id_Type > > & ce, int chunkSize)
  {
    std::set< Change_Package > result;

    auto start = ce.begin();
    auto end = ce.end();

    while (start != end) {
      auto next = std::distance(start, end) >= chunkSize ? start + chunkSize : end;
      result.insert(Change_Package(start, next));
      start = next;
    }
    return result;
  }

  template <class T, class Object>
  using Handle_Methods = Change_Package_Handle_Methods<T, Object>;
};

template <typename Object, typename Id_Type, typename Functor>
struct Change_Package_Process_Ids_Functor {
  Change_Package_Process_Ids_Functor(Functor& f_) :  f(f_) {};

  using reference_type = Object;

  inline void operator()(const void* data) const
  {
    auto elems = unalignedLoad<uint16>((uint16*)data + 0);
    auto bytes = unalignedLoad<uint16>((uint16*)data + 1);

    decompress_ids<Id_Type>(elems, bytes, ((uint8*)data + 4), f);
  }

private:
  Functor& f;
};

template <class T, class Object>
struct Change_Package_Handle_Methods
{
  template <typename Id_Type, typename Functor>
  void inline process_ids(Functor& f) const {
    static_cast<const T*>(this)->apply_func(Change_Package_Process_Ids_Functor<Object, Id_Type, Functor>(f));
  }
};


namespace {

// source: https://github.com/osmcode/libosmium/blob/master/include/osmium/osm/timestamp.hpp


void add_2digit_int_to_string(int value, std::string& out)  {
    assert(value >= 0 && value <= 99);
    if (value > 9) {
        const int dec = value / 10;
        out += static_cast<char>('0' + dec);
        value -= dec * 10;
    } else {
        out += '0';
    }
    out += static_cast<char>('0' + value);
}

void add_4digit_int_to_string(int value, std::string& out)  {
    assert(value >= 0 && value <= 9999);

    const int dec1 = value / 1000;
    out += static_cast<char>('0' + dec1);
    value -= dec1 * 1000;

    const int dec2 = value / 100;
    out += static_cast<char>('0' + dec2);
    value -= dec2 * 100;

    const int dec3 = value / 10;
    out += static_cast<char>('0' + dec3);
    value -= dec3 * 10;

    out += static_cast<char>('0' + value);
}

}

struct Timestamp;

struct Timestamp_64
{
  Timestamp_64() : timestamp(0) {}

  Timestamp_64(uint64 timestamp_) : timestamp(timestamp_) {}

  uint64 timestamp;

  Timestamp_64(const void* data) {

    timestamp = (uint64) (unalignedLoad<uint32>(data));
    timestamp |= (uint64)(*(uint8*)((uint8*)data+4)) << 32;
  }

  Timestamp_64(int year, int month, int day, int hour, int minute, int second)
    : timestamp(0)
  {
    timestamp |= (uint64(year & 0x3fff)<<26); //year
    timestamp |= ((month & 0xf)<<22); //month
    timestamp |= ((day & 0x1f)<<17); //day
    timestamp |= ((hour & 0x1f)<<12); //hour
    timestamp |= ((minute & 0x3f)<<6); //minute
    timestamp |= (second & 0x3f); //second
  }

  Timestamp_64(const std::string& input) : timestamp(0)
  {
    if (input.size() < 19
        || !isdigit(input[0]) || !isdigit(input[1])
        || !isdigit(input[2]) || !isdigit(input[3])
        || !isdigit(input[5]) || !isdigit(input[6])
        || !isdigit(input[8]) || !isdigit(input[9])
        || !isdigit(input[11]) || !isdigit(input[12])
        || !isdigit(input[14]) || !isdigit(input[15])
        || !isdigit(input[17]) || !isdigit(input[18]))
      return;

    timestamp |= (uint64(four_digits(&input[0]) & 0x3fff)<<26); //year
    timestamp |= ((two_digits(&input[5]) & 0xf)<<22); //month
    timestamp |= ((two_digits(&input[8]) & 0x1f)<<17); //day
    timestamp |= ((two_digits(&input[11]) & 0x1f)<<12); //hour
    timestamp |= ((two_digits(&input[14]) & 0x3f)<<6); //minute
    timestamp |= (two_digits(&input[17]) & 0x3f); //second
  }

  Timestamp_64(const Timestamp& ts);

  static int two_digits(const char* input) { return (input[0] - '0')*10 + (input[1] - '0'); }
  static int four_digits(const char* input)
  { return (input[0] - '0')*1000 + (input[1] - '0')*100 + (input[2] - '0')*10 + (input[3] - '0'); }

  static int year(uint64 timestamp) { return ((timestamp>>26) & 0x3fff); }
  static int month(uint64 timestamp) { return ((timestamp>>22) & 0xf); }
  static int day(uint64 timestamp) { return ((timestamp>>17) & 0x1f); }
  static int hour(uint64 timestamp) { return ((timestamp>>12) & 0x1f); }
  static int minute(uint64 timestamp) { return ((timestamp>>6) & 0x3f); }
  static int second(uint64 timestamp) { return (timestamp & 0x3f); }

  int year() const { return year(timestamp); }
  int month() const { return month(timestamp); }
  int day() const { return day(timestamp); }
  int hour() const { return hour(timestamp); }
  int minute() const { return minute(timestamp); }
  int second() const { return second(timestamp); }

  std::string str() const
  {
    if (timestamp == std::numeric_limits< unsigned long long >::max())
      return "NOW";

    std::string s;
    s.reserve(20);

    add_4digit_int_to_string(year(), s);
    s += '-';
    add_2digit_int_to_string(month(), s);
    s += '-';
    add_2digit_int_to_string(day(), s);
    s += 'T';
    add_2digit_int_to_string(hour(), s);
    s += ':';
    add_2digit_int_to_string(minute(), s);
    s += ':';
    add_2digit_int_to_string(second(), s);
    s += 'Z';

    return s;
  }

  uint32 size_of() const
  {
    return 5;
  }

  static uint32 size_of(void* data)
  {
    return 5;
  }

  void to_data(void* data) const
  {
    void* pos = (uint8*)data;
    unalignedStore(pos, (uint32)(timestamp & 0xffffffffull));
    *(uint8*)((uint8*)pos+4) = ((timestamp & 0xff00000000ull)>>32);
  }

  bool operator<(const Timestamp_64& rhs) const
  {
    return (timestamp < rhs.timestamp);
  }

  bool operator==(const Timestamp_64& rhs) const
  {
    return (timestamp == rhs.timestamp);
  }

  static uint32 max_size_of()
  {
    throw Unsupported_Error("static uint32 Timestamp::max_size_of()");
    return 0;
  }

  friend std::ostream & operator<<(std::ostream &os, const Timestamp_64& t);
};

inline std::ostream & operator<<(std::ostream &os, const Timestamp_64& p)
{
    return os << "[ " << p.str() << " ]";
}



struct Timestamp
{
  Timestamp() : timestamp(0) {}

  Timestamp(timestamp_t timestamp_) : timestamp(timestamp_) {}

  static const uint32 YEAR_OFFSET = 2000;

  timestamp_t timestamp;

  Timestamp(const void* data) {

    timestamp = unalignedLoad<uint32>(data);
  }

  Timestamp(int year, int month, int day, int hour, int minute, int second)
    : timestamp(0)
  {
    timestamp |= (((year - YEAR_OFFSET) & 0x3f)<<26); //year
    timestamp |= ((month & 0xf)<<22); //month
    timestamp |= ((day & 0x1f)<<17); //day
    timestamp |= ((hour & 0x1f)<<12); //hour
    timestamp |= ((minute & 0x3f)<<6); //minute
    timestamp |= (second & 0x3f); //second
  }

  Timestamp(const std::string& input) : timestamp(0)
  {
    if (input.size() < 19
        || !isdigit(input[0]) || !isdigit(input[1])
        || !isdigit(input[2]) || !isdigit(input[3])
        || !isdigit(input[5]) || !isdigit(input[6])
        || !isdigit(input[8]) || !isdigit(input[9])
        || !isdigit(input[11]) || !isdigit(input[12])
        || !isdigit(input[14]) || !isdigit(input[15])
        || !isdigit(input[17]) || !isdigit(input[18]))
      return;

    timestamp |= ((four_digits(&input[0]) - YEAR_OFFSET) & 0x3f)<<26; //year
    timestamp |= ((two_digits(&input[5]) & 0xf)<<22); //month
    timestamp |= ((two_digits(&input[8]) & 0x1f)<<17); //day
    timestamp |= ((two_digits(&input[11]) & 0x1f)<<12); //hour
    timestamp |= ((two_digits(&input[14]) & 0x3f)<<6); //minute
    timestamp |= (two_digits(&input[17]) & 0x3f); //second
  }

  Timestamp(const Timestamp_64& ts) :
     Timestamp(ts.year(), ts.month(), ts.day(), ts.hour(), ts.minute(), ts.second()) {}

  static int two_digits(const char* input) { return (input[0] - '0')*10 + (input[1] - '0'); }
  static int four_digits(const char* input)
  { return (input[0] - '0')*1000 + (input[1] - '0')*100 + (input[2] - '0')*10 + (input[3] - '0'); }

  static int year(uint32 timestamp) { return YEAR_OFFSET + ((timestamp>>26) & 0x3f); }
  static int month(uint32 timestamp) { return ((timestamp>>22) & 0xf); }
  static int day(uint32 timestamp) { return ((timestamp>>17) & 0x1f); }
  static int hour(uint32 timestamp) { return ((timestamp>>12) & 0x1f); }
  static int minute(uint32 timestamp) { return ((timestamp>>6) & 0x3f); }
  static int second(uint32 timestamp) { return (timestamp & 0x3f); }

  int year() const { return year(timestamp); }
  int month() const { return month(timestamp); }
  int day() const { return day(timestamp); }
  int hour() const { return hour(timestamp); }
  int minute() const { return minute(timestamp); }
  int second() const { return second(timestamp); }

  std::string str() const
  {
    if (timestamp == std::numeric_limits< uint32 >::max())
      return "NOW";

    std::string s;
    s.reserve(20);

    add_4digit_int_to_string(year(), s);
    s += '-';
    add_2digit_int_to_string(month(), s);
    s += '-';
    add_2digit_int_to_string(day(), s);
    s += 'T';
    add_2digit_int_to_string(hour(), s);
    s += ':';
    add_2digit_int_to_string(minute(), s);
    s += ':';
    add_2digit_int_to_string(second(), s);
    s += 'Z';

    return s;
  }

  uint32 size_of() const
  {
    return 4;
  }

  static uint32 size_of(void* data)
  {
    return 4;
  }

  void to_data(void* data) const
  {
    void* pos = (uint8*)data;
    unalignedStore(pos, timestamp);
  }

  bool operator<(const Timestamp& rhs) const
  {
    return (timestamp < rhs.timestamp);
  }

  bool operator==(const Timestamp& rhs) const
  {
    return (timestamp == rhs.timestamp);
  }

  static uint32 max_size_of()
  {
    throw Unsupported_Error("static uint32 Timestamp::max_size_of()");
    return 0;
  }

  friend std::ostream & operator<<(std::ostream &os, const Timestamp& t);
};

inline std::ostream & operator<<(std::ostream &os, const Timestamp& p)
{
    return os << "[ " << p.str() << " ]";
}


inline Timestamp_64::Timestamp_64(const Timestamp& ts) :
         Timestamp_64(ts.year(), ts.month(), ts.day(), ts.hour(), ts.minute(), ts.second()) {}



typedef enum { only_data, keep_meta, keep_attic } meta_modes;


template< typename Object >
std::string name_of_type() = delete;

template< > inline std::string name_of_type< Node_Skeleton >() { return "Node"; }
template< > inline std::string name_of_type< Way_Skeleton >() { return "Way"; }
template< > inline std::string name_of_type< Relation_Skeleton >() { return "Relation"; }
template< > inline std::string name_of_type< Area_Skeleton >() { return "Area"; }

using kv_pairs = std::vector< std::pair< std::string, std::string > >;

struct Tags_By_Id_Cache
{
public:
  template <typename T>
  std::map< typename T::Id_Type, kv_pairs> & get() = delete;

private:
  std::map< Node_Skeleton::Id_Type, kv_pairs > t_by_id_node;
  std::map< Way_Skeleton::Id_Type, kv_pairs > t_by_id_way;
  std::map< Relation_Skeleton::Id_Type, kv_pairs > t_by_id_relation;
  std::map< Area_Skeleton::Id_Type, kv_pairs > t_by_id_area;
};

template<> inline std::map< Node_Skeleton::Id_Type, kv_pairs > &     Tags_By_Id_Cache::get< Node_Skeleton >()     { return t_by_id_node; }
template<> inline std::map< Way_Skeleton::Id_Type, kv_pairs > &      Tags_By_Id_Cache::get< Way_Skeleton >()      { return t_by_id_way; }
template<> inline std::map< Relation_Skeleton::Id_Type, kv_pairs > & Tags_By_Id_Cache::get< Relation_Skeleton >() { return t_by_id_relation; }
template<> inline std::map< Area_Skeleton::Id_Type, kv_pairs > &     Tags_By_Id_Cache::get< Area_Skeleton >()     { return t_by_id_area; }

#endif
