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

#ifndef DE__OSM3S___OVERPASS_API__CORE__TYPE_TAGS_H
#define DE__OSM3S___OVERPASS_API__CORE__TYPE_TAGS_H

#include <algorithm>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../../template_db/ranges.h"
#include "basic_types.h"


struct Unsupported_Error
{
  Unsupported_Error(const std::string& method_name_) : method_name(method_name_) {}
  std::string method_name;
};


template< class Id_Type >
struct Tag_Entry
{
  uint32 index{};
  std::string key;
  std::string value;
  std::vector< Id_Type > ids;
};

template <class T, class Object>
struct Tag_Index_Local_Handle_Methods;

struct Tag_Index_Local
{
  uint32 index{};
  std::string key;
  std::string value;

  Tag_Index_Local() = default;

  template< typename Id_Type >
  Tag_Index_Local(const Tag_Entry< Id_Type >& entry)
      : index(entry.index), key(entry.key), value(entry.value) {}

  Tag_Index_Local(Uint31_Index index_, std::string key_, std::string value_)
      : index(index_.val() & 0x7fffff00), key(std::move(key_)), value(std::move(value_)) {}

  Tag_Index_Local(const void* data) :
       index((unalignedLoad<uint32>((uint32*)data + 1))<<8),
       key(((int8*)data + 7), unalignedLoad<uint16>(data)),
       value(((int8*)data + 7 + key.length()), unalignedLoad<uint16>((uint16*)data + 1))  {}

  uint32 size_of() const noexcept
  {
    return 7 + key.length() + value.length();
  }

  static uint32 size_of(const void* data) noexcept
  {
    return unalignedLoad<uint16>(data) + unalignedLoad<uint16>((uint16*)data + 1) + 7;
  }

  void to_data(void* data) const noexcept
  {
    unalignedStore(data, (uint16) key.length());
    unalignedStore(((uint16*)data + 1), (uint16) value.length());
    unalignedStore(((uint32*)data + 1), (uint32) (index>>8));
    memcpy(((uint8*)data + 7), key.data(), key.length());
    memcpy(((uint8*)data + 7 + key.length()), value.data(),
	   value.length());
  }

  bool operator<(const Tag_Index_Local& a) const noexcept
  {
    if ((index & 0x7fffffff) != (a.index & 0x7fffffff))
      return ((index & 0x7fffffff) < (a.index & 0x7fffffff));
    if (index != a.index)
      return (index < a.index);
    if (key != a.key)
      return (key < a.key);
    return (value < a.value);
  }

  bool operator==(const Tag_Index_Local& a) const noexcept
  {
    if (index != a.index)
      return false;
    if (key != a.key)
      return false;
    return (value == a.value);
  }

  static uint32 max_size_of()
  {
    throw Unsupported_Error("static uint32 Tag_Index_Local::max_size_of()");
    return 0;
  }

  friend std::ostream & operator<<(std::ostream &os, const Tag_Index_Local& t);

  template <class T, class Object>
  using Handle_Methods = Tag_Index_Local_Handle_Methods<T, Object>;
};

inline std::ostream & operator<<(std::ostream &os, const Tag_Index_Local& p)
{
    return os << "[ " << p.index << " | " << p.key << " | " << p.value << " ]";
}


template <class T, class Object>
struct Tag_Index_Local_Handle_Methods
{
  inline uint32 get_index() const {
     return (static_cast<const T*>(this)->apply_func(Tag_Index_Local_Index_Functor()));
  }

  inline std::string_view get_key() const {
    return (static_cast<const T*>(this)->apply_func(Tag_Index_Local_Get_Key_Functor()));
  }

  inline std::string_view get_value() const {
    return (static_cast<const T*>(this)->apply_func(Tag_Index_Local_Get_Value_Functor()));
  }

  inline bool operator<(const Tag_Index_Local& tig) const {
    return (static_cast<const T*>(this)->apply_func(Tag_Index_Local_Operator_Lower_Functor(tig)));
  }

private:

  struct Tag_Index_Local_Index_Functor {
    Tag_Index_Local_Index_Functor() = default;

    using reference_type = Tag_Index_Local;

    inline uint32 operator()(const void* data) const
     {
       return unalignedLoad<uint32>((uint32*)data + 1)<<8;
     }
  };

  struct Tag_Index_Local_Get_Key_Functor {
    Tag_Index_Local_Get_Key_Functor() = default;

    using reference_type = Tag_Index_Local;

    inline std::string_view operator()(const void* data) const
     {
       auto key_len = unalignedLoad<uint16>(data);
       char* k =  ((int8*)data + 7);

       return std::string_view(k, key_len);
     }
  };

  struct Tag_Index_Local_Get_Value_Functor {
    Tag_Index_Local_Get_Value_Functor() = default;

    using reference_type = Tag_Index_Local;

    inline std::string_view operator()(const void* data) const
     {
       auto key_len = unalignedLoad<uint16>(data);
       char* v = (int8*)data + 7 + key_len;
       auto value_len = unalignedLoad<uint16>((uint16*)data + 1);

       return std::string_view(v, value_len);
     }
  };

  struct Tag_Index_Local_Operator_Lower_Functor {
    Tag_Index_Local_Operator_Lower_Functor(const Tag_Index_Local& til_) : a(til_) { }

    using reference_type = Tag_Index_Local;

    bool operator()(const void* data)
    {
      auto index = Tag_Index_Local_Index_Functor()(data);

      if ((index & 0x7fffffff) != (a.index & 0x7fffffff))
        return ((index & 0x7fffffff) < (a.index & 0x7fffffff));
      if (index != a.index)
        return (index < a.index);

      auto key = Tag_Index_Local_Get_Key_Functor()(data);

      if (key != a.key)
        return (key < a.key);

      auto value = Tag_Index_Local_Get_Value_Functor()(data);

      return (value < a.value);
    }

  private:
    const Tag_Index_Local & a;
  };
};


// void_tag initialization in settings.cc
class void_tag {
  public:
     const static std::string void_tag_value;
     const static std::string void_tag_value_space;
};


inline const std::string& void_tag_value()
{
  return void_tag::void_tag_value;
}

inline const std::string& void_tag_value_space()
{
  return void_tag::void_tag_value_space;
}


template< class Index >
Ranges< Tag_Index_Local > formulate_range_query(const std::set< Index >& coarse_indices)
{
  std::set< std::pair< Tag_Index_Local, Tag_Index_Local > > range_set;
  for (auto it(coarse_indices.begin()); it != coarse_indices.end(); ++it)
  {
    Tag_Index_Local lower, upper;
    lower.index = it->val();
    lower.key = "";
    lower.value = "";
    upper.index = it->val() + 1;
    upper.key = "";
    upper.value = "";
    range_set.insert(std::make_pair(lower, upper));
  }
  return Ranges< Tag_Index_Local >(std::move(range_set));
}

template<  >
inline Ranges< Tag_Index_Local > formulate_range_query(const std::set< uint32 >& coarse_indices)
{
  std::set< std::pair< Tag_Index_Local, Tag_Index_Local > > range_set;
  for (auto val : coarse_indices)
  {
    Tag_Index_Local lower, upper;
    lower.index = val;
    lower.key = "";
    lower.value = "";
    upper.index = val + 1;
    upper.key = "";
    upper.value = "";
    range_set.insert(std::make_pair(lower, upper));
  }
  return Ranges< Tag_Index_Local >(std::move(range_set));
}

template< class Value >
Ranges< Tag_Index_Local > formulate_range_query(const std::map< uint32, Value >& coarse_indices)
{
  std::set< std::pair< Tag_Index_Local, Tag_Index_Local > > range_set;
  for (auto it = coarse_indices.begin(); it != coarse_indices.end(); ++it)
  {
    Tag_Index_Local lower, upper;
    lower.index = it->first;
    lower.key = "";
    lower.value = "";
    upper.index = it->first + 1;
    upper.key = "";
    upper.value = "";
    range_set.insert(std::make_pair(lower, upper));
  }
  return Ranges< Tag_Index_Local >(std::move(range_set));
}

template< class TIndex, class TObject >
void generate_ids_by_coarse
  (std::map< uint32, std::vector< typename TObject::Id_Type > >& ids_by_coarse,
   const std::map< TIndex, std::vector< TObject > >& items,
   bool skip_empty = false)
{
  for (auto it(items.begin()); it != items.end(); ++it)
  {
    if (skip_empty && it->second.empty()) {
      continue;
    }

    std::vector< typename TObject::Id_Type >& ids_by_coarse_ = ids_by_coarse[it->first.val() & 0x7fffff00];

    for (auto it2(it->second.begin());
        it2 != it->second.end(); ++it2)
      ids_by_coarse_.push_back(it2->id);
  }

  for (auto it = ids_by_coarse.begin(); it != ids_by_coarse.end(); ++it)
  {
    std::vector< typename TObject::Id_Type >& ids_by_coarse_ = it->second;
    if (!std::is_sorted(ids_by_coarse_.begin(), ids_by_coarse_.end())) {
      std::sort(ids_by_coarse_.begin(), ids_by_coarse_.end());
    }
    ids_by_coarse_.erase(std::unique(ids_by_coarse_.begin(), ids_by_coarse_.end()), ids_by_coarse_.end());
  }
}


template< class TIndex, class TObject >
void generate_ids_by_coarse
  (std::map< uint32, std::vector< typename TObject::Id_Type > >& ids_by_coarse,
   const std::map< TIndex, std::vector< TObject > >& items,
   typename TObject::Id_Type lower_id_bound, typename TObject::Id_Type upper_id_bound,
   bool skip_empty = false)
{
  for (auto it(items.begin()); it != items.end(); ++it)
  {
    if (skip_empty && it->second.empty()) {
      continue;
    }

    std::vector< typename TObject::Id_Type >& ids_by_coarse_ = ids_by_coarse[it->first.val() & 0x7fffff00];

    for (auto it2(it->second.begin());
        it2 != it->second.end(); ++it2) {
      if (!(it2->id < lower_id_bound) && it2->id < upper_id_bound) {
        ids_by_coarse_.push_back(it2->id);
      }
    }
  }

  for (auto it = ids_by_coarse.begin(); it != ids_by_coarse.end(); ++it)
  {
    std::vector< typename TObject::Id_Type >& ids_by_coarse_ = it->second;
    if (!std::is_sorted(ids_by_coarse_.begin(), ids_by_coarse_.end())) {
      std::sort(ids_by_coarse_.begin(), ids_by_coarse_.end());
    }
    ids_by_coarse_.erase(std::unique(ids_by_coarse_.begin(), ids_by_coarse_.end()), ids_by_coarse_.end());
  }
}


template< class TIndex, class TObject >
void generate_ids_by_coarse
  (std::map< uint32, std::vector< Attic< typename TObject::Id_Type > > >& ids_by_coarse,
   const std::map< TIndex, std::vector< TObject > >& items,
   bool skip_empty = false)
{
  for (auto it(items.begin()); it != items.end(); ++it)
  {
    if (skip_empty && it->second.empty()) {
      continue;
    }

    std::vector< Attic< typename TObject::Id_Type > >& ids_by_coarse_ = ids_by_coarse[it->first.val() & 0x7fffff00];

    for (auto it2(it->second.begin());
        it2 != it->second.end(); ++it2)
      ids_by_coarse_.push_back
          (Attic< typename TObject::Id_Type >(it2->id, it2->timestamp));
  }

  for (auto it = ids_by_coarse.begin(); it != ids_by_coarse.end(); ++it)
  {
    std::vector< Attic< typename TObject::Id_Type > >& ids_by_coarse_ = it->second;
    std::sort(ids_by_coarse_.begin(), ids_by_coarse_.end());
    ids_by_coarse_.erase(std::unique(ids_by_coarse_.begin(), ids_by_coarse_.end()), ids_by_coarse_.end());
  }
  for (auto it = ids_by_coarse.begin(); it != ids_by_coarse.end(); ++it)
    std::sort(it->second.begin(), it->second.end());
}


template <class T, class Object>
struct Tag_Index_Global_Handle_Methods;

struct Tag_Index_Global
{
  std::string key;
  std::string value;

  Tag_Index_Global() = default;

  Tag_Index_Global(const void* data) : key(((int8*)data + 4), unalignedLoad<uint16>(data)),
                                       value(((int8*)data + 4 + key.length()), unalignedLoad<uint16>((uint16*)data + 1))  {}

  Tag_Index_Global(const Tag_Index_Local& tag_idx) : key(tag_idx.key), value(tag_idx.value) {}

  Tag_Index_Global(const std::string& key_, const std::string& value_) : key(key_), value(value_) {}

  Tag_Index_Global(std::string&& key_, std::string&& value_) : key(std::move(key_)), value(std::move(value_)) {}

  uint32 size_of() const noexcept
  {
    return 4 + key.length() + value.length();
  }

  static uint32 size_of(const void* data) noexcept
  {
    return (unalignedLoad<uint16>(data) + unalignedLoad<uint16>((uint16*)data + 1) + 4);
  }

  void to_data(void* data) const noexcept
  {
    unalignedStore(data, (uint16)key.length());
    unalignedStore(((uint16*)data + 1), (uint16)value.length());
    memcpy(((uint8*)data + 4), key.data(), key.length());
    memcpy(((uint8*)data + 4 + key.length()), value.data(),
	   value.length());
  }

  bool operator<(const Tag_Index_Global& a) const noexcept
  {
    if (key != a.key)
      return (key < a.key);
    return (value < a.value);
  }

  bool operator==(const Tag_Index_Global& a) const noexcept
  {
    if (key != a.key)
      return false;
    return (value == a.value);
  }

  static uint32 max_size_of()
  {
    throw Unsupported_Error("static uint32 Tag_Index_Global::max_size_of()");
    return 0;
  }

  friend std::ostream & operator<<(std::ostream &os, const Tag_Index_Global& t);

  template <class T, class Object>
  using Handle_Methods = Tag_Index_Global_Handle_Methods<T, Object>;
};

inline std::ostream & operator<<(std::ostream &os, const Tag_Index_Global& p)
{
    return os << "[ " << p.key << " | " << p.value << " ]";
}


template <class T, class Object>
struct Tag_Index_Global_Handle_Methods
{
  inline bool has_key(std::string& key) const {
     return (static_cast<const T*>(this)->apply_func(Tag_Index_Global_Has_Key_Functor(key)));
  }

  inline bool has_value(std::string& value) const {
     return (static_cast<const T*>(this)->apply_func(Tag_Index_Global_Has_Value_Functor(value)));
  }

  inline std::string_view get_key() const {
    return (static_cast<const T*>(this)->apply_func(Tag_Index_Global_Get_Key_Functor()));
  }

  inline std::string_view get_value() const {
    return (static_cast<const T*>(this)->apply_func(Tag_Index_Global_Get_Value_Functor()));
  }

  inline bool operator<(const Tag_Index_Global& tig) const {
    return (static_cast<const T*>(this)->apply_func(Tag_Index_Global_Operator_Lower_Functor(tig)));
  }

private:
  struct Tag_Index_Global_Has_Key_Functor {
    Tag_Index_Global_Has_Key_Functor(std::string& key) : key(key) {};

    using reference_type = Tag_Index_Global;

    inline bool operator()(const void* data) const
     {
       char* k = ((int8*)data + 4);
       auto len = unalignedLoad<uint16>(data);
       return (len == key.length() && std::strncmp(k, key.c_str(), len) == 0);
     }

    private:
       std::string& key;
  };

  struct Tag_Index_Global_Has_Value_Functor {
    Tag_Index_Global_Has_Value_Functor(std::string& value) : value(value) {};

    using reference_type = Tag_Index_Global;

    inline bool operator()(const void* data) const
     {
       // char* k = ((int8*)data + 4);
      auto key_len = unalignedLoad<uint16>(data);

       char* v = ((int8*)data + 4 + key_len);
       auto value_len = unalignedLoad<uint16>((uint16*)data + 1);

       return (value_len == value.length() && std::strncmp(v, value.c_str(), value_len) == 0);
     }

    private:
       std::string& value;
  };

  struct Tag_Index_Global_Get_Key_Functor {
    Tag_Index_Global_Get_Key_Functor() = default;

    using reference_type = Tag_Index_Global;

    inline std::string_view operator()(const void* data) const
     {
       auto key_len = unalignedLoad<uint16>(data);
       char* k =  ((int8*)data + 4);
       return std::string_view(k, key_len);
     }
  };

  struct Tag_Index_Global_Get_Value_Functor {
    Tag_Index_Global_Get_Value_Functor() = default;

    using reference_type = Tag_Index_Global;

    inline std::string_view operator()(const void* data) const
     {
      auto key_len = unalignedLoad<uint16>(data);

       char* v = ((int8*)data + 4 + key_len);
       auto value_len = unalignedLoad<uint16>((uint16*)data + 1);

       return std::string_view(v, value_len);
     }
  };

  struct Tag_Index_Global_Operator_Lower_Functor {
    Tag_Index_Global_Operator_Lower_Functor(const Tag_Index_Global& tig_) : a(tig_) { }

    using reference_type = Tag_Index_Global;

    bool operator()(const void* data)
    {
      auto key = Tag_Index_Global_Get_Key_Functor()(data);

      if (key != a.key)
        return (key < a.key);

      auto value = Tag_Index_Global_Get_Value_Functor()(data);

      return (value < a.value);
    }

  private:
    const Tag_Index_Global & a;
  };
};



template <class T, class Object>
struct Tag_Object_Global_Handle_Methods;


template< typename Id_Type_ >
struct Tag_Object_Global
{
  typedef Id_Type_ Id_Type;

  Uint31_Index idx{};
  Id_Type id{};

  Tag_Object_Global() = default;

  Tag_Object_Global(Id_Type id_, Uint31_Index idx_) : idx(idx_), id(id_) {}

  Tag_Object_Global(const void* data)
  {
    idx = Uint31_Index(((unalignedLoad<uint32>(data))<<8) & 0xffffff00);
    id = Id_Type((void*)((uint8*)data + 3));
  }

  uint32 size_of() const noexcept
  {
    return 3 + id.size_of();
  }

  static uint32 size_of(const void* data) noexcept
  {
    return 3 + Id_Type::size_of((void*)((uint8*)data + 3));
  }

  void to_data(void* data) const noexcept
  {
    unalignedStore(data, (uint32)((idx.val()>>8) & 0x7fffff));
    id.to_data((void*)((uint8*)data + 3));
  }

  bool operator<(const Tag_Object_Global& a) const noexcept
  {
    if (id < a.id)
      return true;
    if (a.id < id)
      return false;

    return (idx < a.idx);
  }

  bool operator==(const Tag_Object_Global& a) const noexcept
  {
    return (id == a.id && idx == a.idx);
  }

  static uint32 max_size_of() noexcept
  {
    return 3 + Id_Type::max_size_of();
  }

  template <class T, class Object>
  using Handle_Methods = Tag_Object_Global_Handle_Methods<T, Object>;
};



template <class T, class Object>
struct Tag_Object_Global_Handle_Methods
{
  typename Object::Id_Type inline id() const {
     return (static_cast<const T*>(this)->apply_func(Tag_Object_Global_Id_Functor<typename Object::Id_Type>()));
  }

  inline Uint31_Index get_idx() const {
     return (static_cast<const T*>(this)->apply_func(Tag_Object_Global_Idx_Functor<typename Object::Id_Type>()));
  }

private:
  template <typename Id_Type >
  struct Tag_Object_Global_Id_Functor {
    Tag_Object_Global_Id_Functor() = default;

    using reference_type = Tag_Object_Global< Id_Type >;

    Id_Type operator()(const void* data) const
     {
      return Id_Type((void*)((uint8*)data + 3));
     }
  };

  template <typename Id_Type >
  struct Tag_Object_Global_Idx_Functor {
    Tag_Object_Global_Idx_Functor() = default;

    using reference_type = Tag_Object_Global< Id_Type >;

    Uint31_Index operator()(const void* data) const
     {
      return Uint31_Index((unalignedLoad<uint32>(data)<<8) & 0xffffff00);
     }
  };
};


#endif
