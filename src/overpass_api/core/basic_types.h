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

#ifndef DE__OSM3S___OVERPASS_API__CORE__BASIC_TYPES_H
#define DE__OSM3S___OVERPASS_API__CORE__BASIC_TYPES_H

#include <iostream>
#include <vector>
#include <type_traits>

#include <protozero/varint.hpp>

#ifdef HAVE_CONFIG_H
#include <config.h>
#undef VERSION
#endif

#include "../../template_db/types.h"

typedef unsigned int uint;

typedef char int8;
typedef short int int16;
typedef int int32;
typedef long long int64;

typedef unsigned char uint8;
typedef unsigned short int uint16;
typedef unsigned int uint32;
typedef unsigned long long uint64;

struct Uint64;
struct Uint40;

// Global object Id type
// needs to be large enough to store node, way and relation object ids
using Global_Id_Type = Uint40;

typedef uint32 timestamp_t;


template <typename Object >
struct Generic_Add_Element_Functor {
  Generic_Add_Element_Functor(std::vector< Object >& v_) : v(v_) {};

  using reference_type = Object;

  inline void operator()(const void* data) const
  {
    v.emplace_back(data);
  }

private:
  std::vector< Object > & v;
};


template <class T, class Object>
struct Uint32_Index_Handle_Methods;

struct Uint32_Index
{
  typedef uint32 Id_Type;

  Uint32_Index() noexcept = default;
  Uint32_Index(uint32 i) noexcept : value(i) {}
  Uint32_Index(const void* data) noexcept : value(unalignedLoad<uint32>(data)) {}

  uint32 size_of() const noexcept
  {
    return 4;
  }

  static constexpr uint32 max_size_of() noexcept
  {
    return 4;
  }

  static uint64 max_value() noexcept { return (1ull  << (8 * max_size_of())) - 1; }

  static uint32 size_of(const void* ) noexcept
  {
    return 4;
  }

  static constexpr bool is_fixed_size() noexcept { return true; }

  void to_data(void* data) const noexcept
  {
    unalignedStore(data, value);
  }

  bool operator<(const Uint32_Index& index) const noexcept
  {
    return this->value < index.value;
  }

  bool operator==(const Uint32_Index& index) const noexcept
  {
    return this->value == index.value;
  }

  Uint32_Index operator++() noexcept
  {
    ++value;
    return this;
  }

  Uint32_Index operator+=(Uint32_Index offset) noexcept
  {
    value += offset.val();
    return this;
  }

  Uint32_Index operator+(Uint32_Index offset) const noexcept
  {
    Uint32_Index temp(*this);
    return (temp += offset);
  }

  uint32 val() const noexcept
  {
    return value;
  }

  template <class T, class Object>
  using Handle_Methods = Uint32_Index_Handle_Methods<T, Object>;

  friend std::ostream & operator<<(std::ostream &os, const Uint32_Index& t);

  protected:
    uint32 value{};
};

inline std::ostream & operator<<(std::ostream &os, const Uint32_Index& p)
{
    return os << "[ " << p.value << " ]";
}



template <class T, class Object>
struct Uint32_Index_Handle_Methods
{
  uint32 inline get_val() const {
     return (static_cast<const T*>(this)->apply_func(Uint32_Index_Val_Functor()));
  }

  typename Object::Id_Type inline id() const {
     return (static_cast<const T*>(this)->apply_func(Uint32_Id_Functor<typename Object::Id_Type>()));
  }

private:
  struct Uint32_Index_Val_Functor {
    Uint32_Index_Val_Functor() = default;

    using reference_type = Uint32_Index;

    uint32 operator()(const void* data)
    {
      return unalignedLoad<uint32>(data);
    }
  };

  template <typename Id_Type >
  struct Uint32_Id_Functor {
    Uint32_Id_Functor() = default;

    using reference_type = Uint32_Index;

    Id_Type operator()(const void* data)
    {
      return unalignedLoad<Id_Type>(data);
    }
  };
};

namespace std {
  template <> struct hash<Uint32_Index>
  {
    size_t operator()(const Uint32_Index & x) const
    {
      return hash<uint32>()(x.val());
    }
  };
}

#ifdef HAVE_ANKERL
template <>
struct ankerl::unordered_dense::hash<Uint32_Index> {
    using is_avalanching = void;

    [[nodiscard]] auto operator()(Uint32_Index const& x) const noexcept -> uint64_t {
        return detail::wyhash::hash(x.val());
    }
};

#endif


inline Uint32_Index inc(Uint32_Index idx) noexcept
{
  return Uint32_Index(idx.val() + 1);
}


inline Uint32_Index dec(Uint32_Index idx) noexcept
{
  return Uint32_Index(idx.val() - 1);
}


inline unsigned long long difference(Uint32_Index lhs, Uint32_Index rhs) noexcept
{
  return rhs.val() - lhs.val();
}

template <class T, class Object>
struct Uint31_Index_Handle_Methods;

struct Uint31_Index : Uint32_Index
{
  Uint31_Index() noexcept = default;
  Uint31_Index(uint32 i) noexcept : Uint32_Index(i) {}
  Uint31_Index(const void* data) noexcept : Uint32_Index(unalignedLoad<uint32>(data)) {}

  bool operator<(const Uint31_Index& index) const noexcept
  {
    if ((this->value & 0x7fffffff) != (index.value & 0x7fffffff))
    {
      return (this->value & 0x7fffffff) < (index.value & 0x7fffffff);
    }
    return (this->value < index.value);
  }

  inline bool is_compound_idx() const noexcept
  {
    return val() & 0x80000000u;
  }

  friend std::ostream & operator<<(std::ostream &os, const Uint31_Index& t);

  template <class T, class Object>
  using Handle_Methods = Uint31_Index_Handle_Methods<T, Object>;
};

inline std::ostream & operator<<(std::ostream &os, const Uint31_Index& p)
{
    return os << "[ " << (p.value & 0x7fffffff) << " (" << p.value << ") ]";
}


inline Uint31_Index inc(Uint31_Index idx) noexcept
{
  if (idx.val() & 0x80000000)
    return Uint31_Index((idx.val() & 0x7fffffff) + 1);
  else
    return Uint31_Index(idx.val() | 0x80000000);
}


template <class T, class Object>
struct Uint31_Index_Handle_Methods
{
  uint32 inline get_val() const {
     return (static_cast<const T*>(this)->apply_func(Uint31_Index_Val_Functor()));
  }

  typename Object::Id_Type inline id() const {
     return (static_cast<const T*>(this)->apply_func(Uint31_Id_Functor<typename Object::Id_Type>()));
  }

private:
  struct Uint31_Index_Val_Functor {
    Uint31_Index_Val_Functor() = default;

    using reference_type = Uint31_Index;

    uint32 operator()(const void* data)
    {
      return unalignedLoad<uint32>(data);
    }
  };

  template <typename Id_Type >
  struct Uint31_Id_Functor {
    Uint31_Id_Functor() = default;

    using reference_type = Uint31_Index;

    Id_Type operator()(const void* data)
    {
      return unalignedLoad<Id_Type>(data);
    }
  };
};

namespace std {
  template <> struct hash<Uint31_Index>
  {
    size_t operator()(const Uint31_Index & x) const
    {
      return hash<uint32>()(x.val());
    }
  };
}


#ifdef HAVE_ANKERL
template <>
struct ankerl::unordered_dense::hash<Uint31_Index> {
    using is_avalanching = void;

    [[nodiscard]] auto operator()(Uint31_Index const& x) const noexcept -> uint64_t {
        return detail::wyhash::hash(x.val());
    }
};

#endif


inline unsigned long long difference(Uint31_Index lhs, Uint31_Index rhs)
{
  return 2*(rhs.val() - lhs.val()) - ((lhs.val()>>31) & 0x1) + ((rhs.val()>>31) & 0x1);
}

template <class T, class Object>
struct Uint64_Handle_Methods;

struct Uint64
{
  typedef uint64 Id_Type;

  Uint64() noexcept = default;
  Uint64(uint64 i) noexcept : value(i) {}
  Uint64(const void* data) noexcept : value(unalignedLoad<uint64>(data)) {}

  uint32 size_of() const noexcept { return 8; }
  static uint32 max_size_of() noexcept { return 8; }
  static uint64 max_value() noexcept { return (1ull  << (8 * max_size_of())) - 1; }
  static uint32 size_of(const void* ) noexcept { return 8; }

  void to_data(void* data) const noexcept
  {
    unalignedStore(data, value);
  }

  bool operator<(const Uint64& index) const noexcept
  {
    return this->value < index.value;
  }

  bool operator==(const Uint64& index) const noexcept
  {
    return this->value == index.value;
  }

  Uint64 operator++() noexcept
  {
    ++value;
    return this;
  }

  Uint64 operator+=(Uint64 offset) noexcept
  {
    value += offset.val();
    return this;
  }

  Uint64 operator+(Uint64 offset) const noexcept
  {
    Uint64 temp(*this);
    return (temp += offset);
  }

  uint64 val() const noexcept { return value; }

  template <class T, class Object>
  using Handle_Methods = Uint64_Handle_Methods<T, Object>;

  friend std::ostream & operator<<(std::ostream &os, const Uint64& t);

  protected:
    uint64 value{};
};

inline std::ostream & operator<<(std::ostream &os, const Uint64& p)
{
    return os << "[ " << p.value << " ]";
}




template <class T, class Object>
struct Uint64_Handle_Methods
{
  typename Object::Id_Type inline id() const {
     return (static_cast<const T*>(this)->apply_func(Uint64_Id_Functor<typename Object::Id_Type>()));
  }

private:
  template <typename Id_Type >
  struct Uint64_Id_Functor {
    Uint64_Id_Functor() = default;

    using reference_type = Uint64;

    Id_Type operator()(const void* data)
    {
      return Uint64(data).val();
    }
  };
};


/*-------------------------------------*/


template <class T, class Object>
struct Uint40_Handle_Methods;

struct Uint40
{
  typedef uint64 Id_Type;

  Uint40() noexcept = default;
  Uint40(uint64 i) noexcept : value(i) {}
  Uint40(const void* data) noexcept {
    value = (uint64)(unalignedLoad<uint32>(data));
    value |= (uint64)(*(uint8*)((uint8*)data + 4)) << 32;
  }

  uint32 size_of() const noexcept { return 5; }
  static constexpr uint32 max_size_of() noexcept { return 5; }
  static uint64 max_value() noexcept { return (1ull  << (8 * max_size_of())) - 1; }
  static uint32 size_of(const void* ) noexcept { return 5; }
  static constexpr bool is_fixed_size() noexcept { return true; }

  void to_data(void* data) const noexcept
  {
    void* pos = (uint8*)data;
    unalignedStore(pos, (uint32)(value & 0xffffffffull));
    *(uint8*)((uint8*)pos+4) = ((value & 0xff00000000ull)>>32);
  }

  bool operator<(const Uint40& index) const noexcept
  {
    return this->value < index.value;
  }

  bool operator==(const Uint40& index) const noexcept
  {
    return this->value == index.value;
  }

  Uint40 operator++() noexcept
  {
    ++value;
    return this;
  }

  Uint40 operator+=(Uint40 offset) noexcept
  {
    value += offset.val();
    return this;
  }

  Uint40 operator+(Uint40 offset) const noexcept
  {
    Uint40 temp(*this);
    return (temp += offset);
  }

  uint64 val() const noexcept { return value; }

  template <class T, class Object>
  using Handle_Methods = Uint40_Handle_Methods<T, Object>;

  friend std::ostream & operator<<(std::ostream &os, const Uint40& t);

  protected:
    uint64 value{};
};

inline std::ostream & operator<<(std::ostream &os, const Uint40& p)
{
    return os << "[ " << p.value << " ]";
}


template <class T, class Object>
struct Uint40_Handle_Methods
{
  typename Object::Id_Type inline id() const {
     return (static_cast<const T*>(this)->apply_func(Uint40_Id_Functor<typename Object::Id_Type>()));
  }

private:
  template <typename Id_Type >
  struct Uint40_Id_Functor {
    Uint40_Id_Functor() = default;

    using reference_type = Uint40;

    Id_Type operator()(const void* data)
    {
      return Uint40(data).val();
    }
  };
};



namespace std {
  template <> struct hash<Uint40>
  {
    size_t operator()(const Uint40 & x) const
    {
      return hash<uint64>()(x.val());
    }
  };
}



struct Quad_Coord
{
  Quad_Coord() noexcept = default;
  Quad_Coord(uint32 ll_upper_, uint32 ll_lower_) noexcept : ll_upper(ll_upper_), ll_lower(ll_lower_) {}

  uint32 ll_upper{};
  uint32 ll_lower{};

  bool operator==(const Quad_Coord& rhs) const noexcept
  {
    return ll_upper == rhs.ll_upper && ll_lower == rhs.ll_lower;
  }
};



namespace detail
{
template <template <typename > class C>
struct is_base_of_any_helper
{
  template <typename T>
  std::true_type operator ()(const C<T>*) const;

  std::false_type operator() (...) const;

};

}

template <template <typename > class C , typename T>
using is_base_of_any = decltype(detail::is_base_of_any_helper<C>{}(std::declval<const T*>()));


template <class T, class Object, class Element_Skeleton>
struct Attic_Handle_Methods;

template< typename Element_Skeleton >
struct
#ifdef HAVE_WORD_ALIGNMENT
__attribute__ ((packed, aligned(4)))
#endif
Attic : public Element_Skeleton
{
  static_assert(!is_base_of_any< Attic , Element_Skeleton >::value, "Nested attic: Element_Skeleton may not be an Attic struct itself");

  Attic() = default;

  Attic(const Element_Skeleton& elem, timestamp_t timestamp_) : Element_Skeleton(elem), timestamp(timestamp_) {}

  Attic(Element_Skeleton&& elem, timestamp_t timestamp_) : Element_Skeleton(std::move(elem)), timestamp(timestamp_) {}

  timestamp_t timestamp{};

  Attic(const void* data)
    : Element_Skeleton(data) {

    const void* pos = (uint8*)data + Element_Skeleton::size_of(data);

    timestamp = unalignedLoad<timestamp_t>(pos);
  }

  uint32 size_of() const noexcept
  {
    return Element_Skeleton::size_of() + 4;
  }

  static uint32 size_of(const void* data)
  {
    return Element_Skeleton::size_of(data) + 4;
  }

  void to_data(void* data) const noexcept
  {
    Element_Skeleton::to_data(data);
    void* pos = (uint8*)data + Element_Skeleton::size_of();
    unalignedStore(pos, timestamp);
  }

  bool operator<(const Attic& rhs) const noexcept
  {
    if (*static_cast< const Element_Skeleton* >(this) < *static_cast< const Element_Skeleton* >(&rhs))
      return true;
    else if (*static_cast< const Element_Skeleton* >(&rhs) < *static_cast< const Element_Skeleton* >(this))
      return false;
    return (timestamp < rhs.timestamp);
  }

  bool operator==(const Attic& rhs) const noexcept
  {
    return (*static_cast< const Element_Skeleton* >(this) == rhs && timestamp == rhs.timestamp);
  }

  template <class T, class Object>
  using Handle_Methods = Attic_Handle_Methods<T, Object, Element_Skeleton>;
};


template <typename...> using void_t = void;

template< typename Object >
struct Handle;

template <class T, class Object>
struct Empty_Element_Handle { };


template <typename Object, typename = void>
struct Element_Base
{  // Empty class is the default fallback if Object doesn't have a Handle_Methods member type alias
   using type = Empty_Element_Handle < Handle < Object >, Object >;
};

template <typename Object>
struct Element_Base<Object,  void_t<decltype( typename Object::template Handle_Methods< Handle < Object>, Object > ()) >  >
{
   using type = typename Object::template Handle_Methods < Handle < Object>, Object >;
};



template <class T, class Object, class Element_Skeleton>
struct Attic_Handle_Methods : public Element_Base<Element_Skeleton>::type
{
  timestamp_t inline get_timestamp() const {
     return (static_cast<const T*>(this)->apply_func(Attic_Timestamp_Functor()));
  }

  void inline add_element(std::vector< Attic< Element_Skeleton > > & v) const {
    static_cast<const T*>(this)->apply_func(Attic_Add_Element_Functor(v));
  }

private:
  struct Attic_Timestamp_Functor {
    Attic_Timestamp_Functor() = default;

    using reference_type = Attic< Element_Skeleton >;

    timestamp_t operator()(const void* data) const
    {
      const void* pos = (uint8*)data + Element_Skeleton::size_of(data);

      return unalignedLoad<timestamp_t>(pos);
    }
  };

  struct Attic_Add_Element_Functor {
    Attic_Add_Element_Functor(std::vector< Attic< Element_Skeleton > >& v_) : v(v_) {};

    using reference_type = Attic< Element_Skeleton >;

    void operator()(const void* data) const
    {
       v.emplace_back(data);
    }

  private:
    std::vector< Attic< Element_Skeleton > > & v;
  };
};

template< typename Element_Skeleton >
inline std::ostream & operator<<(std::ostream &os, const Attic<  Element_Skeleton >& p)
{
  auto skel = dynamic_cast<const Element_Skeleton*>(&p);

  return os << "Attic(" << p.timestamp << ": " << (skel != nullptr ? *skel : "(null)") << ")";
}



template< typename Attic >
struct Delta_Comparator
{
public:
  bool operator()(const Attic& lhs, const Attic& rhs) const noexcept
  {
    if (lhs.id == rhs.id)
      return rhs.timestamp < lhs.timestamp;
    else
      return lhs.id < rhs.id;
  }
};


template< typename Attic >
struct Delta_Ref_Comparator
{
public:
  bool operator()(const Attic* lhs, const Attic* rhs) const noexcept
  {
    if (lhs->id == rhs->id)
      return rhs->timestamp < lhs->timestamp;
    else
      return lhs->id < rhs->id;
  }
};


template< typename Object >
void make_delta(const std::vector< Object >& source, const std::vector< Object >& reference,
                std::vector< uint >& to_remove, std::vector< std::pair< uint, Object > >& to_add)
{
  //Detect a common prefix
  uint prefix_length = 0;
  while (prefix_length < source.size() && prefix_length < reference.size()
      && source[prefix_length] == reference[prefix_length])
    ++prefix_length;

  //Detect a common suffix
  uint suffix_length = 1;
  while (suffix_length < source.size() - prefix_length && suffix_length < reference.size() - prefix_length
      && source[source.size() - suffix_length] == reference[reference.size() - suffix_length])
    ++suffix_length;
  --suffix_length;

  for (uint i = prefix_length; i < reference.size() - suffix_length; ++i)
    to_remove.push_back(i);

  for (uint i = prefix_length; i < source.size() - suffix_length; ++i)
    to_add.push_back(std::make_pair(i, source[i]));
}


template< typename Object >
void copy_elems(const std::vector< Object >& source, std::vector< std::pair< uint, Object > >& target)
{
  uint i = 0;
  for (auto it = source.begin(); it != source.end(); ++it)
    target.push_back(std::make_pair(i++, *it));
}


template< typename Object >
void expand_diff(const std::vector< Object >& reference,
    const std::vector< uint >& removed, const std::vector< std::pair< uint, Object > >& added,
    std::vector< Object >& target)
{
  if (removed.empty() && added.empty())
  {
    target = reference;
    return;
  }

  target.reserve(reference.size() - removed.size() + added.size());
  auto it_removed = removed.begin();
  auto it_added = added.begin();
  for (uint i = 0; i < reference.size(); ++i)
  {
    while (it_added != added.end() && target.size() == it_added->first)
    {
      target.push_back(it_added->second);
      ++it_added;
    }

    if (it_removed == removed.end() || i < *it_removed)
      target.push_back(reference[i]);
    else
      ++it_removed;
  }
  while (it_added != added.end() && target.size() == it_added->first)
  {
    target.push_back(it_added->second);
    ++it_added;
  }
}

template< typename Object >
void expand_diff_fast(std::vector< Object >& reference,
    const std::vector< uint >& removed, const std::vector< std::pair< uint, Object > >& added,
    std::vector< Object >& target)
{
  uint removed_min{};
  uint removed_max{};
  uint added_min{};
  uint added_max{};
  bool copy_prefix;
  bool copy_suffix;
  uint prefix_end_index{};
  uint suffix_start_index{};

  if (removed.empty() && added.empty())
  {
    target = std::move(reference);
    return;
  }

  if (!removed.empty()) {
    removed_min = removed.front();
    removed_max = removed.back();

    // Paranoia fallback, if removed does not match expected structure (should never be called!)
    // Code assumes that removed has been populated by make_delta method above
    if (removed_max - removed_min + 1 != removed.size()) {
      expand_diff(reference, removed, added, target);
      return;
    }
  }

  if (!added.empty()) {
    added_min = added.front().first;
    added_max = added.back().first;

    // Fallback (should never be called!)
    // Code assumes that removed has been populated by make_delta method above
    if (added_max - added_min + 1 != added.size()) {
      expand_diff(reference, removed, added, target);
      return;
    }
  }

  // Added and Removed have exactly the same indices -> do in-place update of elements instead of copying the reference
  if (added.size() == removed.size() &&
      added_min    == removed_min    &&
      added_max    == removed_max) {

    target = std::move(reference);

    for (const auto & e : added) {
      target[e.first] = e.second;
    }
    return;
  }

  target.reserve(reference.size() - removed.size() + added.size());

  if (reference.empty()) {
    // in case reference is empty, we only add new elements
    copy_prefix = false;
    copy_suffix = false;
  }
  else if (removed.empty()) {
     // only new elements should be added
    copy_prefix = true;
    copy_suffix = true;
    prefix_end_index = added_min;
    suffix_start_index = added_min;
  } else {
    copy_prefix = true;
    copy_suffix = true;

    prefix_end_index = removed_min;
    suffix_start_index = removed_max + 1;
  }

  // out of range checks
  if (prefix_end_index > reference.size()) {
    copy_prefix = false;
  }

  if (suffix_start_index > reference.size()) {
    copy_suffix = false;
  }

  // copy prefix
  if (copy_prefix) {
    target.insert(target.end(), reference.cbegin(), reference.cbegin() + prefix_end_index);
  }

  // copy added elements
   auto it_added = added.begin();

   while (it_added != added.end() && target.size() == it_added->first)
   {
     target.emplace_back(std::move(it_added->second));
     ++it_added;
   }

   // copy suffix
   if (copy_suffix) {
     target.insert(target.end(), reference.cbegin() + suffix_start_index, reference.cend());
   }
}

// Shared data helper classes
// Derived from QtCore module of the Qt Toolkit. (qt5)

template <class T> class SharedDataPointer;

class SharedData
{
public:
  inline SharedData() = default;
  inline SharedData(const SharedData &) {}

  // using the assignment operator would lead to corruption in the ref-counting
  SharedData &operator=(const SharedData &) = delete;

  inline uint32& ref() { return ref_count; }
private:
  mutable uint32 ref_count{};   // not thread safe! code used atomic originally, but we don't need it here
};

template <class T> class SharedDataPointer
{
  static_assert(std::is_base_of<SharedData, T>::value, "T must be a classes derived from SharedData");

public:
  typedef T Type;
  typedef T *pointer;

  inline void detach() { if (d && d->ref() != 1) detach_helper(); }
  inline T &operator*() { detach(); return *d; }
  inline const T &operator*() const { return *d; }
  inline T *operator->() { detach(); return d; }
  inline const T *operator->() const { return d; }
  inline operator T *() { detach(); return d; }
  inline operator const T *() const { return d; }
  inline T *data() { detach(); return d; }
  inline const T *data() const { return d; }
  inline const T *constData() const { return d; }

  inline bool operator==(const SharedDataPointer<T> &other) const { return d == other.d; }
  inline bool operator!=(const SharedDataPointer<T> &other) const { return d != other.d; }

  inline SharedDataPointer() noexcept = default;
  inline ~SharedDataPointer() { if (d && !--d->ref()) delete d; }

  SharedDataPointer(SharedDataPointer &&o) noexcept : d(o.d) { o.d = nullptr; }
  inline SharedDataPointer<T> &operator=(SharedDataPointer<T> &&other) noexcept
  { std::swap(d, other.d); return *this; }

  explicit SharedDataPointer(T *data) noexcept;
  inline SharedDataPointer(const SharedDataPointer<T> &o) : d(o.d) { if (d) ++d->ref(); }
  inline SharedDataPointer<T> & operator=(const SharedDataPointer<T> &o) {
    if (o.d != d) {
      if (o.d)
        ++o.d->ref();
      T *old = d;
      d = o.d;
      if (old && !--old->ref())
        delete old;
    }
    return *this;
  }
  inline SharedDataPointer &operator=(T *o) {
    if (o != d) {
      if (o)
        ++o->ref();
      T *old = d;
      d = o;
      if (old && !--old->ref())
        delete old;
    }
    return *this;
  }

  inline bool operator!() const { return !d; }

protected:
  T *clone();

private:
  void detach_helper();

  T *d = nullptr;
};

template <class T>
SharedDataPointer<T>::SharedDataPointer(T *adata) noexcept
: d(adata) { if (d) ++d->ref(); }

template <class T>
T *SharedDataPointer<T>::clone()
{
  return new T(*d);
}

template <class T>
void SharedDataPointer<T>::detach_helper()
{
  T *x = clone();
  ++x->ref();
  if (!(--d->ref()))
    delete d;
  d = x;
}


namespace {

template <typename Id_Type >
inline uint32 calculate_ids_compressed_size(const std::vector< Id_Type >& ids_)
{
  Id_Type prev = (uint64) 0;
  uint32 compressed_size = 0;

  for (auto it = ids_.begin();
      it != ids_.end(); ++it)
  {
    int64_t diff = (int64_t) it->val() - (int64_t) prev.val();
    compressed_size += protozero::length_of_varint(protozero::encode_zigzag64(diff));
    prev = it->val();
  }
  compressed_size += compressed_size & 1;
  return compressed_size;
}


template <typename Id_Type >
uint8* compress_ids(const std::vector< Id_Type >& ids_, uint8* buffer_)
{
  char* current = (char*) buffer_;
  char* buffer = (char*) buffer_;
  Id_Type prev = (uint64) 0;

  for (auto it = ids_.begin();
       it != ids_.end(); ++it)
  {
    int64_t delta = (int64_t) it->val() - (int64_t) prev.val();
    uint64 zigzag = protozero::encode_zigzag64(delta);
    int size = protozero::add_varint_to_buffer(current, zigzag);
    current += size;
    prev = it->val();
  }

  if ((current - buffer) & 1)    // add padding byte
    *current++ = 0;

  return (uint8*) current;
}

template <typename Id_Type, typename Functor >
void decompress_ids(const uint16 ids_count, const uint16 ids_bytes, uint8* buffer_, Functor& f)
{
  const char* current = (char*) buffer_;
  const char* end = (char*)(buffer_ + ids_bytes);

  Id_Type id = (uint64) 0;

  for (int i=0; i<ids_count;i++)
  {
    auto value = protozero::decode_varint(&current, end);
    int64_t delta = protozero::decode_zigzag64(value);
    id += delta;
    f(static_cast<const Id_Type>(id));   // Call functor as callback
  }
}

// Function iterates over all nodes in a way as long as the functor does not return "true" (=match found)
template <typename Id_Type, typename Functor >
[[nodiscard]] bool decompress_ids_matches_any(const uint16 ids_count, const uint16 ids_bytes, uint8* buffer_, Functor& f)
{
  const char* current = (char*) buffer_;
  const char* end = (char*)(buffer_ + ids_bytes);

  Id_Type id = (uint64) 0;

  for (int i=0; i<ids_count;i++)
  {
    auto value = protozero::decode_varint(&current, end);
    int64_t delta = protozero::decode_zigzag64(value);
    id += delta;
    if (f(static_cast<const Id_Type>(id)))      // did the callback function indicate that a match was found for "id"?
      return true;  // match found, we're done.
  }
  return false;
}

template <typename Id_Type >
uint8* decompress_ids(std::vector< Id_Type >& ids_, const uint16 ids_count, const uint16 ids_bytes, uint8* buffer_)
{
  const char* current = (char*) buffer_;
  const char* end = (char*)(buffer_ + ids_bytes);

  ids_.resize(ids_count);

  Id_Type id = (uint64) 0;

  for (int i=0; i<ids_count;i++)
  {
    auto value = protozero::decode_varint(&current, end);
    int64_t delta = protozero::decode_zigzag64(value);
    id += delta;
    ids_[i] = id;
  }
  if ((current - (char*) buffer_) & 1)    // add padding byte
    current++;
  return (uint8*) current;
}

}


#endif
