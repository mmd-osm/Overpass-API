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


typedef unsigned int uint;

typedef char int8;
typedef short int int16;
typedef int int32;
typedef long long int64;

typedef unsigned char uint8;
typedef unsigned short int uint16;
typedef unsigned int uint32;
typedef unsigned long long uint64;


template <class T, class Object>
struct Uint32_Index_Handle_Methods;

struct Uint32_Index
{
  typedef uint32 Id_Type;

  Uint32_Index() : value(0u) {}
  Uint32_Index(uint32 i) : value(i) {}
  Uint32_Index(void* data) : value(*(uint32*)data) {}

  uint32 size_of() const
  {
    return 4;
  }

  static uint32 max_size_of()
  {
    return 4;
  }

  static uint32 size_of(const void* data)
  {
    return 4;
  }

  void to_data(void* data) const
  {
    *(uint32*)data = value;
  }

  bool operator<(const Uint32_Index& index) const
  {
    return this->value < index.value;
  }

  bool operator==(const Uint32_Index& index) const
  {
    return this->value == index.value;
  }

  Uint32_Index operator++()
  {
    ++value;
    return this;
  }

  Uint32_Index operator+=(Uint32_Index offset)
  {
    value += offset.val();
    return this;
  }

  Uint32_Index operator+(Uint32_Index offset) const
  {
    Uint32_Index temp(*this);
    return (temp += offset);
  }

  uint32 val() const
  {
    return value;
  }

  template <class T, class Object>
  using Handle_Methods = Uint32_Index_Handle_Methods<T, Object>;

  friend std::ostream & operator<<(std::ostream &os, const Uint32_Index& t);

  protected:
    uint32 value;
};

inline std::ostream & operator<<(std::ostream &os, const Uint32_Index& p)
{
    return os << "[ " << p.value << " ]";
}


struct Uint32_Index_Val_Functor {
  Uint32_Index_Val_Functor() {};

  using reference_type = Uint32_Index;

  uint32 operator()(const void* data)
  {
    return *(uint32*)data;
  }
};

template <typename Id_Type >
struct Uint32_Id_Functor {
  Uint32_Id_Functor() {};

  using reference_type = Uint32_Index;

  Id_Type operator()(const void* data)
  {
    return *(Id_Type*)data;
  }
};


template <class T, class Object>
struct Uint32_Index_Handle_Methods
{
  uint32 inline get_val() const {
     return (static_cast<const T*>(this)->apply_func(Uint32_Index_Val_Functor()));
  }

  typename Object::Id_Type inline id() const {
     return (static_cast<const T*>(this)->apply_func(Uint32_Id_Functor<typename Object::Id_Type>()));
  }
};




inline Uint32_Index inc(Uint32_Index idx)
{
  return Uint32_Index(idx.val() + 1);
}


inline Uint32_Index dec(Uint32_Index idx)
{
  return Uint32_Index(idx.val() - 1);
}


inline unsigned long long difference(Uint32_Index lhs, Uint32_Index rhs)
{
  return rhs.val() - lhs.val();
}


struct Uint31_Index : Uint32_Index
{
  Uint31_Index() : Uint32_Index() {}
  Uint31_Index(uint32 i) : Uint32_Index(i) {}
  Uint31_Index(void* data) : Uint32_Index(*(uint32*)data) {}

  bool operator<(const Uint31_Index& index) const
  {
    if ((this->value & 0x7fffffff) != (index.value & 0x7fffffff))
    {
      return (this->value & 0x7fffffff) < (index.value & 0x7fffffff);
    }
    return (this->value < index.value);
  }

  friend std::ostream & operator<<(std::ostream &os, const Uint31_Index& t);
};

inline std::ostream & operator<<(std::ostream &os, const Uint31_Index& p)
{
    return os << "[ " << (p.value & 0x7fffffff) << " (" << p.value << ") ]";
}


inline Uint31_Index inc(Uint31_Index idx)
{
  if (idx.val() & 0x80000000)
    return Uint31_Index((idx.val() & 0x7fffffff) + 1);
  else
    return Uint31_Index(idx.val() | 0x80000000);
}


inline unsigned long long difference(Uint31_Index lhs, Uint31_Index rhs)
{
  return 2*(rhs.val() - lhs.val()) - ((lhs.val()>>31) & 0x1) + ((rhs.val()>>31) & 0x1);
}

template <class T, class Object>
struct Uint64_Handle_Methods;

struct Uint64
{
  typedef uint64 Id_Type;

  Uint64() : value(0ull) {}
  Uint64(uint64 i) : value(i) {}
  Uint64(void* data) : value(*(uint64*)data) {}

  uint32 size_of() const { return 8; }
  static uint32 max_size_of() { return 8; }
  static uint32 size_of(const void* data) { return 8; }

  void to_data(void* data) const
  {
    *(uint64*)data = value;
  }

  bool operator<(const Uint64& index) const
  {
    return this->value < index.value;
  }

  bool operator==(const Uint64& index) const
  {
    return this->value == index.value;
  }

  Uint64 operator++()
  {
    ++value;
    return this;
  }

  Uint64 operator+=(Uint64 offset)
  {
    value += offset.val();
    return this;
  }

  Uint64 operator+(Uint64 offset) const
  {
    Uint64 temp(*this);
    return (temp += offset);
  }

  uint64 val() const { return value; }

  template <class T, class Object>
  using Handle_Methods = Uint64_Handle_Methods<T, Object>;

  friend std::ostream & operator<<(std::ostream &os, const Uint64& t);

  protected:
    uint64 value;
};

inline std::ostream & operator<<(std::ostream &os, const Uint64& p)
{
    return os << "[ " << p.value << " ]";
}


template <typename Id_Type >
struct Uint64_Id_Functor {
  Uint64_Id_Functor() {};

  using reference_type = Uint64;

  Id_Type operator()(const void* data)
  {
    return *(Id_Type*)data;
  }
};

template <class T, class Object>
struct Uint64_Handle_Methods
{
  typename Object::Id_Type inline id() const {
     return (static_cast<const T*>(this)->apply_func(Uint64_Id_Functor<typename Object::Id_Type>()));
  }
};


struct Quad_Coord
{
  Quad_Coord() : ll_upper(0), ll_lower(0) {}
  Quad_Coord(uint32 ll_upper_, uint32 ll_lower_) : ll_upper(ll_upper_), ll_lower(ll_lower_) {}

  uint32 ll_upper;
  uint32 ll_lower;

  bool operator==(const Quad_Coord& rhs) const
  {
    return ll_upper == rhs.ll_upper && ll_lower == rhs.ll_lower;
  }
};


template <class T, class Object, class Element_Skeleton>
struct Attic_Handle_Methods;

template< typename Element_Skeleton >
struct Attic : public Element_Skeleton
{
  Attic(const Element_Skeleton& elem, uint64 timestamp_) : Element_Skeleton(elem), timestamp(timestamp_) {}

  Attic(Element_Skeleton&& elem, uint64 timestamp_) : Element_Skeleton(std::move(elem)), timestamp(timestamp_) {}

  uint64 timestamp;

  Attic(void* data)
    : Element_Skeleton(data),
      timestamp(*(uint64*)((uint8*)data + Element_Skeleton::size_of(data)) & 0xffffffffffull) {}

  uint32 size_of() const
  {
    return Element_Skeleton::size_of() + 5;
  }

  static uint32 size_of(const void* data)
  {
    return Element_Skeleton::size_of(data) + 5;
  }

  void to_data(void* data) const
  {
    Element_Skeleton::to_data(data);
    void* pos = (uint8*)data + Element_Skeleton::size_of();
    *(uint32*)(pos) = (timestamp & 0xffffffffull);
    *(uint8*)((uint8*)pos+4) = ((timestamp & 0xff00000000ull)>>32);
  }

  bool operator<(const Attic& rhs) const
  {
    if (*static_cast< const Element_Skeleton* >(this) < *static_cast< const Element_Skeleton* >(&rhs))
      return true;
    else if (*static_cast< const Element_Skeleton* >(&rhs) < *static_cast< const Element_Skeleton* >(this))
      return false;
    return (timestamp < rhs.timestamp);
  }

  bool operator==(const Attic& rhs) const
  {
    return (*static_cast< const Element_Skeleton* >(this) == rhs && timestamp == rhs.timestamp);
  }

  template <class T, class Object>
  using Handle_Methods = Attic_Handle_Methods<T, Object, Element_Skeleton>;
};

template< typename Element_Skeleton >
struct Attic_Timestamp_Functor {
  Attic_Timestamp_Functor() {};

  using reference_type = Attic< Element_Skeleton >;

  uint64 operator()(const void* data) const
   {
    uint64 _timestamp(*(uint64*)((uint8*)data + Element_Skeleton::size_of(data)) & 0xffffffffffull);
    return _timestamp;
   }
};

template <typename...> using void_t = void;

template< typename Object >
class Handle;

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
  uint64 inline get_timestamp() const {
     return (static_cast<const T*>(this)->apply_func(Attic_Timestamp_Functor< Element_Skeleton >()));
  }

};


template< typename Attic >
struct Delta_Comparator
{
public:
  bool operator()(const Attic& lhs, const Attic& rhs) const
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
  bool operator()(const Attic* lhs, const Attic* rhs) const
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
  for (typename std::vector< Object >::const_iterator it = source.begin(); it != source.end(); ++it)
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
  std::vector< uint >::const_iterator it_removed = removed.begin();
  typename std::vector< std::pair< uint, Object > >::const_iterator it_added = added.begin();
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
  int removed_min;
  int removed_max;
  int added_min;
  int added_max;
  bool copy_prefix;
  bool copy_suffix;
  int prefix_end_index;
  int suffix_start_index;

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
  if ( prefix_end_index < 0 || prefix_end_index > reference.size()) {
    copy_prefix = false;
  }

  if (suffix_start_index < 0 || suffix_start_index > reference.size()) {
    copy_suffix = false;
  }

  // copy prefix
  if (copy_prefix) {
    target.insert(target.end(), reference.cbegin(), reference.cbegin() + prefix_end_index);
  }

  // copy added elements
   typename std::vector< std::pair< uint, Object > >::const_iterator it_added = added.begin();

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
  mutable uint32 ref;   // not thread safe!

  inline SharedData() : ref(0) { }
  inline SharedData(const SharedData &) : ref(0) { }

  // using the assignment operator would lead to corruption in the ref-counting
  SharedData &operator=(const SharedData &) = delete;
};

template <class T> class SharedDataPointer
{
public:
  typedef T Type;
  typedef T *pointer;

  inline void detach() { if (d && d->ref != 1) detach_helper(); }
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

  inline SharedDataPointer() { d = nullptr; }
  inline ~SharedDataPointer() { if (d && !--d->ref) delete d; }

  SharedDataPointer(SharedDataPointer &&o) noexcept : d(o.d) { o.d = nullptr; }
  inline SharedDataPointer<T> &operator=(SharedDataPointer<T> &&other) noexcept
  { std::swap(d, other.d); return *this; }

  explicit SharedDataPointer(T *data) noexcept;
  inline SharedDataPointer(const SharedDataPointer<T> &o) : d(o.d) { if (d) ++d->ref; }
  inline SharedDataPointer<T> & operator=(const SharedDataPointer<T> &o) {
    if (o.d != d) {
      if (o.d)
        ++o.d->ref;
      T *old = d;
      d = o.d;
      if (old && !--old->ref)
        delete old;
    }
    return *this;
  }
  inline SharedDataPointer &operator=(T *o) {
    if (o != d) {
      if (o)
        ++o->ref;
      T *old = d;
      d = o;
      if (old && !--old->ref)
        delete old;
    }
    return *this;
  }

  inline bool operator!() const { return !d; }

protected:
  T *clone();

private:
  void detach_helper();

  T *d;
};

template <class T>
SharedDataPointer<T>::SharedDataPointer(T *adata) noexcept
: d(adata) { if (d) ++d->ref; }

template <class T>
T *SharedDataPointer<T>::clone()
{
  return new T(*d);
}

template <class T>
void SharedDataPointer<T>::detach_helper()
{
  T *x = clone();
  ++x->ref;
  if (!(--d->ref))
    delete d;
  d = x;
}


#endif
