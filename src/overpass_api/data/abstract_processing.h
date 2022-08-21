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

#ifndef DE__OSM3S___OVERPASS_API__DATA__ABSTRACT_PROCESSING_H
#define DE__OSM3S___OVERPASS_API__DATA__ABSTRACT_PROCESSING_H

#include "../core/datatypes.h"
#include "../statements/statement.h"
#include "collect_items.h"
#include "filenames.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <iterator>
#include <memory>
#include <type_traits>
#include <vector>



template <typename T, unsigned int L = 25>
class IdSetHybrid {
  static_assert(std::is_unsigned<T>::value, "Needs unsigned type");
  static_assert(sizeof(T) >= 4, "Needs at least 32bit type");

  constexpr static const std::size_t chunk_bits = L;
  constexpr static const std::size_t chunk_size = 1 << (chunk_bits - 6);  // number of 64bit words in chunk
  constexpr static const std::size_t max_vector_elem_count = (1UL << (chunk_bits - 5UL));

  std::vector<std::vector<uint64_t>>   m_data_bitmap;
  std::vector<std::vector<uint32_t>>   m_data_vector;
  std::vector<bool> m_data_use_bitmap;
  std::size_t m_cid_size = 0;

  T m_size = 0;  // number of additions (may include duplicates)

  static std::size_t chunk_id(T id) noexcept { return id >> chunk_bits; }

  static uint64_t lower(T id) noexcept {
    return (id & ((1UL << chunk_bits) - 1UL));
  }

  static uint64_t lower_l(T id) noexcept {
    return (lower(id) & ((1UL << 6UL) - 1UL));
  }

  static uint64_t bitmask(T id) noexcept {
    return (1UL << lower_l(id));
  }

  static uint64_t offset(T id) noexcept {
      return (lower(id) >> 6UL);
  }


  uint64_t get_cid(T id) {
    const auto cid = chunk_id(id);
    if (cid >= m_cid_size) {
      m_data_bitmap.resize(cid + 1);
      m_data_use_bitmap.resize(cid + 1);
      m_data_vector.resize(cid + 1);
      m_cid_size = cid + 1;
    }
    return cid;
  }

  uint64_t& get_element(T id) {
    const auto cid = chunk_id(id);
    auto& chunk = m_data_bitmap[cid];
    if (chunk.empty()) {
       chunk.resize(chunk_size);
    }
    return chunk[offset(id)];
  }

  void check_vector_to_bitmap_migration(uint64_t cid) {
    if (m_data_vector[cid].size() == max_vector_elem_count) {
      // migrate to bitmap
      auto& chunk = m_data_bitmap[cid];
      if (chunk.empty())
        chunk.resize(chunk_size);

      for (const auto& e : m_data_vector[cid]) {
         chunk[offset(e)] |= bitmask(e);
      }

      m_data_use_bitmap[cid] = true;
      std::vector<uint32_t>().swap(m_data_vector[cid]);
    }
  }

 public:
  IdSetHybrid() = default;

  /**
   * Add the given Id to the set.
   *
   * @param id The Id to set.
   */
  void set(T id) {
    auto cid = get_cid(id);

    if (m_data_use_bitmap[cid]) {
      auto& element = get_element(id);
      element |= bitmask(id);
      ++m_size;
      return;
    }

    m_data_vector[cid].push_back(lower(id));
    ++m_size;

    // Migrate from vector to bitmap if threshold value was reached
    check_vector_to_bitmap_migration(cid);
  }

  /**
   * Is the Id in the set?
   *
   * @param id The Id to check.
   */
  bool get(T id) const noexcept {
    auto cid = chunk_id(id);

    if (cid >= m_cid_size) {
      return false;
    }

    // Use bitmap or vector for lookup?
    if (m_data_use_bitmap[cid]) {

      if (m_data_bitmap[cid].empty()) {
        return false;
      }
      auto p = lower_l(id);
      auto word = m_data_bitmap[cid][offset(id)];
      return (word >> p) & 1UL;
    }

    auto& v = m_data_vector[cid];

    if (v.empty())
      return false;

    auto lower_half = lower(id);

    // check if we're outside vector min/max values
    if (lower_half < v[0] || v[v.size()-1] < lower_half)
      return false;

    //otherwise binary search
    return std::binary_search(v.cbegin(), v.cend(), lower_half);
  }

  /**
   * Sort the internal vector and remove duplicates. Call this
   * before using get()
   */
  void sort_unique() {
    for (auto& v : m_data_vector) {
      if (!std::is_sorted(v.begin(), v.end())) {
        std::sort(v.begin(), v.end());
      }
      v.erase(std::unique(v.begin(), v.end()), v.end());
    }
  }

  /**
   * Is the set empty?
   */
  bool empty() const noexcept { return m_size == 0; }

  T size() const noexcept { return m_size; }

  void clear() {
    std::vector<std::vector<uint64_t>>().swap(m_data_bitmap);
    std::vector<std::vector<uint32_t>>().swap(m_data_vector);
    std::vector<bool>().swap(m_data_use_bitmap);
    m_size = 0;
    m_cid_size = 0;
  }
};



//-----------------------------------------------------------------------------

template < class Object, class TPredicateA, class TPredicateB >
class And_Predicate
{
  public:
    And_Predicate(const TPredicateA& predicate_a_, const TPredicateB& predicate_b_)
        : predicate_a(predicate_a_), predicate_b(predicate_b_) {}
    And_Predicate(TPredicateA&& predicate_a_, TPredicateB&& predicate_b_)
        : predicate_a(std::move(predicate_a_)), predicate_b(std::move(predicate_b_)) {}
    bool match(const Object& obj) const { return (predicate_a.match(obj) && predicate_b.match(obj)); }
    bool match(const Handle< Object >& h) const { return (predicate_a.match(h) && predicate_b.match(h)); }
    bool match(const Handle< Attic< Object > >& h) const { return (predicate_a.match(h) && predicate_b.match(h)); }
    bool is_time_dependent() const { return (predicate_a.is_time_dependent() || predicate_b.is_time_dependent()); }

  private:
    TPredicateA predicate_a;
    TPredicateB predicate_b;
};

template < class Object, class TPredicateA, class TPredicateB >
class Or_Predicate
{
  public:
    Or_Predicate(const TPredicateA& predicate_a_, const TPredicateB& predicate_b_)
        : predicate_a(predicate_a_), predicate_b(predicate_b_) {}
    Or_Predicate(TPredicateA&& predicate_a_, TPredicateB&& predicate_b_)
        : predicate_a(std::move(predicate_a_)), predicate_b(std::move(predicate_b_)) {}
    bool match(const Object& obj) const { return (predicate_a.match(obj) || predicate_b.match(obj)); }
    bool match(const Handle< Object >& h) const { return (predicate_a.match(h) || predicate_b.match(h)); }
    bool match(const Handle< Attic< Object > >& h) const { return (predicate_a.match(h) || predicate_b.match(h)); }
    bool is_time_dependent() const { return (predicate_a.is_time_dependent() || predicate_b.is_time_dependent()); }

  private:
    TPredicateA predicate_a;
    TPredicateB predicate_b;
};

template < class Object, class TPredicateA >
class Not_Predicate
{
  public:
    Not_Predicate(const TPredicateA& predicate_a_)
        : predicate_a(predicate_a_) {}
    Not_Predicate(TPredicateA&& predicate_a_)
        : predicate_a(std::move(predicate_a_)) {}
    bool match(const Object& obj) const { return (!predicate_a.match(obj)); }
    bool match(const Handle< Object >& h) const { return (!predicate_a.match(h)); }
    bool match(const Handle< Attic< Object > >& h) const { return (!predicate_a.match(h)); }
    bool is_time_dependent() const { return predicate_a.is_time_dependent(); }

  private:
    TPredicateA predicate_a;
};

template < class Object >
class Trivial_Predicate
{
  public:
    Trivial_Predicate() = default;
    bool match(const Object& obj) const { return true; }
    bool match(const Handle< Object >& h) const { return true; }
    bool match(const Handle< Attic< Object > >& h) const { return true; }
    bool is_time_dependent() const { return false; }
};

//-----------------------------------------------------------------------------

template < class Object >
class Id_Predicate
{
public:
  Id_Predicate(IdSetHybrid< typename Object::Id_Type::Id_Type> && set) : ids_hybrid(std::move(set)) {}

  Id_Predicate(const std::vector< typename Object::Id_Type >& ids_)
  {
    bool sorting_required = false;
    typename Object::Id_Type::Id_Type previous_id{0};

    for (const auto & id : ids_) {
      const auto current_id = id.val();
      sorting_required |= (current_id <= previous_id);
      ids_hybrid.set(current_id);
      previous_id = current_id;
    }
    if (sorting_required) {
      ids_hybrid.sort_unique();
    }
  }
  bool match(const Object& obj) const
  {
    return (ids_hybrid.get(obj.id.val()));
  }
  bool match(const Handle< Object >& h) const
  {
    return (ids_hybrid.get(h.id().val()));
  }
  bool match(const Handle< Attic< Object > >& h) const
  {
    return (ids_hybrid.get(h.id().val()));
  }
  bool is_time_dependent() const
  {
    return false;
  }

private:
  IdSetHybrid< typename Object::Id_Type::Id_Type> ids_hybrid;
};


//-----------------------------------------------------------------------------

inline bool has_a_child_with_id
    (const Relation_Skeleton& relation, const std::vector< Global_Id_Type >& ids, uint32 type)
{
  for (auto it3(relation.members().begin());
      it3 != relation.members().end(); ++it3)
  {
    if (it3->type == type &&
        std::binary_search(ids.begin(), ids.end(), it3->ref))
      return true;
  }
  return false;
}


inline bool has_a_child_with_id_and_role
    (const Relation_Skeleton& relation, const std::vector< Global_Id_Type >& ids, uint32 type, uint32 role_id)
{
  for (auto it3(relation.members().begin());
      it3 != relation.members().end(); ++it3)
  {
    if (it3->type == type && it3->role == role_id &&
        std::binary_search(ids.begin(), ids.end(), it3->ref))
      return true;
  }
  return false;
}


inline bool has_a_child_with_id
    (const Way_Skeleton& way, const std::vector< int >* pos, const std::vector< Node::Id_Type >& ids)
{
  if (pos)
  {
    auto it3 = pos->begin();
    for (; it3 != pos->end() && *it3 < 0; ++it3)
    {
      if (*it3 + (int)way.nds().size() >= 0 &&
          std::binary_search(ids.begin(), ids.end(), way.nds()[*it3 + way.nds().size()]))
        return true;
    }
    for (; it3 != pos->end(); ++it3)
    {
      if (*it3 > 0 && *it3 < (int)way.nds().size()+1 &&
          std::binary_search(ids.begin(), ids.end(), way.nds()[*it3-1]))
        return true;
    }
  }
  else
  {
    for (auto it3(way.nds().begin());
        it3 != way.nds().end(); ++it3)
    {
      if (std::binary_search(ids.begin(), ids.end(), *it3))
        return true;
    }
  }
  return false;
}

inline bool has_a_child_with_id_hybrid
    (const Relation_Skeleton& relation, const IdSetHybrid< Node::Id_Type::Id_Type>& ids, uint32 type)
{
  for (auto it3(relation.members().begin());
      it3 != relation.members().end(); ++it3)
  {
    if (it3->type == type &&
        ids.get(it3->ref.val()))
      return true;
  }
  return false;
}


inline bool has_a_child_with_id_and_role_hybrid
    (const Relation_Skeleton& relation, const IdSetHybrid< Node::Id_Type::Id_Type> & ids, uint32 type, uint32 role_id)
{
  for (auto it3(relation.members().begin());
      it3 != relation.members().end(); ++it3)
  {
    if (it3->type == type && it3->role == role_id &&
        ids.get(it3->ref.val()))
      return true;
  }
  return false;
}


inline bool has_a_child_with_id_hybrid
    (const Way_Skeleton& way, const std::vector< int >* pos, const IdSetHybrid< Node::Id_Type::Id_Type>& ids)
{
  if (pos)
  {
    auto it3 = pos->begin();
    for (; it3 != pos->end() && *it3 < 0; ++it3)
    {
      if (*it3 + (int)way.nds().size() >= 0 &&
          ids.get(way.nds()[*it3 + way.nds().size()].val()))
        return true;
    }
    for (; it3 != pos->end(); ++it3)
    {
      if (*it3 > 0 && *it3 < (int)way.nds().size()+1 &&
          ids.get(way.nds()[*it3-1].val()))
        return true;
    }
  }
  else
  {
    for (auto it3(way.nds().cbegin());
        it3 != way.nds().cend(); ++it3)
    {
      if (ids.get((*it3).val()))
        return true;
    }
  }
  return false;
}



class Get_Parent_Rels_Predicate
{
public:
  Get_Parent_Rels_Predicate(const std::vector< Global_Id_Type >& ids_, uint32 child_type_)
    : ids(ids_), child_type(child_type_) {}
  bool match(const Relation_Skeleton& obj) const
  { return has_a_child_with_id(obj, ids, child_type); }
  bool match(const Handle< Relation_Skeleton >& h) const
  { return h.has_child_with_id(ids, child_type); }
  bool match(const Handle< Attic< Relation_Skeleton > >& h) const
  { return h.has_child_with_id(ids, child_type); }
  bool is_time_dependent() const { return true; };

private:
  const std::vector< Global_Id_Type >& ids;
  uint32 child_type;
};


class Get_Parent_Rels_Role_Predicate
{
public:
  Get_Parent_Rels_Role_Predicate(const std::vector< Global_Id_Type >& ids_, uint32 child_type_, uint32 role_id_)
    : ids(ids_), child_type(child_type_), role_id(role_id_) {}
  bool match(const Relation_Skeleton& obj) const
  { return has_a_child_with_id_and_role(obj, ids, child_type, role_id); }
  bool match(const Handle< Relation_Skeleton >& h) const
  { return has_a_child_with_id_and_role(h.object_tmp(), ids, child_type, role_id); }
  bool match(const Handle< Attic< Relation_Skeleton > >& h) const
  { return has_a_child_with_id_and_role(h.object_tmp(), ids, child_type, role_id); }
  bool is_time_dependent() const { return true; };

private:
  const std::vector< Global_Id_Type >& ids;
  uint32 child_type;
  uint32 role_id;
};


class Get_Parent_Ways_Predicate
{
public:
  Get_Parent_Ways_Predicate(const std::vector< Node::Id_Type >& ids_, const std::vector< int >* pos_)
    : ids(ids_), pos(pos_) {}
  bool match(const Way_Skeleton& obj) const { return has_a_child_with_id(obj, pos, ids); }
  bool match(const Handle< Way_Skeleton >& h) const { return has_a_child_with_id(h.object_tmp(), pos, ids); }
  bool match(const Handle< Attic< Way_Skeleton > >& h) const { return has_a_child_with_id(h.object_tmp(), pos, ids); }
  bool is_time_dependent() const { return true; };

private:
  const std::vector< Node::Id_Type >& ids;
  const std::vector< int >* pos;
};


class Get_Parent_Ways_Predicate_Hybrid
{
public:
  Get_Parent_Ways_Predicate_Hybrid(IdSetHybrid< Node::Id_Type::Id_Type> && set, const std::vector< int >* pos_) : ids(std::move(set)), pos(pos_) {}
  bool match(const Way_Skeleton& obj) const { return has_a_child_with_id_hybrid(obj, pos, ids); }
  bool match(const Handle< Way_Skeleton >& h) const {

    if (!pos) {
      auto id_functor = [&] (Node_Skeleton::Id_Type id) {
        return (ids.get(id.val()));
      };
      return h.matches_any<Node_Skeleton::Id_Type>(id_functor);
    }

    return has_a_child_with_id_hybrid(h.object_tmp(), pos, ids);
  }

  bool match(const Handle< Attic< Way_Skeleton > >& h) const {

    if (!pos) {
      auto id_functor = [&] (Node_Skeleton::Id_Type id) {
        return (ids.get(id.val()));
      };
      return h.matches_any<Node_Skeleton::Id_Type>(id_functor);
    }

    return has_a_child_with_id_hybrid(h.object_tmp(), pos, ids);
  }
  bool is_time_dependent() const { return true; };

private:
  IdSetHybrid< typename Node::Id_Type::Id_Type> ids;
  const std::vector< int >* pos;
};


//-----------------------------------------------------------------------------


template < typename Attic_Skeleton >
struct Attic_Comparator
{
public:
  bool operator()(const Attic_Skeleton& lhs, const Attic_Skeleton& rhs)
  {
    if (lhs.id < rhs.id)
      return true;
    if (rhs.id < lhs.id)
      return false;
    return (lhs.timestamp < rhs.timestamp);
  }
};


template < class TIndex, class TObject >
void keep_only_least_younger_than
    (std::map< TIndex, std::vector< Attic< TObject > > >& attic_result,
     std::map< TIndex, std::vector< TObject > >& result,
     uint64 timestamp)
{
  std::map< typename TObject::Id_Type, uint64 > timestamp_per_id;

  for (typename std::map< TIndex, std::vector< Attic< TObject > > >::iterator
      it = attic_result.begin(); it != attic_result.end(); ++it)
  {
    std::sort(it->second.begin(), it->second.end(), Attic_Comparator< Attic< TObject > >());
    typename std::vector< Attic< TObject > >::iterator it_from = it->second.begin();
    typename std::vector< Attic< TObject > >::iterator it_to = it->second.begin();
    while (it_from != it->second.end())
    {
      if (it_from->timestamp <= timestamp)
        ++it_from;
      else
      {
        *it_to = *it_from;
        if (timestamp_per_id[it_to->id] == 0 || timestamp_per_id[it_to->id] > it_to->timestamp)
          timestamp_per_id[it_to->id] = it_to->timestamp;
        ++it_from;
        while (it_from != it->second.end() && it_from->id == it_to->id)
          ++it_from;
        ++it_to;
      }
    }
    it->second.erase(it_to, it->second.end());
  }

  for (typename std::map< TIndex, std::vector< Attic< TObject > > >::iterator
      it = attic_result.begin(); it != attic_result.end(); ++it)
  {
    typename std::vector< Attic< TObject > >::iterator it_from = it->second.begin();
    typename std::vector< Attic< TObject > >::iterator it_to = it->second.begin();
    while (it_from != it->second.end())
    {
      if (timestamp_per_id[it_from->id] == it_from->timestamp)
      {
        *it_to = *it_from;
        ++it_to;
      }
      ++it_from;
    }
    it->second.erase(it_to, it->second.end());
  }

  for (typename std::map< TIndex, std::vector< TObject > >::iterator
      it = result.begin(); it != result.end(); ++it)
  {
    typename std::vector< TObject >::iterator it_from = it->second.begin();
    typename std::vector< TObject >::iterator it_to = it->second.begin();
    while (it_from != it->second.end())
    {
      if (timestamp_per_id.find(it_from->id) == timestamp_per_id.end())
      {
        *it_to = *it_from;
        ++it_to;
      }
      ++it_from;
    }
    it->second.erase(it_to, it->second.end());
  }
}


//-----------------------------------------------------------------------------

template < class TIndex, class TObject, class TPredicate >
void filter_items(const TPredicate& predicate, std::map< TIndex, std::vector< TObject > >& data)
{
  for (auto it = data.begin();
  it != data.end(); ++it)
  {
    std::vector< TObject > local_into;
    for (typename std::vector< TObject >::const_iterator iit = it->second.begin();
    iit != it->second.end(); ++iit)
    {
      if (predicate.match(*iit))
	local_into.push_back(*iit);
    }
    it->second.swap(local_into);
  }
}

template< class TIndex, class TObject >
std::vector< typename TObject::Id_Type > filter_for_ids(const std::map< TIndex, std::vector< TObject > >& elems)
{
  std::vector< typename TObject::Id_Type > ids;
  for (auto it = elems.begin();
  it != elems.end(); ++it)
  {
    for (auto iit = it->second.begin();
    iit != it->second.end(); ++iit)
    ids.push_back(iit->id);
  }
  std::sort(ids.begin(), ids.end());

  return ids;
}

//-----------------------------------------------------------------------------


template< typename TObject >
struct Compare_By_Id
{
  bool operator()(const TObject& lhs, const TObject& rhs) { return lhs.id < rhs.id; }
};


template< class TIndex, class TObject >
uint64 indexed_set_union(std::map< TIndex, std::vector< TObject > >& result,
		       const std::map< TIndex, std::vector< TObject > >& summand)
{
  uint64 result_size_increase = 0;

  for (auto it = summand.begin(); it != summand.end(); ++it)
  {
    if (it->second.empty())
      continue;

    std::vector< TObject >& target = result[it->first];
    if (target.empty())
    {
      target = it->second;
      result_size_increase += eval_map_index_size + it->second.size()*eval_elem<TObject>();
      continue;
    }

    if (it->second.size() == 1 && target.size() > 64)
    {
      auto it_target = std::lower_bound(target.begin(), target.end(), it->second.front());
      if (it_target == target.end())
      {
        target.push_back(it->second.front());
        result_size_increase += eval_elem<TObject>();
      }
      else if (!(*it_target == it->second.front()))
      {
        target.insert(it_target, it->second.front());
        result_size_increase += eval_elem<TObject>();
      }
    }
    else
    {
      std::vector< TObject > other;
      other.swap(target);
      std::set_union(it->second.begin(), it->second.end(), other.begin(), other.end(),
                back_inserter(target), Compare_By_Id< TObject >());

      result_size_increase += eval_elem<TObject>() * (target.size() - other.size());
    }
  }

  return result_size_increase;
}

template< class TIndex, class TObject >
uint64 indexed_set_union(std::map< TIndex, std::vector< TObject > >& result,
                         std::map< TIndex, std::vector< TObject > >&& summand)
{
  uint64 result_size_increase = 0;

  for (auto it = summand.begin(); it != summand.end(); )
  {
    if (it->second.empty()) {
      ++it;
      continue;
    }

    auto target = result.find(it->first);
    if (target == result.end() || (target != result.end() && target->second.empty())) {
      if (target != result.end() && target->second.empty()) {
        result.erase(target);
      }
      result_size_increase += eval_map_index_size + it->second.size()*eval_elem<TObject>();
      auto out = it++;
      const auto status = result.insert(summand.extract(out));
      assert(status.inserted == true);
      continue;
    }

    if (it->second.size() == 1 && target->second.size() > 64)
    {
      auto it_target = std::lower_bound(target->second.begin(), target->second.end(), it->second.front());
      if (it_target == target->second.end())
      {
        target->second.push_back(it->second.front());
        result_size_increase += eval_elem<TObject>();
      }
      else if (!(*it_target == it->second.front()))
      {
        target->second.insert(it_target, it->second.front());
        result_size_increase += eval_elem<TObject>();
      }
    }
    else
    {
      std::vector< TObject > other;
      other.swap(target->second);
      std::set_union(it->second.begin(), it->second.end(), other.begin(), other.end(),
                back_inserter(target->second), Compare_By_Id< TObject >());

      result_size_increase += eval_elem<TObject>() * (target->second.size() - other.size());
    }

    ++it;
  }

  return result_size_increase;
}

//-----------------------------------------------------------------------------

template< class TIndex, class TObject >
void indexed_set_difference(std::map< TIndex, std::vector< TObject > >& result,
                            const std::map< TIndex, std::vector< TObject > >& to_substract)
{
  for (auto it = to_substract.begin(); it != to_substract.end(); ++it)
  {
    std::vector< TObject > other;
    other.swap(result[it->first]);
    std::sort(other.begin(), other.end());
    std::set_difference(other.begin(), other.end(), it->second.begin(), it->second.end(),
                   back_inserter(result[it->first]));
  }
}

//-----------------------------------------------------------------------------

/* Returns for the given set of ids the set of corresponding indexes.
 * For ids where the timestamp is zero, only the current index is returned.
 * For ids where the timestamp is nonzero, all attic indexes are also returned.
 * The function requires that the ids are sorted ascending by id.
 */
template< typename Index, typename Skeleton >
std::pair< std::vector< Index >, std::vector< Index > > get_indexes
    (const std::vector< typename Skeleton::Id_Type >& ids, Resource_Manager& rman)
{
  std::pair< std::vector< Index >, std::vector< Index > > result;

  Random_File< typename Skeleton::Id_Type, Index > current(rman.get_transaction()->random_index
      (current_skeleton_file_properties< Skeleton >()));
  for (auto it = ids.begin(); it != ids.end(); ++it)
    result.first.push_back(current.get(it->val()));

  std::sort(result.first.begin(), result.first.end());
  result.first.erase(std::unique(result.first.begin(), result.first.end()), result.first.end());

  if (rman.get_desired_timestamp() != NOW)
  {
    Random_File< typename Skeleton::Id_Type, Index > attic_random(rman.get_transaction()->random_index
        (attic_skeleton_file_properties< Skeleton >()));
    std::set< typename Skeleton::Id_Type > idx_list_ids;
    for (auto it = ids.begin(); it != ids.end(); ++it)
    {
      if (attic_random.get(it->val()).val() == 0)
        ;
      else if (attic_random.get(it->val()) == 0xff)
        idx_list_ids.insert(it->val());
      else
        result.second.push_back(attic_random.get(it->val()));
    }

    Block_Backend< typename Skeleton::Id_Type, Index > idx_list_db
        (rman.get_transaction()->data_index(attic_idx_list_properties< Skeleton >()));

    for (const auto & it : idx_list_db.as_discrete(idx_list_ids))
      result.second.push_back(it.object_tmp());

    std::sort(result.second.begin(), result.second.end());
    result.second.erase(std::unique(result.second.begin(), result.second.end()), result.second.end());
  }

  return result;
}


#endif
