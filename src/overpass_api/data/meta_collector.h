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

#ifndef DE__OSM3S___OVERPASS_API__STATEMENTS__META_COLLECTOR_H
#define DE__OSM3S___OVERPASS_API__STATEMENTS__META_COLLECTOR_H

#include <map>
#include <memory>
#include <optional>
#include <set>
#include <vector>

#include "../../template_db/block_backend.h"
#include "../../template_db/random_file.h"
#include "../../template_db/transaction.h"
#include "../core/datatypes.h"
#include "../core/settings.h"
#include "filenames.h"

template <typename Id_Type >
class AlwaysTrueFunctor {

public:
  constexpr bool operator()(const OSM_Element_Metadata_Skeleton< Id_Type > & obj) const {
    return true;
  }
};


template< typename Index, typename Id_Type, class Functor = AlwaysTrueFunctor< Id_Type > >
struct Meta_Collector
{
public:
  template< typename Object >
  Meta_Collector(const std::map< Index, std::vector< Object > >& items,
      Transaction& transaction, const File_Properties* meta_file_prop = 0);

  Meta_Collector(const std::set< std::pair< Index, Index > >& used_ranges,
      Transaction& transaction, const File_Properties* meta_file_prop = 0);

  template< typename Object >
  Meta_Collector(const std::map< Index, std::vector< Object > >& items,
      Transaction& transaction, Functor functor, const File_Properties* meta_file_prop = 0);

  Meta_Collector(const std::set< std::pair< Index, Index > >& used_ranges,
      Transaction& transaction, Functor functor,
      const File_Properties* meta_file_prop = 0);

  Meta_Collector(const Meta_Collector&) = delete;
  Meta_Collector& operator=(const Meta_Collector& a) = delete;

  void reset();
  const OSM_Element_Metadata_Skeleton< Id_Type >* get
      (const Index& index, Id_Type ref);
  const OSM_Element_Metadata_Skeleton< Id_Type >* get
      (const Index& index, Id_Type ref, timestamp_t timestamp);

  ~Meta_Collector() = default;

private:
  std::set< std::pair< Index, Index > > used_ranges;
  std::unique_ptr< Block_Backend< Index, OSM_Element_Metadata_Skeleton< Id_Type > > > meta_db;
  std::unique_ptr< typename Block_Backend< Index, OSM_Element_Metadata_Skeleton< Id_Type > >
      ::Range_Iterator > range_it;
  std::optional<Index> current_index;
  std::optional<Index> last_index;
  std::vector< OSM_Element_Metadata_Skeleton< Id_Type > > current_objects;

  Functor m_functor;

  void update_current_objects(const Index&);
  bool is_sorted = false;
  uint32_t remaining_linear_reads = 0;

  constexpr static uint32_t MAX_LINEAR_READS = 10;

};


template< typename Index, typename Object >
struct Attic_Meta_Collector
{
public:
  Attic_Meta_Collector(const std::map< Index, std::vector< Attic< Object > > >& items,
                       Transaction& transaction, bool turn_on);

  const OSM_Element_Metadata_Skeleton< typename Object::Id_Type >* get
      (const Index& index, typename Object::Id_Type ref, timestamp_t timestamp = NOW);

private:
  Meta_Collector< Index, typename Object::Id_Type > current;
  Meta_Collector< Index, typename Object::Id_Type > attic;
};


/** Implementation --------------------------------------------------------- */

template< typename Index, typename Object >
void generate_range_query
  (std::set< std::pair< Index, Index > >& ranges,
   const std::map< Index, std::vector< Object > >& items)
{
  std::set< std::pair< Index, Index > > temp_ranges;

  ranges.clear();

  for (auto it(items.begin()); it != items.end(); ++it) {
    temp_ranges.insert(std::make_pair(it->first, inc(it->first)));
  }

  if (temp_ranges.empty()) {
    return;
  }

  // Merge ranges
  auto it = temp_ranges.begin();
  Index last_first = it->first;
  Index last_second = it->second;
  ++it;
  for (; it != temp_ranges.end(); ++it)
  {
    if (last_second < it->first)
    {
      ranges.insert(std::make_pair(last_first, last_second));
      last_first = it->first;
    }
    if (last_second < it->second)
      last_second = it->second;
  }
  ranges.insert(std::make_pair(last_first, last_second));
}


template< typename Index, typename Id_Type, class Functor >
template< typename Object >
Meta_Collector< Index, Id_Type, Functor >::Meta_Collector
    (const std::map< Index, std::vector< Object > >& items,
     Transaction& transaction, const File_Properties* meta_file_prop)
{
  if (!meta_file_prop)
    return;

  generate_range_query(used_ranges, items);
  meta_db = std::make_unique< Block_Backend< Index, OSM_Element_Metadata_Skeleton< Id_Type > > >
      (transaction.data_index(meta_file_prop));

  reset();
}


template< typename Index, typename Id_Type, class Functor >
Meta_Collector< Index, Id_Type, Functor >::Meta_Collector
    (const std::set< std::pair< Index, Index > >& used_ranges_,
     Transaction& transaction, const File_Properties* meta_file_prop)
  : used_ranges(used_ranges_)
{
  if (!meta_file_prop)
    return;

  meta_db = std::make_unique< Block_Backend< Index, OSM_Element_Metadata_Skeleton< Id_Type > > >
      (transaction.data_index(meta_file_prop));

  reset();
}


template< typename Index, typename Id_Type, class Functor >
template< typename Object >
Meta_Collector< Index, Id_Type, Functor >::Meta_Collector
    (const std::map< Index, std::vector< Object > >& items,
     Transaction& transaction,  Functor functor,
     const File_Properties* meta_file_prop) : m_functor(functor)
{
  if (!meta_file_prop)
    return;

  generate_range_query(used_ranges, items);
  meta_db = std::make_unique< Block_Backend< Index, OSM_Element_Metadata_Skeleton< Id_Type > > >
      (transaction.data_index(meta_file_prop));

  reset();
}

template< typename Index, typename Id_Type, class Functor >
Meta_Collector< Index, Id_Type, Functor >::Meta_Collector
    (const std::set< std::pair< Index, Index > >& used_ranges_,
     Transaction& transaction, Functor functor,
     const File_Properties* meta_file_prop) :
     used_ranges(used_ranges_), m_functor(functor)

{
  if (!meta_file_prop)
    return;

  meta_db = std::make_unique< Block_Backend< Index, OSM_Element_Metadata_Skeleton< Id_Type > > >
      (transaction.data_index(meta_file_prop));

  reset();
}


template< typename Index, typename Id_Type, class Functor >
void Meta_Collector< Index, Id_Type, Functor >::reset()
{
  if (!meta_db)
    return;

  range_it.reset(nullptr);

  current_index.reset();
  last_index.reset();

  last_index = Index(used_ranges.begin()->first);

  range_it = std::make_unique< typename Block_Backend< Index, OSM_Element_Metadata_Skeleton< Id_Type > >
      ::Range_Iterator >(meta_db->range_begin(
          typename Ranges< Index >::Iterator(used_ranges.begin()),
          typename Ranges< Index >::Iterator(used_ranges.end())));

  if (!(*range_it == meta_db->range_end()))
  {
    current_index = Index(range_it->index());
  }
  while (!(*range_it == meta_db->range_end()) && (*current_index == range_it->index()))
  {
    if (m_functor(range_it->object())) {
      range_it->handle().add_element(current_objects);
    }
    ++(*range_it);
  }

  remaining_linear_reads = MAX_LINEAR_READS;
  is_sorted = false;
}


template< typename Index, typename Id_Type, class Functor >
void Meta_Collector< Index, Id_Type, Functor >::update_current_objects(const Index& index)
{
  {
    std::vector< OSM_Element_Metadata_Skeleton< Id_Type > > new_objects{};
    current_objects.swap(new_objects);
  }

  if (last_index)
    last_index = index;

  if (range_it)
  {
    while (!(*range_it == meta_db->range_end()) && (Index(range_it->index_handle().id()) < index)) {
      range_it->skip_current_index();
      ++(*range_it);
    }
    if (!(*range_it == meta_db->range_end()))
      current_index = Index(range_it->index_handle().id());
    while (!(*range_it == meta_db->range_end()) && (*current_index == Index(range_it->index_handle().id())))
    {
      if (m_functor(range_it->object())) {
        range_it->handle().add_element(current_objects);
      }
      ++(*range_it);
    }
  }
  remaining_linear_reads = MAX_LINEAR_READS;
  is_sorted = false;
}



template< typename Index, typename Id_Type, class Functor >
const OSM_Element_Metadata_Skeleton< Id_Type >* Meta_Collector< Index, Id_Type, Functor >::get
    (const Index& index, Id_Type ref)
{
  if (!meta_db)
    return nullptr;

  if (current_index && index < *last_index) {
    reset();
  }
  if (current_index && *current_index < index) {
    update_current_objects(index);
  }

  // lazy sorting: sort current_objects, if we hit the max. number of linear reads
  if (!is_sorted && remaining_linear_reads == 0) {
    std::sort(current_objects.begin(), current_objects.end());
    is_sorted = true;
  }

  typename std::vector< OSM_Element_Metadata_Skeleton< Id_Type > >::const_iterator it;

  if (is_sorted) {
    it = std::lower_bound(current_objects.begin(), current_objects.end(),
               OSM_Element_Metadata_Skeleton< Id_Type >(ref));
  }
  else
  {
    --remaining_linear_reads;
    it = std::find_if(current_objects.begin(), current_objects.end(),
        [ref](const OSM_Element_Metadata_Skeleton< Id_Type > el)
        { return el.ref == ref; });
  }

  if (it != current_objects.end() && it->ref == ref)
    return &*it;
  else
    return nullptr;
}


template< typename Index, typename Id_Type, class Functor >
const OSM_Element_Metadata_Skeleton< Id_Type >* Meta_Collector< Index, Id_Type, Functor >::get
    (const Index& index, Id_Type ref, timestamp_t timestamp)
{
  if (!meta_db)
    return nullptr;

  if (current_index && index < *last_index)
    reset();
  if (current_index && *current_index < index)
    update_current_objects(index);

  if (!is_sorted) {
    std::sort(current_objects.begin(), current_objects.end());
    is_sorted = true;
  }

  auto it
      = std::lower_bound(current_objects.begin(), current_objects.end(),
                         OSM_Element_Metadata_Skeleton< Id_Type >(ref, timestamp));
  if (it == current_objects.begin())
    return nullptr;
  --it;
  if (it->ref == ref)
    return &*it;
  else
    return nullptr;
}


template< typename Index, typename Object >
Attic_Meta_Collector< Index, Object >::Attic_Meta_Collector(
    const std::map< Index, std::vector< Attic< Object > > >& items, Transaction& transaction, bool turn_on)
    : current(items, transaction, turn_on ? current_meta_file_properties< Object >() : nullptr),
    attic(items, transaction, turn_on ? attic_meta_file_properties< Object >() : nullptr)
{}


template< typename Index, typename Object >
const OSM_Element_Metadata_Skeleton< typename Object::Id_Type >* Attic_Meta_Collector< Index, Object >::get(
    const Index& index, typename Object::Id_Type ref, timestamp_t timestamp)
{
  const OSM_Element_Metadata_Skeleton< typename Object::Id_Type >* meta
      = current.get(index, ref, timestamp);
  if (meta)
    return meta;
  return attic.get(index, ref, timestamp);
}


#endif
