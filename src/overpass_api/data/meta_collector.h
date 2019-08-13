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
  constexpr bool operator()(const OSM_Element_Metadata_Skeleton< Id_Type > & obj) {
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

  void reset();
  const OSM_Element_Metadata_Skeleton< Id_Type >* get
      (const Index& index, Id_Type ref);
  const OSM_Element_Metadata_Skeleton< Id_Type >* get
      (const Index& index, Id_Type ref, uint64 timestamp);

  ~Meta_Collector()
  {
    if (meta_db)
    {
      delete db_it;
      delete meta_db;
    }

    if (current_index)
      delete current_index;
  }

private:
  std::set< Index > used_indices;
  std::set< std::pair< Index, Index > > used_ranges;
  Block_Backend< Index, OSM_Element_Metadata_Skeleton< Id_Type > >* meta_db;
  typename Block_Backend< Index, OSM_Element_Metadata_Skeleton< Id_Type > >
      ::Discrete_Iterator* db_it;
  typename Block_Backend< Index, OSM_Element_Metadata_Skeleton< Id_Type > >
      ::Range_Iterator* range_it;
  Index* current_index;
  std::vector< OSM_Element_Metadata_Skeleton< Id_Type > > current_objects;

  Functor m_functor;

  void update_current_objects(const Index&);
};


template< typename Index, typename Object >
struct Attic_Meta_Collector
{
public:
  Attic_Meta_Collector(const std::map< Index, std::vector< Attic< Object > > >& items,
                       Transaction& transaction, bool turn_on);

  const OSM_Element_Metadata_Skeleton< typename Object::Id_Type >* get
      (const Index& index, typename Object::Id_Type ref, uint64 timestamp = NOW);

private:
  Meta_Collector< Index, typename Object::Id_Type > current;
  Meta_Collector< Index, typename Object::Id_Type > attic;
};


/** Implementation --------------------------------------------------------- */


template<typename Id_Type>
OSM_Element_Metadata_Skeleton< Id_Type > get_elem(const void * a) {
  return OSM_Element_Metadata_Skeleton< Id_Type >(a);
}


template< typename Index, typename Object >
void generate_index_query
  (std::set< Index >& indices,
   const std::map< Index, std::vector< Object > >& items)
{
  for (typename std::map< Index, std::vector< Object > >::const_iterator
      it(items.begin()); it != items.end(); ++it)
    indices.insert(it->first);
}


template< typename Index, typename Id_Type, class Functor >
template< typename Object >
Meta_Collector< Index, Id_Type, Functor >::Meta_Collector
    (const std::map< Index, std::vector< Object > >& items,
     Transaction& transaction, const File_Properties* meta_file_prop)
  : meta_db(0), db_it(0), range_it(0), current_index(0)
{
  if (!meta_file_prop)
    return;

  generate_index_query(used_indices, items);
  meta_db = new Block_Backend< Index, OSM_Element_Metadata_Skeleton< Id_Type > >
      (transaction.data_index(meta_file_prop));

  reset();
}


template< typename Index, typename Id_Type, class Functor >
Meta_Collector< Index, Id_Type, Functor >::Meta_Collector
    (const std::set< std::pair< Index, Index > >& used_ranges_,
     Transaction& transaction, const File_Properties* meta_file_prop)
  : used_ranges(used_ranges_), meta_db(0), db_it(0), range_it(0), current_index(0)
{
  if (!meta_file_prop)
    return;

  meta_db = new Block_Backend< Index, OSM_Element_Metadata_Skeleton< Id_Type > >
      (transaction.data_index(meta_file_prop));

  reset();
}


template< typename Index, typename Id_Type, class Functor >
template< typename Object >
Meta_Collector< Index, Id_Type, Functor >::Meta_Collector
    (const std::map< Index, std::vector< Object > >& items,
     Transaction& transaction,  Functor functor,
     const File_Properties* meta_file_prop) :
     meta_db(0), db_it(0), range_it(0), current_index(0), m_functor(functor)
{
  if (!meta_file_prop)
    return;

  generate_index_query(used_indices, items);
  meta_db = new Block_Backend< Index, OSM_Element_Metadata_Skeleton< Id_Type > >
      (transaction.data_index(meta_file_prop));

  reset();
}

template< typename Index, typename Id_Type, class Functor >
Meta_Collector< Index, Id_Type, Functor >::Meta_Collector
    (const std::set< std::pair< Index, Index > >& used_ranges_,
     Transaction& transaction, Functor functor,
     const File_Properties* meta_file_prop) :
     used_ranges(used_ranges_), meta_db(0), db_it(0), range_it(0), current_index(0),
     m_functor(functor)

{
  if (!meta_file_prop)
    return;

  meta_db = new Block_Backend< Index, OSM_Element_Metadata_Skeleton< Id_Type > >
      (transaction.data_index(meta_file_prop));

  reset();
}


template< typename Index, typename Id_Type, class Functor >
void Meta_Collector< Index, Id_Type, Functor >::reset()
{
  if (!meta_db)
    return;

  if (db_it)
    delete db_it;
  if (range_it)
    delete range_it;
  if (current_index)
  {
    delete current_index;
    current_index = 0;
  }

  if (used_ranges.empty())
  {
    db_it = new typename Block_Backend< Index, OSM_Element_Metadata_Skeleton< Id_Type > >
        ::Discrete_Iterator(meta_db->discrete_begin(used_indices.begin(), used_indices.end()));

    if (!(*db_it == meta_db->discrete_end()))
    {
      if (current_index)
        delete current_index;
      current_index = new Index(db_it->index());
    }
    while (!(*db_it == meta_db->discrete_end()) && (*current_index == db_it->index()))
    {
      auto obj = db_it->apply_func( get_elem< Id_Type > );

      if (m_functor(obj))
        current_objects.push_back(std::move(obj));
      ++(*db_it);
    }
  }
  else
  {
    range_it = new typename Block_Backend< Index, OSM_Element_Metadata_Skeleton< Id_Type > >
        ::Range_Iterator(meta_db->range_begin(
	    Default_Range_Iterator< Index >(used_ranges.begin()),
	    Default_Range_Iterator< Index >(used_ranges.end())));

    if (!(*range_it == meta_db->range_end()))
    {
      if (current_index)
        delete current_index;
      current_index = new Index(range_it->index());
    }
    while (!(*range_it == meta_db->range_end()) && (*current_index == range_it->index()))
    {
      auto obj = range_it->apply_func( get_elem< Id_Type > );
      if (m_functor(obj))
        current_objects.push_back(std::move(obj));
      ++(*range_it);
    }
  }
  std::sort(current_objects.begin(), current_objects.end());
}


template< typename Index, typename Id_Type, class Functor >
void Meta_Collector< Index, Id_Type, Functor >::update_current_objects(const Index& index)
{
  {
    std::vector< OSM_Element_Metadata_Skeleton< Id_Type > > new_objects{};
    current_objects.swap(new_objects);
  }

  if (db_it)
  {
    while (!(*db_it == meta_db->discrete_end()) && (db_it->index() < index))
      ++(*db_it);
    if (!(*db_it == meta_db->discrete_end()))
      *current_index = db_it->index();
    while (!(*db_it == meta_db->discrete_end()) && (*current_index == db_it->index()))
    {
      auto obj = db_it->apply_func( get_elem< Id_Type > );

      if (m_functor(obj))
        current_objects.push_back(std::move(obj));
      ++(*db_it);
    }
  }
  else if (range_it)
  {
    while (!(*range_it == meta_db->range_end()) && (range_it->index() < index))
      ++(*range_it);
    if (!(*range_it == meta_db->range_end()))
      *current_index = range_it->index();
    while (!(*range_it == meta_db->range_end()) && (*current_index == range_it->index()))
    {
      auto obj = range_it->apply_func( get_elem< Id_Type > );
      if (m_functor(obj))
        current_objects.push_back(std::move(obj));
      ++(*range_it);
    }
  }

  std::sort(current_objects.begin(), current_objects.end());
//  const auto last = std::unique(current_objects.begin(), current_objects.end());
//  current_objects.erase(last, current_objects.end());
}



template< typename Index, typename Id_Type, class Functor >
const OSM_Element_Metadata_Skeleton< Id_Type >* Meta_Collector< Index, Id_Type, Functor >::get
    (const Index& index, Id_Type ref)
{
  if (!meta_db)
    return 0;

  if ((current_index) && (*current_index < index))
    update_current_objects(index);

  typename std::vector< OSM_Element_Metadata_Skeleton< Id_Type > >::iterator it
      = std::lower_bound(current_objects.begin(), current_objects.end(),
             OSM_Element_Metadata_Skeleton< Id_Type >(ref));
  if (it != current_objects.end() && it->ref == ref)
    return &*it;
  else
    return 0;
}


template< typename Index, typename Id_Type, class Functor >
const OSM_Element_Metadata_Skeleton< Id_Type >* Meta_Collector< Index, Id_Type, Functor >::get
    (const Index& index, Id_Type ref, uint64 timestamp)
{
  if (!meta_db)
    return 0;

  if ((current_index) && (*current_index < index))
    update_current_objects(index);

  typename std::vector< OSM_Element_Metadata_Skeleton< Id_Type > >::iterator it
      = std::lower_bound(current_objects.begin(), current_objects.end(),
                         OSM_Element_Metadata_Skeleton< Id_Type >(ref, timestamp));
  if (it == current_objects.begin())
    return 0;
  --it;
  if (it->ref == ref)
    return &*it;
  else
    return 0;
}


template< typename Index, typename Object >
Attic_Meta_Collector< Index, Object >::Attic_Meta_Collector(
    const std::map< Index, std::vector< Attic< Object > > >& items, Transaction& transaction, bool turn_on)
    : current(items, transaction, turn_on ? current_meta_file_properties< Object >() : 0),
    attic(items, transaction, turn_on ? attic_meta_file_properties< Object >() : 0)
{}


template< typename Index, typename Object >
const OSM_Element_Metadata_Skeleton< typename Object::Id_Type >* Attic_Meta_Collector< Index, Object >::get(
    const Index& index, typename Object::Id_Type ref, uint64 timestamp)
{
  const OSM_Element_Metadata_Skeleton< typename Object::Id_Type >* meta
      = current.get(index, ref, timestamp);
  if (meta)
    return meta;
  return attic.get(index, ref, timestamp);
}


#endif
