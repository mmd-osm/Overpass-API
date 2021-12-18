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

#ifndef DE__OSM3S___OVERPASS_API__DATA__TAG_STORE_H
#define DE__OSM3S___OVERPASS_API__DATA__TAG_STORE_H


#include "filenames.h"
#include "../core/datatypes.h"
#include "../data/abstract_processing.h"
#include "../../template_db/block_backend.h"
#include "../../template_db/transaction.h"

#include <map>
#include <string>
#include <vector>



template< >
struct Range_Idx_Assessor<Tag_Index_Local, Ranges< Tag_Index_Local >::Iterator >
{
  Range_Idx_Assessor(const Ranges< Tag_Index_Local >::Iterator& index_it_, const Ranges< Tag_Index_Local >::Iterator& index_end_)
      : index_it(index_it_), index_end(index_end_) {}

  bool is_relevant(Handle < Tag_Index_Local > & handle)
  {
    while (index_it != index_end && !(handle < index_it.upper_bound()))
      ++index_it;
    return index_it != index_end && !(handle < index_it.lower_bound()) && handle < index_it.upper_bound();
  }

private:
  Ranges< Tag_Index_Local >::Iterator index_it;
  Ranges< Tag_Index_Local >::Iterator index_end;
};


template< typename Index, typename Object >
class Tag_Store
{
public:
  Tag_Store(Transaction& transaction);
  ~Tag_Store();

  void prefetch_all(const std::map< Index, std::vector< Object > >& elems);
  void prefetch_chunk(const std::map< Index, std::vector< Object > >& elems,
      typename Object::Id_Type lower_id_bound, typename Object::Id_Type upper_id_bound);
  void prefetch_all(const std::map< Index, std::vector< Attic< Object > > >& elems);
  void prefetch_chunk(const std::map< Index, std::vector< Attic< Object > > >& elems,
      typename Object::Id_Type lower_id_bound, typename Object::Id_Type upper_id_bound);

  const std::vector< std::pair< std::string, std::string > >* get(const Index& index, const Object& elem);

private:
  std::map< typename Object::Id_Type, std::vector< std::pair< std::string, std::string > > > tags_by_id;
  Transaction* transaction;
  bool use_index;
  Index stored_index;
  Ranges< Tag_Index_Local > ranges;
  IdSetHybrid<typename Object::Id_Type::Id_Type> ids;
  std::map< uint32, std::vector< Attic< typename Object::Id_Type > > > attic_ids_by_coarse;
  Block_Backend< Tag_Index_Local, typename Object::Id_Type >* items_db;
  typename Block_Backend< Tag_Index_Local, typename Object::Id_Type >::Range_Iterator* tag_it;
  Block_Backend< Tag_Index_Local, Attic< typename Object::Id_Type > >* attic_items_db;
  typename Block_Backend< Tag_Index_Local, Attic< typename Object::Id_Type > >::Range_Iterator* attic_tag_it;
};


template< >
class Tag_Store< Uint31_Index, Derived_Structure >
{
public:
  Tag_Store(Transaction& transaction) {}
  Tag_Store() = default;

  void prefetch_all(const std::map< Uint31_Index, std::vector< Derived_Structure > >& elems) {}
  void prefetch_chunk(const std::map< Uint31_Index, std::vector< Derived_Structure > >& elems,
      Derived_Structure::Id_Type lower_id_bound, Derived_Structure::Id_Type upper_id_bound) {}

  const std::vector< std::pair< std::string, std::string > >* get(
      const Uint31_Index& index, const Derived_Structure& elem) const { return &elem.tags; }
};


template< class Id_Type >
void collect_attic_tags
  (std::map< Id_Type, std::vector< std::pair< std::string, std::string > > >& tags_by_id,
   const Block_Backend< Tag_Index_Local, Id_Type >& current_items_db,
   typename Block_Backend< Tag_Index_Local, Id_Type >::Range_Iterator& current_tag_it,
   const Block_Backend< Tag_Index_Local, Attic< Id_Type > >& attic_items_db,
   typename Block_Backend< Tag_Index_Local, Attic< Id_Type > >::Range_Iterator& attic_tag_it,
   const std::vector< Attic< Id_Type > >& id_vec, uint32 coarse_index)
{
  std::map< Attic< Id_Type >, std::vector< std::pair< std::string, std::string > > > found_tags;

  // Collect all id-matched tag information from the current tags
  while ((!(current_tag_it == current_items_db.range_end())) &&
      (((current_tag_it.index().index) & 0x7fffff00) < coarse_index))
    ++current_tag_it;
  while ((!(current_tag_it == current_items_db.range_end())) &&
      (((current_tag_it.index().index) & 0x7fffff00) == coarse_index))
  {
    auto it_id = std::lower_bound(id_vec.begin(), id_vec.end(), Attic< Id_Type >(current_tag_it.handle().id(), 0ull));
    auto it_id_end = std::upper_bound(id_vec.begin(), id_vec.end(), Attic< Id_Type >
            (current_tag_it.handle().id(), 0xffffffffffffffffull));
    if (it_id != it_id_end)
      found_tags[Attic< Id_Type >(current_tag_it.handle().id(), 0xffffffffffffffffull)].push_back
          (std::make_pair(current_tag_it.index().key, current_tag_it.index().value));
    ++current_tag_it;
  }

  // Collect all id-matched tag information that is younger than the respective timestamp from the attic tags
  while ((!(attic_tag_it == attic_items_db.range_end())) &&
      (((attic_tag_it.index().index) & 0x7fffff00) < coarse_index))
    ++attic_tag_it;
  while ((!(attic_tag_it == attic_items_db.range_end())) &&
      (((attic_tag_it.index().index) & 0x7fffff00) == coarse_index))
  {
    auto it_id = std::lower_bound(id_vec.begin(), id_vec.end(), Attic< Id_Type >(attic_tag_it.handle().id(), 0ull));
    auto it_id_end = std::upper_bound(id_vec.begin(), id_vec.end(), attic_tag_it.object());
    if (it_id != it_id_end)
      found_tags[attic_tag_it.object()].push_back
          (std::make_pair(attic_tag_it.index().key, attic_tag_it.index().value));
    ++attic_tag_it;
  }

  // Actually take for each object and key of the multiple versions only the oldest valid version
  for (typename std::map< Attic< Id_Type >, std::vector< std::pair< std::string, std::string > > >
          ::const_iterator
      it = found_tags.begin(); it != found_tags.end(); ++it)
  {
    auto it_id = std::lower_bound(id_vec.begin(), id_vec.end(), Attic< Id_Type >(it->first, 0ull));
    auto it_id_end = std::upper_bound(id_vec.begin(), id_vec.end(), it->first);
    while (it_id != it_id_end)
    {
      std::vector< std::pair< std::string, std::string > >& obj_vec = tags_by_id[*it_id];
      auto last_added_it = it->second.end();
      for (auto it_source = it->second.begin(); it_source != it->second.end(); ++it_source)
      {
        if (last_added_it != it->second.end())
        {
          if (last_added_it->first == it_source->first)
          {
            obj_vec.push_back(*it_source);
            continue;
          }
          else
            last_added_it = it->second.end();
        }

        std::vector< std::pair< std::string, std::string > >::const_iterator it_obj = obj_vec.begin();
        for (; it_obj != obj_vec.end(); ++it_obj)
        {
          if (it_obj->first == it_source->first)
            break;
        }
        if (it_obj == obj_vec.end())
        {
          obj_vec.push_back(*it_source);
          last_added_it = it_source;
        }
      }
      ++it_id;
    }
  }

  // Remove empty tags. They are placeholders for tags added later than each timestamp in question.
  for (auto it_obj = tags_by_id.begin(); it_obj != tags_by_id.end(); ++it_obj)
  {
    for (typename std::vector< std::pair< std::string, std::string > >::size_type
        i = 0; i < it_obj->second.size(); )
    {
      if (it_obj->second[i].second == void_tag_value())
      {
        it_obj->second[i] = it_obj->second.back();
        it_obj->second.pop_back();
      }
      else
        ++i;
    }
  }
}


template< class Id_Type >
void collect_attic_tags
  (std::map< Id_Type, std::vector< std::pair< std::string, std::string > > >& tags_by_id,
   const Block_Backend< Tag_Index_Local, Id_Type >& current_items_db,
   typename Block_Backend< Tag_Index_Local, Id_Type >::Range_Iterator& current_tag_it,
   const Block_Backend< Tag_Index_Local, Attic< Id_Type > >& attic_items_db,
   typename Block_Backend< Tag_Index_Local, Attic< Id_Type > >::Range_Iterator& attic_tag_it,
   std::map< uint32, std::vector< Attic< Id_Type > > >& ids_by_coarse,
   uint32 coarse_index,
   Id_Type lower_id_bound, Id_Type upper_id_bound)
{
  std::vector< Attic< Id_Type > > id_vec = ids_by_coarse[coarse_index];

  if (!(upper_id_bound == Id_Type()))
  {
    auto it_id = std::lower_bound(id_vec.begin(), id_vec.end(),
                           Attic< Id_Type >(upper_id_bound, 0ull));
    id_vec.erase(it_id, id_vec.end());
  }
  if (!(lower_id_bound == Id_Type()))
  {
    auto it_id = std::lower_bound(id_vec.begin(), id_vec.end(),
                           Attic< Id_Type >(lower_id_bound, 0ull));
    id_vec.erase(id_vec.begin(), it_id);
  }

  collect_attic_tags< Id_Type >(tags_by_id, current_items_db, current_tag_it, attic_items_db, attic_tag_it,
      id_vec, coarse_index);
}



template< class Id_Type >
void collect_tags_new
  (std::map< Id_Type, std::vector< std::pair< std::string, std::string > > >& tags_by_id,
   const Block_Backend< Tag_Index_Local, Id_Type >& items_db,
   typename Block_Backend< Tag_Index_Local, Id_Type >::Range_Iterator& tag_it,
   const IdSetHybrid<typename Id_Type::Id_Type>& ids, uint32 coarse_index)
{
  while ((!(tag_it == items_db.range_end())) &&
      (((tag_it.index_handle().get_index()) & 0x7fffff00) < coarse_index))
    ++tag_it;
  while ((!(tag_it == items_db.range_end())) &&
      (((tag_it.index_handle().get_index()) & 0x7fffff00) == coarse_index))
  {
    Id_Type current(tag_it.handle().id());     // avoid creating a new object instance via object()
    if (ids.get(current.val()))
    {
      auto elem = tag_it.index_handle().get_element();
      tags_by_id[current].push_back
          (std::make_pair(std::move(elem.key), std::move(elem.value)));
    }
    ++tag_it;
  }
}


template< class Id_Type >
void collect_tags_framed_new
  (std::map< Id_Type, std::vector< std::pair< std::string, std::string > > >& tags_by_id,
   const Block_Backend< Tag_Index_Local, Id_Type >& items_db,
   typename Block_Backend< Tag_Index_Local, Id_Type >::Range_Iterator& tag_it,
   IdSetHybrid<typename Id_Type::Id_Type>& ids,
   uint32 coarse_index,
   Id_Type lower_id_bound, Id_Type upper_id_bound)
{
  while ((!(tag_it == items_db.range_end())) &&
      (((tag_it.index_handle().get_index()) & 0x7fffff00) < coarse_index))
    ++tag_it;
  while ((!(tag_it == items_db.range_end())) &&
      (((tag_it.index_handle().get_index()) & 0x7fffff00) == coarse_index))
  {
    Id_Type current(tag_it.handle().id());     // avoid creating a new object instance via object()

    if (!(current < lower_id_bound) &&
      (current < upper_id_bound) &&
      ids.get(current.val()))
    {
      auto elem = tag_it.index_handle().get_element();
      tags_by_id[current].push_back
          (std::make_pair(std::move(elem.key), std::move(elem.value)));
    }
    ++tag_it;
  }
}

template< class TIndex, class TObject >
IdSetHybrid<typename TObject::Id_Type::Id_Type> generate_ids
  (std::set< TIndex >& coarse_indices,
   const std::map< TIndex, std::vector< TObject > >& items)
{
  IdSetHybrid<typename TObject::Id_Type::Id_Type> ids;

  for (auto it(items.begin()); it != items.end(); ++it)
  {
    coarse_indices.insert(it->first.val() & 0x7fffff00);

    for (auto it2(it->second.begin()); it2 != it->second.end(); ++it2)
      ids.set(it2->id.val());
  }

  ids.sort_unique();

  return ids;
}


template< typename Index, typename Object >
Tag_Store< Index, Object >::Tag_Store(Transaction& transaction_)
    : transaction(&transaction_), use_index(false), ranges({}), items_db(0), tag_it(0), attic_items_db(0), attic_tag_it(0) {}


template< typename Index, typename Object >
void Tag_Store< Index, Object >::prefetch_all(const std::map< Index, std::vector< Object > >& elems)
{
  use_index = true;

  std::set< Index > coarse_indices;
  ids = generate_ids(coarse_indices, elems);

  ranges = formulate_range_query(coarse_indices);

  delete items_db;
  items_db = new Block_Backend< Tag_Index_Local, typename Object::Id_Type >(
      transaction->data_index(current_local_tags_file_properties< Object >()));

  delete tag_it;
  tag_it = new typename Block_Backend< Tag_Index_Local, typename Object::Id_Type >::Range_Iterator(
      items_db->range_begin(ranges));

  if (!coarse_indices.empty())
  {
    tags_by_id.clear();
    stored_index = *(coarse_indices.begin());
    collect_tags_new< typename Object::Id_Type >(tags_by_id, *items_db, *tag_it, ids, stored_index.val());
  }
}


template< typename Index, typename Object >
void Tag_Store< Index, Object >::prefetch_chunk(const std::map< Index, std::vector< Object > >& elems,
    typename Object::Id_Type lower_id_bound, typename Object::Id_Type upper_id_bound)
{
  tags_by_id.clear();

  //generate set of relevant coarse indices
  std::set< Index > coarse_indices;
  ids = generate_ids(coarse_indices, elems);

  Block_Backend< Tag_Index_Local, typename Object::Id_Type > items_db
      (transaction->data_index(current_local_tags_file_properties< Object >()));

  Ranges< Tag_Index_Local > ranges = formulate_range_query(coarse_indices);
  auto tag_it = items_db.range_begin(ranges);

  for (auto it = coarse_indices.begin(); it != coarse_indices.end(); ++it)
    collect_tags_framed_new< typename Object::Id_Type >(tags_by_id, items_db, tag_it, ids, it->val(), lower_id_bound, upper_id_bound);
}


template< typename Index, typename Object >
void Tag_Store< Index, Object >::prefetch_all(const std::map< Index, std::vector< Attic< Object > > >& attic_items)
{
  use_index = true;

  generate_ids_by_coarse(attic_ids_by_coarse, attic_items);

  ranges = formulate_range_query(attic_ids_by_coarse);

  delete items_db;
  items_db = new Block_Backend< Tag_Index_Local, typename Object::Id_Type >(
      transaction->data_index(current_local_tags_file_properties< Object >()));
  delete attic_items_db;
  attic_items_db = new Block_Backend< Tag_Index_Local, Attic< typename Object::Id_Type > >(
      transaction->data_index(attic_local_tags_file_properties< Object >()));

  delete tag_it;
  tag_it = new typename Block_Backend< Tag_Index_Local, typename Object::Id_Type >::Range_Iterator(
      items_db->range_begin(ranges));
  delete attic_tag_it;
  attic_tag_it = new typename Block_Backend< Tag_Index_Local, Attic< typename Object::Id_Type > >::Range_Iterator(
      attic_items_db->range_begin(ranges));

  if (!attic_ids_by_coarse.empty())
  {
    tags_by_id.clear();
    stored_index = attic_ids_by_coarse.begin()->first;
    collect_attic_tags< typename Object::Id_Type >(tags_by_id, *items_db, *tag_it, *attic_items_db, *attic_tag_it,
        attic_ids_by_coarse[stored_index.val()], stored_index.val());
  }
}


template< typename Index, typename Object >
void Tag_Store< Index, Object >::prefetch_chunk(const std::map< Index, std::vector< Attic< Object > > >& attic_items,
    typename Object::Id_Type lower_id_bound, typename Object::Id_Type upper_id_bound)
{
  //generate std::set of relevant coarse indices
  generate_ids_by_coarse(attic_ids_by_coarse, attic_items);

  Block_Backend< Tag_Index_Local, typename Object::Id_Type > current_tags_db
      (transaction->data_index(current_local_tags_file_properties< Object >()));
  Block_Backend< Tag_Index_Local, Attic< typename Object::Id_Type > > attic_tags_db
      (transaction->data_index(attic_local_tags_file_properties< Object >()));

  Ranges< Tag_Index_Local > ranges = formulate_range_query(attic_ids_by_coarse);
  auto current_tag_it = current_tags_db.range_begin(ranges);
  auto attic_tag_it = attic_tags_db.range_begin(ranges);

  for (typename std::map< uint32, std::vector< Attic< typename Object::Id_Type > > >::const_iterator
      it = attic_ids_by_coarse.begin(); it != attic_ids_by_coarse.end(); ++it)
    collect_attic_tags(tags_by_id, current_tags_db, current_tag_it, attic_tags_db, attic_tag_it,
               attic_ids_by_coarse, it->first, lower_id_bound, upper_id_bound);
}


template< typename Index, typename Object >
Tag_Store< Index, Object >::~Tag_Store()
{
  delete items_db;
  delete tag_it;
  delete attic_items_db;
  delete attic_tag_it;
}


template< typename Index, typename Object >
const std::vector< std::pair< std::string, std::string > >*
    Tag_Store< Index, Object >::get(const Index& index, const Object& elem)
{
  if (use_index) {
    auto current_index = Index(index.val() & 0x7fffff00);

    if (!(stored_index == current_index))
    {
      if (current_index < stored_index)
      {
        delete tag_it;
        tag_it = new typename Block_Backend< Tag_Index_Local, typename Object::Id_Type >::Range_Iterator(
            items_db->range_begin(ranges));
        if (attic_items_db)
        {
          delete attic_tag_it;
          attic_tag_it = new typename Block_Backend< Tag_Index_Local, Attic< typename Object::Id_Type > >::Range_Iterator(
              attic_items_db->range_begin(ranges));
        }
      }

      tags_by_id.clear();
      stored_index = current_index;
      if (attic_items_db)
        collect_attic_tags< typename Object::Id_Type >(tags_by_id, *items_db, *tag_it, *attic_items_db, *attic_tag_it,
            attic_ids_by_coarse[stored_index.val()], stored_index.val());
      else
        collect_tags_new< typename Object::Id_Type >(tags_by_id, *items_db, *tag_it,
            ids, stored_index.val());
    }
  }

  auto it = tags_by_id.find(elem.id);
  if (it != tags_by_id.end())
    return &it->second;
  else
    return 0;
}


#endif
