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

#ifndef DE__OSM3S___OVERPASS_API__DATA__COLLECT_ITEMS_H
#define DE__OSM3S___OVERPASS_API__DATA__COLLECT_ITEMS_H

#include "filenames.h"

#include <exception>
#include <functional>


inline uint64 timestamp_of(const Attic< Node_Skeleton >& skel) { return skel.timestamp; }
inline uint64 timestamp_of(const Attic< Way_Skeleton >& skel) { return skel.timestamp; }
inline uint64 timestamp_of(const Attic< Relation_Skeleton >& skel) { return skel.timestamp; }

inline uint64 timestamp_of(const Node_Skeleton& skel) { return NOW; }
inline uint64 timestamp_of(const Way_Skeleton& skel) { return NOW; }
inline uint64 timestamp_of(const Relation_Skeleton& skel) { return NOW; }

template< typename TObject, class TIterator>
inline typename std::enable_if< std::is_same< TObject, Attic < Node_Skeleton > >::value, uint64 >::type
  timestamp_of_it(TIterator& it) { return it.handle().get_timestamp(); };

template< typename TObject, class TIterator>
inline typename std::enable_if< std::is_same< TObject, Attic < Way_Skeleton > >::value, uint64 >::type
  timestamp_of_it(TIterator& it) { return it.handle().get_timestamp(); };

template< typename TObject, class TIterator>
inline typename std::enable_if< std::is_same< TObject, Attic < Relation_Skeleton > >::value, uint64 >::type
  timestamp_of_it(TIterator& it) { return it.handle().get_timestamp(); };

template< typename TObject, class TIterator>
inline typename std::enable_if< std::is_same< TObject, Attic < Way_Delta > >::value, uint64 >::type
  timestamp_of_it(TIterator& it) { return it.handle().get_timestamp(); };

template< typename TObject, class TIterator>
inline typename std::enable_if< std::is_same< TObject, Attic < Relation_Delta > >::value, uint64 >::type
  timestamp_of_it(TIterator& it) { return it.handle().get_timestamp(); };

template< typename TObject, class TIterator>
inline typename std::enable_if< std::is_same< TObject, Node_Skeleton >::value, uint64 >::type
  timestamp_of_it(TIterator& it) { return NOW; };

template< typename TObject, class TIterator>
inline typename std::enable_if< std::is_same< TObject, Way_Skeleton >::value, uint64 >::type
  timestamp_of_it(TIterator& it) { return NOW; };

template< typename TObject, class TIterator>
inline typename std::enable_if< std::is_same< TObject, Relation_Skeleton >::value, uint64 >::type
  timestamp_of_it(TIterator& it) { return NOW; };




template < class Index, class Object, class Iterator, class Predicate >
void reconstruct_items(
    Iterator& it, Iterator& end, Index& index,
    const Predicate& predicate,
    std::vector< Object >& result,
    std::vector< std::pair< typename Object::Id_Type, uint64 > >& timestamp_by_id_attic,
    std::vector< typename Object::Id_Type >& timestamp_by_id_current,
    uint64 timestamp, uint32& count)
{
  bool time_dependent = predicate.is_time_dependent();

  while (!(it == end) && it.index() == index)
  {
    ++count;
    if (timestamp < timestamp_of_it< typename Iterator::object_type >(it))
    {
      bool match = predicate.match(it.handle());

      if ( time_dependent || (!time_dependent && match))
      {
        if (timestamp_of_it< typename Iterator::object_type >(it) == NOW)
          timestamp_by_id_current.push_back(it.handle().id());
        else
          timestamp_by_id_attic.push_back(std::make_pair(it.handle().id(), timestamp_of_it< typename Iterator::object_type >(it)));
      }

      if (match)
        result.push_back(it.object());
    }
    ++it;
  }
}


template < class Index, class Object, class Attic_Iterator, class Current_Iterator, class Predicate >
void reconstruct_items(const Statement* stmt, Resource_Manager& rman,
    Current_Iterator& current_it, Current_Iterator& current_end,
    Attic_Iterator& attic_it, Attic_Iterator& attic_end, Index& idx,
    const Predicate& predicate,
    std::vector< Object >& result, std::vector< Attic< Object > > & attic_result,
    std::vector< std::pair< typename Object::Id_Type, uint64 > >& timestamp_by_id_attic,
    std::vector< typename Object::Id_Type >& timestamp_by_id_current,
    uint64 timestamp)
{
    std::vector< Object > skels;
    std::vector< Attic< typename Object::Delta > > deltas;
    std::vector< std::pair< typename Object::Id_Type, uint64 > > local_timestamp_by_id;

    while (!(current_it == current_end) && current_it.index() == idx)
    {
      timestamp_by_id_current.push_back(current_it.object().id);
      local_timestamp_by_id.push_back(std::make_pair(current_it.object().id, NOW));
      skels.push_back(current_it.object());
      ++current_it;
    }

    while (!(attic_it == attic_end) && attic_it.index() == idx)
    {
      if (timestamp < timestamp_of_it< typename Attic_Iterator::object_type >(attic_it))
      {
        timestamp_by_id_attic.push_back(std::make_pair(attic_it.object().id, attic_it.object().timestamp));
        local_timestamp_by_id.push_back(std::make_pair(attic_it.object().id, attic_it.object().timestamp));
        deltas.push_back(attic_it.object());
      }
      ++attic_it;
    }

    std::vector< const Attic< typename Object::Delta >* > delta_refs;
    delta_refs.reserve(deltas.size());
    for (typename std::vector< Attic< typename Object::Delta > >::const_iterator it = deltas.begin();
        it != deltas.end(); ++it)
      delta_refs.push_back(&*it);

    std::sort(skels.begin(), skels.end());
    std::sort(delta_refs.begin(), delta_refs.end(), Delta_Ref_Comparator< Attic< typename Object::Delta > >());
    std::sort(local_timestamp_by_id.begin(), local_timestamp_by_id.end());

    std::vector< Attic< Object > > attics;
    typename std::vector< Object >::const_iterator skels_it = skels.begin();
    Object reference;
    for (typename std::vector< const Attic< typename Object::Delta >* >::const_iterator it = delta_refs.begin();
         it != delta_refs.end(); ++it)
    {
      if (!(reference.id == (*it)->id))
      {
        while (skels_it != skels.end() && skels_it->id < (*it)->id)
          ++skels_it;
        if (skels_it != skels.end() && skels_it->id == (*it)->id)
          reference = *skels_it;
        else
          reference = Object();
      }
      try
      {
	Attic< Object > attic_obj = Attic< Object >((*it)->expand_fast(reference), (*it)->timestamp);
        if (attic_obj.id.val() != 0)
	{
          typename std::vector< std::pair< typename Object::Id_Type, uint64 > >::const_iterator
              tit = std::lower_bound(local_timestamp_by_id.begin(), local_timestamp_by_id.end(),
				     std::make_pair((*it)->id, 0ull));
	  if (tit != local_timestamp_by_id.end() && tit->first == (*it)->id && tit->second == (*it)->timestamp)
	    attics.push_back(attic_obj);
          reference = std::move(attic_obj);
	}
        else
        {
          // Relation_Delta without a reference of the same index
          std::ostringstream out;
	  out<<name_of_type< Object >()<<" "<<(*it)->id.val()<<" cannot be expanded at timestamp "
	      <<Timestamp((*it)->timestamp).str()<<".";
          rman.log_and_display_error(out.str());
        }
      }
      catch (const std::exception& e)
      {
        std::ostringstream out;
	out<<name_of_type< Object >()<<" "<<(*it)->id.val()<<" cannot be expanded at timestamp "
	    <<Timestamp((*it)->timestamp).str()<<": "<<e.what();
        rman.log_and_display_error(out.str());
      }
    }

    for (typename std::vector< Attic< Object > >::const_iterator it = attics.begin(); it != attics.end(); ++it)
    {
      if (predicate.match(*it))
        attic_result.push_back(*it);
    }

    for (typename std::vector< Object >::const_iterator it = skels.begin(); it != skels.end(); ++it)
    {
      if (predicate.match(*it))
        result.push_back(*it);
    }
}


template < class Object >
void filter_items_by_timestamp(
    const std::vector< std::pair< typename Object::Id_Type, uint64 > >& timestamp_by_id_attic,
    const std::vector< typename Object::Id_Type >& timestamp_by_id_current,
    std::vector< Object > & result)
{
    typename std::vector< Object >::iterator target_it = result.begin();
    for (typename std::vector< Object >::iterator it2 = result.begin();
         it2 != result.end(); ++it2)
    {
      typename std::vector< std::pair< typename Object::Id_Type, uint64 > >::const_iterator
      tit_attic = std::lower_bound(timestamp_by_id_attic.begin(), timestamp_by_id_attic.end(),
          std::make_pair(it2->id, 0ull));
      if (tit_attic != timestamp_by_id_attic.end() && tit_attic->first == it2->id)
      {
         if (tit_attic->second == timestamp_of(*it2))
         {
          *target_it = *it2;
          ++target_it;
         }
      }
      else
      {
        if (NOW == timestamp_of(*it2) &&
            std::binary_search(timestamp_by_id_current.begin(), timestamp_by_id_current.end(), it2->id))
        {
          *target_it = *it2;
          ++target_it;
        }
      }
    }
    result.erase(target_it, result.end());
}

template< typename Object >
void check_for_duplicated_objects(
    const std::vector< std::pair< typename Object::Id_Type, uint64 > >& timestamp_by_id, Resource_Manager& rman)
{
  // Debug-Feature. Can be disabled once no further bugs appear
  for (typename std::vector< std::pair< typename Object::Id_Type, uint64 > >::size_type i = 0;
      i+1 < timestamp_by_id.size(); ++i)
  {
    if (timestamp_by_id[i].second == timestamp_by_id[i+1].second
      && timestamp_by_id[i].first == timestamp_by_id[i+1].first)
    {
      std::ostringstream out;
      out<<name_of_type< Object >()<<" "<<timestamp_by_id[i].first.val()
          <<" appears multiple times at timestamp "<<Timestamp(timestamp_by_id[i].second).str();
      rman.log_and_display_error(out.str());
    }
  }
}


template < class Index, class Object, class Current_Iterator, class Attic_Iterator, class Predicate >
bool collect_items_by_timestamp(const Statement* stmt, Resource_Manager& rman,
                   Current_Iterator current_begin, Current_Iterator current_end,
                   Attic_Iterator attic_begin, Attic_Iterator attic_end,
                   const Predicate& predicate, Index* cur_idx, uint64 timestamp,
                   std::map< Index, std::vector< Object > >& result,
                   std::map< Index, std::vector< Attic< Object > > >& attic_result)
{
  auto result_eval_map = eval_map(result);
  auto attic_result_eval_map = eval_map(attic_result);

  uint32 count = 0;
  while (!(current_begin == current_end) || !(attic_begin == attic_end))
  {
    std::vector< std::pair< typename Object::Id_Type, uint64 > > timestamp_by_id_attic;
    std::vector< typename Object::Id_Type > timestamp_by_id_current;

    bool too_much_data = false;
    if (++count >= 128*1024)
    {
      count = 0;
      if (stmt)
      {
        rman.health_check(*stmt, 0, result_eval_map);
        rman.health_check(*stmt, 0, attic_result_eval_map);
      }
    }
    Index index =
        (attic_begin == attic_end ||
            (!(current_begin == current_end) && current_begin.index() < attic_begin.index())
        ? current_begin.index() : attic_begin.index());
    if (too_much_data && cur_idx)
    {
      *cur_idx = index;
      return true;
    }

    auto prev_result_size = result.size();
    auto prev_attic_result_size = attic_result.size();

    reconstruct_items(current_begin, current_end, index, predicate, result[index], timestamp_by_id_attic, timestamp_by_id_current, timestamp, count);
    reconstruct_items(attic_begin, attic_end, index, predicate, attic_result[index], timestamp_by_id_attic, timestamp_by_id_current, timestamp, count);

    std::sort(timestamp_by_id_attic.begin(), timestamp_by_id_attic.end());
    std::sort(timestamp_by_id_current.begin(), timestamp_by_id_current.end());

    filter_items_by_timestamp(timestamp_by_id_attic, timestamp_by_id_current, result[index]);
    filter_items_by_timestamp(timestamp_by_id_attic, timestamp_by_id_current, attic_result[index]);

    // check_for_duplicated_objects< Object >(timestamp_by_id_attic, rman);
    result_eval_map       += result[index].size() * eval_elem<Object>() + (result.size() - prev_result_size) * eval_map_index_size;
    attic_result_eval_map += attic_result[index].size() * eval_elem< Attic< Object > >() + (attic_result.size() - prev_attic_result_size) * eval_map_index_size;
  }
  return false;    
}


template < class Index, class Current_Iterator, class Attic_Iterator, class Predicate >
bool collect_items_by_timestamp(const Statement* stmt, Resource_Manager& rman,
                   Current_Iterator current_begin, Current_Iterator current_end,
                   Attic_Iterator attic_begin, Attic_Iterator attic_end,
                   const Predicate& predicate, Index* cur_idx, uint64 timestamp,
                   std::map< Index, std::vector< Relation_Skeleton > >& result,
                   std::map< Index, std::vector< Attic< Relation_Skeleton > > >& attic_result)
{
  auto result_eval_map = eval_map(result);
  auto attic_result_eval_map = eval_map(attic_result);

  uint32 count = 0;
  while (!(current_begin == current_end) || !(attic_begin == attic_end))
  {
    std::vector< std::pair< Relation_Skeleton::Id_Type, uint64 > > timestamp_by_id_attic;
    std::vector< Relation_Skeleton::Id_Type > timestamp_by_id_current;

    bool too_much_data = false;
    if (++count >= 128*1024)
    {
      count = 0;
      if (stmt)
      {
        rman.health_check(*stmt, 0, result_eval_map);
        rman.health_check(*stmt, 0, attic_result_eval_map);
      }
    }
    Index index =
        (attic_begin == attic_end ||
            (!(current_begin == current_end) && current_begin.index() < attic_begin.index())
        ? current_begin.index() : attic_begin.index());
    if (too_much_data && cur_idx)
    {
      *cur_idx = index;
      return true;
    }

    auto prev_result_size = result.size();
    auto prev_attic_result_size = attic_result.size();

    reconstruct_items(stmt, rman, current_begin, current_end, attic_begin, attic_end, index,
                      predicate, result[index], attic_result[index], timestamp_by_id_attic, timestamp_by_id_current, timestamp);

    std::sort(timestamp_by_id_attic.begin(), timestamp_by_id_attic.end());
    std::sort(timestamp_by_id_current.begin(), timestamp_by_id_current.end());

    filter_items_by_timestamp(timestamp_by_id_attic, timestamp_by_id_current, result[index]);
    filter_items_by_timestamp(timestamp_by_id_attic, timestamp_by_id_current, attic_result[index]);

   // check_for_duplicated_objects< Relation_Skeleton >(timestamp_by_id, rman);
    result_eval_map       += result[index].size() * eval_elem<Relation_Skeleton>() + (result.size() - prev_result_size) * eval_map_index_size;
    attic_result_eval_map += attic_result[index].size() * eval_elem< Attic< Relation_Skeleton > >() + (attic_result.size() - prev_attic_result_size) * eval_map_index_size;


  }
  return false;
}


template < class Index, class Current_Iterator, class Attic_Iterator, class Predicate >
bool collect_items_by_timestamp(const Statement* stmt, Resource_Manager& rman,
                   Current_Iterator current_begin, Current_Iterator current_end,
                   Attic_Iterator attic_begin, Attic_Iterator attic_end,
                   const Predicate& predicate, Index* cur_idx, uint64 timestamp,
                   std::map< Index, std::vector< Way_Skeleton > >& result,
                   std::map< Index, std::vector< Attic< Way_Skeleton > > >& attic_result)
{
  auto result_eval_map = eval_map(result);
  auto attic_result_eval_map = eval_map(attic_result);

  uint32 count = 0;
  while (!(current_begin == current_end) || !(attic_begin == attic_end))
  {
    std::vector< std::pair< Relation_Skeleton::Id_Type, uint64 > > timestamp_by_id;

    bool too_much_data = false;
    if (++count >= 128*1024)
    {
      count = 0;
      if (stmt)
      {
        rman.health_check(*stmt, 0, result_eval_map);
        rman.health_check(*stmt, 0, attic_result_eval_map);
      }
    }
    Index index =
        (attic_begin == attic_end ||
            (!(current_begin == current_end) && current_begin.index() < attic_begin.index())
        ? current_begin.index() : attic_begin.index());
    if (too_much_data && cur_idx)
    {
      *cur_idx = index;
      return true;
    }
    
    auto prev_result_size = result.size();
    auto prev_attic_result_size = attic_result.size();

    std::vector< std::pair< Way_Skeleton::Id_Type, uint64 > > timestamp_by_id_attic;
    std::vector< Way_Skeleton::Id_Type > timestamp_by_id_current;

    reconstruct_items(stmt, rman, current_begin, current_end, attic_begin, attic_end, index,
                      predicate, result[index], attic_result[index], timestamp_by_id_attic, timestamp_by_id_current, timestamp);

    std::sort(timestamp_by_id_attic.begin(), timestamp_by_id_attic.end());
    std::sort(timestamp_by_id_current.begin(), timestamp_by_id_current.end());

    filter_items_by_timestamp(timestamp_by_id_attic, timestamp_by_id_current, result[index]);
    filter_items_by_timestamp(timestamp_by_id_attic, timestamp_by_id_current, attic_result[index]);

    result_eval_map       += result[index].size() * eval_elem<Way_Skeleton>() + (result.size() - prev_result_size) * eval_map_index_size;
    attic_result_eval_map += attic_result[index].size() * eval_elem< Attic< Way_Skeleton > >() + (attic_result.size() - prev_attic_result_size) * eval_map_index_size;

  // check_for_duplicated_objects< Object >(timestamp_by_id_attic, rman);
   }
  return false; 
}


template < class Index, class Object, class Container, class Predicate >
void collect_items_discrete(const Statement* stmt, Resource_Manager& rman,
		   File_Properties& file_properties,
		   const Container& req, const Predicate& predicate,
		   std::map< Index, std::vector< Object > >& result)
{
  uint32 count = 0;
  uint64 current_result_size = eval_map(result);

  Block_Backend< Index, Object, typename Container::const_iterator > db
      (rman.get_transaction()->data_index(&file_properties));
  for (typename Block_Backend< Index, Object, typename Container
      ::const_iterator >::Discrete_Iterator
      it(db.discrete_begin(req.begin(), req.end())); !(it == db.discrete_end()); ++it)
  {
    if (++count >= 256*1024)
    {
      count = 0;
      if (stmt)
        rman.health_check(*stmt, 0, current_result_size);
    }
    if (predicate.match(it.handle()))
    {
      auto prev_map_size = result.size();
      result[it.index()].push_back(it.object());
      if (result.size() != prev_map_size) {     // new index added to map?
        current_result_size += eval_map_index_size;
      }
      current_result_size += eval_elem<Object>();
    }
  }
}


template < class Index, class Object, class Container, class Predicate >
void collect_items_discrete(Transaction& transaction,
                   File_Properties& file_properties,
                   const Container& req, const Predicate& predicate,
                   std::map< Index, std::vector< Object > >& result)
{
  Block_Backend< Index, Object, typename Container::const_iterator > db
      (transaction.data_index(&file_properties));
  for (typename Block_Backend< Index, Object, typename Container
      ::const_iterator >::Discrete_Iterator
      it(db.discrete_begin(req.begin(), req.end())); !(it == db.discrete_end()); ++it)
  {
    if (predicate.match(it.handle()))
      result[it.index()].push_back(it.object());
  }
}


template < class Index, class Object, class Container, class Predicate >
void collect_items_discrete_by_timestamp(const Statement* stmt, Resource_Manager& rman,
                   const Container& req, const Predicate& predicate,
                   std::map< Index, std::vector< Object > >& result,
                   std::map< Index, std::vector< Attic< Object > > >& attic_result)
{
  Block_Backend< Index, Object, typename Container::const_iterator > current_db
      (rman.get_transaction()->data_index(current_skeleton_file_properties< Object >()));
  Block_Backend< Index, Attic< typename Object::Delta >, typename Container::const_iterator > attic_db
      (rman.get_transaction()->data_index(attic_skeleton_file_properties< Object >()));
  collect_items_by_timestamp(stmt, rman,
      current_db.discrete_begin(req.begin(), req.end()), current_db.discrete_end(),
      attic_db.discrete_begin(req.begin(), req.end()), attic_db.discrete_end(),
      predicate, (Index*)0, rman.get_desired_timestamp(), result, attic_result);
}


template < class Index, class Object, class Container, class Predicate >
void collect_items_discrete_by_timestamp(const Statement* stmt, Resource_Manager& rman,
                   const Container& req, const Predicate& predicate, uint64 timestamp,
                   std::map< Index, std::vector< Object > >& result,
                   std::map< Index, std::vector< Attic< Object > > >& attic_result)
{
  Block_Backend< Index, Object, typename Container::const_iterator > current_db
      (rman.get_transaction()->data_index(current_skeleton_file_properties< Object >()));
  Block_Backend< Index, Attic< typename Object::Delta >, typename Container::const_iterator > attic_db
      (rman.get_transaction()->data_index(attic_skeleton_file_properties< Object >()));
  collect_items_by_timestamp(stmt, rman,
      current_db.discrete_begin(req.begin(), req.end()), current_db.discrete_end(),
      attic_db.discrete_begin(req.begin(), req.end()), attic_db.discrete_end(),
      predicate, (Index*)0, timestamp, result, attic_result);
}


template < class Index, class Container >
struct Shortened_Idx
{
  Shortened_Idx(const Container& req, const Index& cur_idx) : skipped(req.begin()), end_(req.end())
  {
    if (skipped != req.end() && !(skipped.lower_bound() == cur_idx))
    {
      while (skipped != req.end() && !(cur_idx < skipped.upper_bound()))
        ++skipped;

      if (skipped != req.end() && !(skipped.lower_bound() == cur_idx))
      {
        shortened.insert(std::make_pair(cur_idx, skipped.upper_bound()));
        for (++skipped; skipped != end_; ++skipped)
          shortened.insert(*skipped);
        skipped = shortened.begin();
        end_ = shortened.end();
      }
    }
}

  typename std::set< std::pair< Index, Index > >::const_iterator begin() { return skipped; }
  typename std::set< std::pair< Index, Index > >::const_iterator end() { return end_; }

private:
  std::set< std::pair< Index, Index > > shortened;
  Default_Range_Iterator< Index > skipped;
  Default_Range_Iterator< Index > end_;
};


template < class Index, class Object, class Container, class Predicate >
bool collect_items_range(const Statement* stmt, Resource_Manager& rman,
		   File_Properties& file_properties,
		   const Container& req, const Predicate& predicate,
		   Index& cur_idx,
		   std::map< Index, std::vector< Object > >& result)
{
  uint32 count = 0;
  uint64 current_result_size = eval_map(result);
  
  bool too_much_data = false;

  Block_Backend< Index, Object > db
      (rman.get_transaction()->data_index(&file_properties));
      
  Shortened_Idx< Index, Container > shortened(req, cur_idx);      
  for (typename Block_Backend< Index, Object >::Range_Iterator
      it(db.range_begin(shortened.begin(), shortened.end()));
	   !(it == db.range_end()); ++it)
  {
    if (too_much_data && !(cur_idx == it.index()))
    {
      cur_idx = it.index();
      return true;
    }  

    if (++count >= 256*1024 && stmt)
    {
      count = 0;
      too_much_data = rman.health_check(*stmt, 0, current_result_size);
      cur_idx = it.index();
    }

    if (predicate.match(it.handle()))
    {
      auto prev_map_size = result.size();

      it.handle().add_element(result[it.index()]);

      if (result.size() != prev_map_size) {     // new index added to map?
        current_result_size += eval_map_index_size;
      }
      current_result_size += eval_elem<Object>();
    }
  }
  
  return false;
}

template < class Index, class Object, class Container, class Functor >
bool collect_items_range(const Statement* stmt, Resource_Manager& rman,
                   File_Properties& file_properties,
                   const Container& req, Index& cur_idx,
                   std::map< Index, std::vector< Object > >& result,
                   Functor pred)
{
  uint32 count = 0;
  uint64 current_result_size = eval_map(result);
  
  bool too_much_data = false;

  Block_Backend< Index, Object > db
      (rman.get_transaction()->data_index(&file_properties));
      
  Shortened_Idx< Index, Container > shortened(req, cur_idx);            
  for (typename Block_Backend< Index, Object >::Range_Iterator
      it(db.range_begin(shortened.begin(), shortened.end()));
           !(it == db.range_end()); ++it)
  {
    if (too_much_data && !(cur_idx == it.index()))
    {
      cur_idx = it.index();
      return true;
    }  

    if (++count >= 256*1024 && stmt)
    {
      count = 0;
      too_much_data = rman.health_check(*stmt, 0, current_result_size);
      cur_idx = it.index();
    }

    if (pred(it.index(), it.handle().id()))
    {
      auto prev_map_size = result.size();

      it.handle().add_element(result[it.index()]);

      if (result.size() != prev_map_size) {     // new index added to map?
        current_result_size += eval_map_index_size;
      }
      current_result_size += eval_elem<Object>();
    }
  }
  
  return false;
}


template < class Index, class Object, class Container, class Predicate >
bool collect_items_range_by_timestamp(const Statement* stmt, Resource_Manager& rman,
                   const Container& req, const Predicate& predicate, Index& cur_idx,
                   std::map< Index, std::vector< Object > >& result,
                   std::map< Index, std::vector< Attic< Object > > >& attic_result)
{
  Shortened_Idx< Index, Container > shortened(req, cur_idx);
  Block_Backend< Index, Object > current_db
      (rman.get_transaction()->data_index(current_skeleton_file_properties< Object >()));
  Block_Backend< Index, Attic< typename Object::Delta > > attic_db
      (rman.get_transaction()->data_index(attic_skeleton_file_properties< Object >()));
  return collect_items_by_timestamp(stmt, rman,
      current_db.range_begin(shortened.begin(), shortened.end()), current_db.range_end(),
      attic_db.range_begin(shortened.begin(), shortened.end()), attic_db.range_end(),
      predicate, &cur_idx, rman.get_desired_timestamp(), result, attic_result);
}


template < class Index, class Object, class Predicate >
void collect_items_flat(const Statement& stmt, Resource_Manager& rman,
		   File_Properties& file_properties, const Predicate& predicate,
		   std::map< Index, std::vector< Object > >& result)
{
  uint32 count = 0;
  uint64 current_result_size = eval_map(result);

  Block_Backend< Index, Object > db
      (rman.get_transaction()->data_index(&file_properties));
  for (typename Block_Backend< Index, Object >::Flat_Iterator
      it(db.flat_begin()); !(it == db.flat_end()); ++it)
  {
    if (++count >= 256*1024)
    {
      count = 0;
      rman.health_check(stmt, 0, current_result_size);
    }
    if (predicate.match(it.handle()))
    {
      auto prev_map_size = result.size();
      result[it.index()].push_back(it.object());
      if (result.size() != prev_map_size) {     // new index added to map?
        current_result_size += eval_map_index_size;
      }
      current_result_size += eval_elem<Object>();
    }
  }
}


template < class Index, class Object, class Predicate >
void collect_items_flat_by_timestamp(const Statement& stmt, Resource_Manager& rman,
                   const Predicate& predicate,
                   std::map< Index, std::vector< Object > >& result,
                   std::map< Index, std::vector< Attic< Object > > >& attic_result)
{
  Block_Backend< Index, Object > current_db
      (rman.get_transaction()->data_index(current_skeleton_file_properties< Object >()));
  Block_Backend< Index, Attic< typename Object::Delta > > attic_db
      (rman.get_transaction()->data_index(attic_skeleton_file_properties< Object >()));
  collect_items_by_timestamp(&stmt, rman,
      current_db.flat_begin(), current_db.flat_end(),
      attic_db.flat_begin(), attic_db.flat_end(),
      predicate, (Index*)0, rman.get_desired_timestamp(), result, attic_result);
}


/* Returns for the given std::set of ids the std::set of corresponding indexes.
 * The function requires that the ids are sorted ascending by id.
 */
template< typename Index, typename Skeleton >
std::vector< Index > get_indexes_
    (const std::vector< typename Skeleton::Id_Type >& ids, Resource_Manager& rman, bool get_attic_idxs = false)
{
  std::vector< Index > result;

  Random_File< typename Skeleton::Id_Type, Index > current(rman.get_transaction()->random_index
      (current_skeleton_file_properties< Skeleton >()));
  for (typename std::vector< typename Skeleton::Id_Type >::const_iterator
      it = ids.begin(); it != ids.end(); ++it)
    result.push_back(current.get(it->val()));

  std::sort(result.begin(), result.end());
  result.erase(std::unique(result.begin(), result.end()), result.end());

  if (rman.get_desired_timestamp() != NOW || get_attic_idxs)
  {
    Random_File< typename Skeleton::Id_Type, Index > attic_random(rman.get_transaction()->random_index
        (attic_skeleton_file_properties< Skeleton >()));
    std::set< typename Skeleton::Id_Type > idx_list_ids;
    for (typename std::vector< typename Skeleton::Id_Type >::const_iterator
        it = ids.begin(); it != ids.end(); ++it)
    {
      if (attic_random.get(it->val()).val() == 0)
        ;
      else if (attic_random.get(it->val()) == 0xff)
        idx_list_ids.insert(it->val());
      else
        result.push_back(attic_random.get(it->val()));
    }

    Block_Backend< typename Skeleton::Id_Type, Index > idx_list_db
        (rman.get_transaction()->data_index(attic_idx_list_properties< Skeleton >()));
    for (typename Block_Backend< typename Skeleton::Id_Type, Index >::Discrete_Iterator
        it(idx_list_db.discrete_begin(idx_list_ids.begin(), idx_list_ids.end()));
        !(it == idx_list_db.discrete_end()); ++it)
      result.push_back(it.object());

    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
  }

  return result;
}


#endif
