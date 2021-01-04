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

#include "../../template_db/block_backend.h"
#include "../../template_db/random_file.h"
#include "../core/settings.h"
#include "../data/abstract_processing.h"
#include "../data/collect_members.h"
#include "../data/filenames.h"
#include "../data/filter_by_tags.h"
#include "../data/filter_ids_by_tags.h"
#include "../data/meta_collector.h"
#include "../data/regular_expression.h"
#include "area_query.h"
#include "bbox_query.h"
#include "query.h"

#include <algorithm>
#include <sstream>



//-----------------------------------------------------------------------------

int Query_Statement::area_query_ref_counter_ = 0;

Generic_Statement_Maker< Query_Statement > Query_Statement::statement_maker("query");

Query_Statement::Query_Statement
    (int line_number_, const std::map< std::string, std::string >& input_attributes, Parsed_Query& global_settings)
    : Output_Statement(line_number_), global_bbox_statement(0)
{
  std::map< std::string, std::string > attributes;

  attributes["into"] = "_";
  attributes["type"] = "";

  eval_attributes_array(get_name(), attributes, input_attributes);

  set_output(attributes["into"]);
  if (attributes["type"] == "node")
    type = QUERY_NODE;
  else if (attributes["type"] == "way")
    type = QUERY_WAY;
  else if (attributes["type"] == "relation")
    type = QUERY_RELATION;
  else if (attributes["type"] == "derived")
    type = QUERY_DERIVED;
  else if (attributes["type"] == "nwr")
    type = (QUERY_NODE | QUERY_WAY | QUERY_RELATION);
  else if (attributes["type"] == "nw")
    type = (QUERY_NODE | QUERY_WAY);
  else if (attributes["type"] == "wr")
    type = (QUERY_WAY | QUERY_RELATION);
  else if (attributes["type"] == "nr")
    type = (QUERY_NODE | QUERY_RELATION);
  else if (attributes["type"] == "area")
  {
    type = QUERY_AREA;
    ++area_query_ref_counter_;
  }
  else
  {
    type = 0;
    std::ostringstream temp;
    temp<<"For the attribute \"type\" of the element \"query\""
        <<" the only allowed values are \"node\", \"way\", \"relation\", \"nwr\", \"nw\", \"wr\", \"nr\", or \"area\".";
    add_static_error(temp.str());
  }

  if (global_settings.get_global_bbox_limitation().valid())
  {
    global_bbox_statement = new Bbox_Query_Statement(global_settings.get_global_bbox_limitation());
    constraints.push_back(global_bbox_statement->get_query_constraint());
  }
}

Query_Statement::~Query_Statement() {
  if (type == QUERY_AREA && area_query_ref_counter_ > 0)
    --area_query_ref_counter_;

  delete global_bbox_statement;
}

void Query_Statement::add_statement(Statement* statement, std::string text)
{
  assure_no_text(text, this->get_name());

  Has_Kv_Statement* has_kv(dynamic_cast<Has_Kv_Statement*>(statement));
  if (has_kv)
  {
    substatements.push_back(statement);

    if (has_kv->get_value() != "")
    {
      if (has_kv->get_straight())
        key_values.push_back(std::make_pair< std::string, std::string >
	    (has_kv->get_key(), has_kv->get_value()));
      else
        key_nvalues.push_back(std::make_pair< std::string, std::string >
	    (has_kv->get_key(), has_kv->get_value()));
    }
    else if (has_kv->get_key_regex())
    {
      if (has_kv->get_straight())
	regkey_regexes.push_back(std::make_pair< Regular_Expression*, Regular_Expression* >
            (has_kv->get_key_regex(), has_kv->get_regex()));
      else
	regkey_nregexes.push_back(std::make_pair< Regular_Expression*, Regular_Expression* >
            (has_kv->get_key_regex(), has_kv->get_regex()));
    }
    else if (has_kv->get_regex())
    {
      if (has_kv->get_straight())
	key_regexes.push_back(std::make_pair< std::string, Regular_Expression* >
            (has_kv->get_key(), has_kv->get_regex()));
      else
	key_nregexes.push_back(std::make_pair< std::string, Regular_Expression* >
            (has_kv->get_key(), has_kv->get_regex()));
    }
    else
      keys.push_back(has_kv->get_key());
    return;
  }

  Query_Constraint* constraint = statement->get_query_constraint();
  if (constraint)
  {
    constraints.push_back(constraint);
    substatements.push_back(statement);
  }
  else
    substatement_error(get_name(), statement);
}


template < typename T >
struct Optional
{
  Optional(T* obj_) : obj(obj_) {}
  ~Optional() { delete obj; }

  T* obj;
};


struct Trivial_Regex
{
public:
  bool matches(const std::string&) const { return true; }
};


template< typename Id_Type, typename Iterator, typename Key_Regex, typename Val_Regex >
void filter_id_list(
    std::vector< std::pair< Id_Type, Uint31_Index > >& new_ids, bool& filtered,
    Iterator begin, Iterator end, const Key_Regex& key_regex, const Val_Regex& val_regex,
    Query_Filter_Strategy& check_keys_late)
{
  std::vector< std::pair< Id_Type, Uint31_Index > > old_ids;
  old_ids.swap(new_ids);

  for (Iterator it = begin; !(it == end); ++it)
  {
    if (key_regex.matches(it.index().key) && it.index().value != void_tag_value()
        && val_regex.matches(it.index().value) && (!filtered ||
	binary_search(old_ids.begin(), old_ids.end(), std::make_pair(it.handle().id(), Uint31_Index(0u)))))
      new_ids.push_back(std::make_pair(it.handle().id(), it.object().idx));

    if (!filtered && new_ids.size() == 1024*1024)
    {
      if (check_keys_late == prefer_ranges)
      {
        new_ids.clear();
        return;
      }
      else if (check_keys_late == ids_useful)
      {
        check_keys_late = prefer_ranges;
        new_ids.clear();
        return;
      }
    }
  }

  sort(new_ids.begin(), new_ids.end());
  new_ids.erase(unique(new_ids.begin(), new_ids.end()), new_ids.end());

  filtered = true;
}


template< typename Id_Type, typename Iterator, typename Key_Regex, typename Val_Regex, unsigned int L >
std::vector< std::pair< Id_Type, Uint31_Index > > filter_id_list_fast(
    IdSetHybrid<typename Id_Type::Id_Type, L>& new_ids, bool& filtered,
    Iterator begin, Iterator end, const Key_Regex& key_regex, const Val_Regex& val_regex,
    Query_Filter_Strategy& check_keys_late, bool final)
{
  std::vector< std::pair< Id_Type, Uint31_Index > > new_ids_idx;

  IdSetHybrid<typename Id_Type::Id_Type, L> old_ids(std::move(new_ids));
  new_ids.clear();

  if (filtered && old_ids.empty()) {
    new_ids.clear();
    new_ids_idx.clear();
    return new_ids_idx;
  }

  for (Iterator it = begin; !(it == end); ++it)
  {
    auto current_id = it.handle().id().val();

    if (key_regex.matches(it.index().key) &&
        it.index().value != void_tag_value() &&
        val_regex.matches(it.index().value) &&
        (!filtered || old_ids.get(current_id)))
    {
      if (final)
         new_ids_idx.push_back(std::make_pair(current_id, it.handle().get_idx())); // it.object().idx));
      else
         new_ids.set(current_id);
    }

    if (!filtered && new_ids_idx.size() == 1024*1024)
    {
      if (check_keys_late == prefer_ranges)
      {
        new_ids.clear();
        new_ids_idx.clear();
        return new_ids_idx;
      }
      else if (check_keys_late == ids_useful)
      {
        check_keys_late = prefer_ranges;
        new_ids.clear();
        new_ids_idx.clear();
        return new_ids_idx;
      }
    }
  }

  // sort and remove duplicates in small set
  new_ids.sort_unique();

  if (final) {
    sort(new_ids_idx.begin(), new_ids_idx.end());
    new_ids_idx.erase(unique(new_ids_idx.begin(), new_ids_idx.end()), new_ids_idx.end());
  }

  filtered = true;

  return new_ids_idx;
}

template< typename Id_Type, typename Iterator, typename Key_Regex, typename Val_Regex >
void filter_id_list(
    std::vector< Id_Type >& new_ids, bool& filtered,
    Iterator begin, Iterator end, const Key_Regex& key_regex, const Val_Regex& val_regex)
{
  std::vector< Id_Type > old_ids;
  old_ids.swap(new_ids);

  for (Iterator it = begin; !(it == end); ++it)
  {
    if (key_regex.matches(it.index().key) && it.index().value != void_tag_value()
        && val_regex.matches(it.index().value) &&
	(!filtered || binary_search(old_ids.begin(), old_ids.end(), it.object())))
      new_ids.push_back(it.object());
  }

  sort(new_ids.begin(), new_ids.end());
  new_ids.erase(unique(new_ids.begin(), new_ids.end()), new_ids.end());

  filtered = true;
}


template< typename Id_Type, typename Container >
void filter_id_list(
    std::vector< std::pair< Id_Type, Uint31_Index > >& new_ids, bool& filtered,
    const Container& container)
{
  std::vector< std::pair< Id_Type, Uint31_Index > > old_ids;
  old_ids.swap(new_ids);

  for (typename Container::const_iterator it = container.begin(); it != container.end(); ++it)
  {
    if (!filtered ||
	binary_search(old_ids.begin(), old_ids.end(), std::make_pair(it->first, Uint31_Index(0u))))
      new_ids.push_back(std::make_pair(it->first, it->second.second));
  }

  sort(new_ids.begin(), new_ids.end());
  new_ids.erase(unique(new_ids.begin(), new_ids.end()), new_ids.end());

  filtered = true;
}

template< typename Id_Type, typename Container, unsigned int L >
std::vector< std::pair< Id_Type, Uint31_Index > > filter_id_list_fast(
    IdSetHybrid<typename Id_Type::Id_Type, L>& new_ids, bool& filtered,
    const Container& container, bool final)
{
  std::vector< std::pair< Id_Type, Uint31_Index > > new_ids_result;

  IdSetHybrid<typename Id_Type::Id_Type, L> old_ids(std::move(new_ids));
  new_ids.clear();

  for (typename Container::const_iterator it = container.begin(); it != container.end(); ++it)
  {
    if (!filtered || old_ids.get(it->first.val()))
    {
     if (final)
       new_ids_result.push_back(std::make_pair(it->first, it->second.second));
     else
       new_ids.set(it->first.val());
    }
  }

  // sort and remove duplicates in small set
  new_ids.sort_unique();

  if (final)
  {
    sort(new_ids_result.begin(), new_ids_result.end());
    new_ids_result.erase(unique(new_ids_result.begin(), new_ids_result.end()), new_ids_result.end());
  }

  filtered = true;

  return new_ids_result;
}


template< typename Id_Type, typename Container, unsigned int L >
std::vector< std::pair< Id_Type, Uint31_Index > > filter_id_list_fast2(
    IdSetHybrid<typename Id_Type::Id_Type, L>& new_ids, bool& filtered,
    Container& container, bool final)
{
  std::vector< std::pair< Id_Type, Uint31_Index > > new_ids_result;

  IdSetHybrid<typename Id_Type::Id_Type, L> old_ids(std::move(new_ids));
  new_ids.clear();

  if (!filtered && final) {
    new_ids_result = std::move(container);
  }
  else
  {
    for (typename Container::const_iterator it = container.begin(); it != container.end(); ++it)
    {
      if (!filtered || old_ids.get(it->first.val()))
      {
       if (final)
         new_ids_result.emplace_back(*it);
       else
         new_ids.set(it->first.val());
      }
    }
  }

  filtered = true;

  return new_ids_result;
}

template <typename Iter>
Iter next_element(Iter iter)
{
    return ++iter;
}

template <typename Iter, typename Cont>
bool is_last(Iter iter, const Cont& cont)
{
    return (iter != cont.end()) && (next_element(iter) == cont.end());
}

enum class FinalProcessing {
  key_values,
  keys,
  key_regexes,
  regkey_regexes
};


template< typename Skeleton, typename Id_Type >
std::vector< std::pair< Id_Type, Uint31_Index > > Query_Statement::collect_ids
  (const File_Properties& file_prop, const File_Properties& attic_file_prop, Resource_Manager& rman,
   uint64 timestamp, Query_Filter_Strategy& check_keys_late, bool& result_valid)
{
  if (key_values.empty() && keys.empty() && key_regexes.empty() && regkey_regexes.empty())
    return std::vector< std::pair< Id_Type, Uint31_Index > >();

  Block_Backend< Tag_Index_Global, Tag_Object_Global< Id_Type > > tags_db
      (rman.get_transaction()->data_index(&file_prop));
  Optional< Block_Backend< Tag_Index_Global, Attic< Tag_Object_Global< Id_Type > > > > attic_tags_db
      (timestamp == NOW ? 0 :
        new Block_Backend< Tag_Index_Global, Attic< Tag_Object_Global< Id_Type > > >
        (rman.get_transaction()->data_index(&attic_file_prop)));


  IdSetHybrid<typename Id_Type::Id_Type> tmp_ids;

  // Handle simple Key-Value pairs
  std::vector< std::pair< Id_Type, Uint31_Index > > new_ids;
  bool filtered = false;

  bool last = false;

  FinalProcessing fp = FinalProcessing::key_values;

  if (check_keys_late != prefer_ranges) {
    if (!regkey_regexes.empty())
      fp = FinalProcessing::regkey_regexes;
    else if(!key_regexes.empty())
      fp = FinalProcessing::key_regexes;
    else if(!keys.empty())
      fp = FinalProcessing::keys;
  }

  for (std::vector< std::pair< std::string, std::string > >::const_iterator kvit = key_values.begin();
       kvit != key_values.end(); ++kvit)
  {

    last = (fp == FinalProcessing::key_values && is_last(kvit, key_values));

    if (timestamp == NOW)
    {
      std::set< Tag_Index_Global > tag_req = get_kv_req(kvit->first, kvit->second);
      new_ids = filter_id_list_fast<Id_Type>(tmp_ids, filtered,
		     tags_db.discrete_begin(tag_req.begin(), tag_req.end()), tags_db.discrete_end(),
		     Trivial_Regex(), Trivial_Regex(), check_keys_late, last);
      if (!filtered)
      {
	result_valid = false;
	break;
      }
    }
    else
    {
      auto attic_kv = collect_attic_kv2(kvit, timestamp, tags_db, *attic_tags_db.obj);
      new_ids = filter_id_list_fast2<Id_Type>(tmp_ids, filtered, attic_kv, last);
    }

    rman.health_check(*this);
  }

  if (check_keys_late != prefer_ranges)
  {
    // Handle simple Keys Only
    for (std::vector< std::string >::const_iterator kit = keys.begin(); kit != keys.end(); ++kit)
    {
      last = (fp == FinalProcessing::keys && is_last(kit, keys));

      if (timestamp == NOW)
      {
        std::set< std::pair< Tag_Index_Global, Tag_Index_Global > > range_req = get_k_req(*kit);
        new_ids = filter_id_list_fast<Id_Type>(tmp_ids, filtered,
		       tags_db.range_begin(range_req.begin(), range_req.end()), tags_db.range_end(),
			Trivial_Regex(), Trivial_Regex(), check_keys_late, last);
        if (!filtered)
        {
          result_valid = false;
          break;
        }
      }
      else
      {
        auto attic_k = collect_attic_k2(kit, timestamp, tags_db, *attic_tags_db.obj);
	new_ids = filter_id_list_fast2<Id_Type>(tmp_ids, filtered, attic_k, last);
      }

      rman.health_check(*this);
    }
  }

  if (check_keys_late != prefer_ranges)
  {
    // Handle Key-Regular-Expression-Value pairs
    for (std::vector< std::pair< std::string, Regular_Expression* > >::const_iterator krit = key_regexes.begin();
	 krit != key_regexes.end(); ++krit)
    {
      last = (fp == FinalProcessing::key_regexes && is_last(krit, key_regexes));

      if (timestamp == NOW)
      {
        std::set< std::pair< Tag_Index_Global, Tag_Index_Global > > range_req = get_k_req(krit->first);
        new_ids = filter_id_list_fast<Id_Type>(tmp_ids, filtered,
	    tags_db.range_begin(range_req.begin(), range_req.end()), tags_db.range_end(),
		Trivial_Regex(), *krit->second, check_keys_late, last);
        if (!filtered)
        {
          result_valid = false;
          break;
        }
      }
      else
      {
        auto attic_kregv = collect_attic_kregv(krit, timestamp, tags_db, *attic_tags_db.obj);
	new_ids = filter_id_list_fast<Id_Type>(tmp_ids, filtered, attic_kregv, last);
      }

      rman.health_check(*this);
    }
  }

  if (check_keys_late != prefer_ranges)
  {
    // Handle Regular-Key-Regular-Expression-Value pairs
    for (std::vector< std::pair< Regular_Expression*, Regular_Expression* > >::const_iterator it = regkey_regexes.begin();
	 it != regkey_regexes.end(); ++it)
    {
      last = (fp == FinalProcessing::regkey_regexes && is_last(it, regkey_regexes));

      if (timestamp == NOW)
      {
	std::set< std::pair< Tag_Index_Global, Tag_Index_Global > > range_req
	    = get_regk_req< Skeleton >(it->first, rman, *this);
	new_ids = filter_id_list_fast<Id_Type>(tmp_ids, filtered,
	    tags_db.range_begin(range_req.begin(), range_req.end()), tags_db.range_end(),
	    *it->first, *it->second, check_keys_late, last);
        if (!filtered)
        {
          result_valid = false;
          break;
        }
      }
      else
      {
        auto attic_regkregv = collect_attic_regkregv< Skeleton, Id_Type >(it, timestamp, tags_db,
                                                                  *attic_tags_db.obj, rman, *this);
	new_ids = filter_id_list_fast<Id_Type>(tmp_ids, filtered, attic_regkregv, last);
      }

      rman.health_check(*this);
    }
  }

  return new_ids;
}


template< class Id_Type >
std::vector< Id_Type > Query_Statement::collect_ids
  (const File_Properties& file_prop, Resource_Manager& rman,
   Query_Filter_Strategy check_keys_late)
{
  if (key_values.empty() && keys.empty() && key_regexes.empty() && regkey_regexes.empty())
    return std::vector< Id_Type >();

  Block_Backend< Tag_Index_Global, Id_Type > tags_db
      (rman.get_transaction()->data_index(&file_prop));

  // Handle simple Key-Value pairs
  std::vector< Id_Type > new_ids;
  bool filtered = false;

  for (std::vector< std::pair< std::string, std::string > >::const_iterator kvit = key_values.begin();
       kvit != key_values.end(); ++kvit)
  {
    std::set< Tag_Index_Global > tag_req = get_kv_req(kvit->first, kvit->second);
    filter_id_list(new_ids, filtered,
	tags_db.discrete_begin(tag_req.begin(), tag_req.end()), tags_db.discrete_end(),
	    Trivial_Regex(), Trivial_Regex());

    rman.health_check(*this);
  }

  // Handle simple Keys Only
  if (check_keys_late != prefer_ranges)
  {
    for (std::vector< std::string >::const_iterator kit = keys.begin(); kit != keys.end(); ++kit)
    {
      std::set< std::pair< Tag_Index_Global, Tag_Index_Global > > range_req = get_k_req(*kit);
      filter_id_list(new_ids, filtered,
	  tags_db.range_begin(range_req.begin(), range_req.end()), tags_db.range_end(),
	      Trivial_Regex(), Trivial_Regex());

      rman.health_check(*this);
    }

    // Handle Key-Regular-Expression-Value pairs
    for (std::vector< std::pair< std::string, Regular_Expression* > >::const_iterator krit = key_regexes.begin();
	 krit != key_regexes.end(); ++krit)
    {
      std::set< std::pair< Tag_Index_Global, Tag_Index_Global > > range_req = get_k_req(krit->first);
      filter_id_list(new_ids, filtered,
	  tags_db.range_begin(range_req.begin(), range_req.end()), tags_db.range_end(),
	      Trivial_Regex(), *krit->second);

      rman.health_check(*this);
    }

    // Handle Key-Regular-Expression-Value pairs
    for (std::vector< std::pair< Regular_Expression*, Regular_Expression* > >::const_iterator it = regkey_regexes.begin();
	 it != regkey_regexes.end(); ++it)
    {
      filter_id_list(new_ids, filtered,
	  tags_db.flat_begin(), tags_db.flat_end(), *it->first, *it->second);

      rman.health_check(*this);
    }
  }

  return new_ids;
}


template< class Id_Type >
IdSetHybrid<typename Id_Type::Id_Type> Query_Statement::collect_non_ids_hybrid
  (const File_Properties& file_prop, const File_Properties& attic_file_prop,
   Resource_Manager& rman, uint64 timestamp)
{
  if (key_nvalues.empty() && key_nregexes.empty())
    return IdSetHybrid<typename Id_Type::Id_Type>();

  Block_Backend< Tag_Index_Global, Tag_Object_Global< Id_Type > > tags_db
      (rman.get_transaction()->data_index(&file_prop));
  Optional< Block_Backend< Tag_Index_Global, Attic< Tag_Object_Global< Id_Type > > > > attic_tags_db
      (timestamp == NOW ? 0 :
        new Block_Backend< Tag_Index_Global, Attic< Tag_Object_Global< Id_Type > > >
        (rman.get_transaction()->data_index(&attic_file_prop)));

  IdSetHybrid<typename Id_Type::Id_Type> new_ids;

  // Handle Key-Non-Value pairs
  for (std::vector< std::pair< std::string, std::string > >::const_iterator knvit = key_nvalues.begin();
      knvit != key_nvalues.end(); ++knvit)
  {
    if (timestamp == NOW)
    {
      std::set< Tag_Index_Global > tag_req = get_kv_req(knvit->first, knvit->second);
      for (typename Block_Backend< Tag_Index_Global, Tag_Object_Global< Id_Type > >::Discrete_Iterator
          it2(tags_db.discrete_begin(tag_req.begin(), tag_req.end()));
          !(it2 == tags_db.discrete_end()); ++it2)
        new_ids.set(it2.handle().id().val());
    }
    else
    {
      std::map< Id_Type, std::pair< uint64, Uint31_Index > > timestamp_per_id
          = collect_attic_kv(knvit, timestamp, tags_db, *attic_tags_db.obj);

      for (typename std::map< Id_Type, std::pair< uint64, Uint31_Index > >::const_iterator
          it = timestamp_per_id.begin(); it != timestamp_per_id.end(); ++it)
        new_ids.set(it->first.val());
    }
    rman.health_check(*this);
  }

  // Handle Key-Regular-Expression-Non-Value pairs
  for (std::vector< std::pair< std::string, Regular_Expression* > >::const_iterator knrit = key_nregexes.begin();
      knrit != key_nregexes.end(); ++knrit)
  {
    if (timestamp == NOW)
    {
      std::set< std::pair< Tag_Index_Global, Tag_Index_Global > > range_req = get_k_req(knrit->first);
      for (typename Block_Backend< Tag_Index_Global, Tag_Object_Global< Id_Type > >::Range_Iterator
          it2(tags_db.range_begin
          (Default_Range_Iterator< Tag_Index_Global >(range_req.begin()),
           Default_Range_Iterator< Tag_Index_Global >(range_req.end())));
          !(it2 == tags_db.range_end()); ++it2)
      {
        if (knrit->second->matches(it2.index().value))
          new_ids.set(it2.handle().id().val());
      }
    }
    else
    {
      std::map< Id_Type, std::pair< uint64, Uint31_Index > > timestamp_per_id
          = collect_attic_kregv(knrit, timestamp, tags_db, *attic_tags_db.obj);

      for (typename std::map< Id_Type, std::pair< uint64, Uint31_Index > >::const_iterator
          it = timestamp_per_id.begin(); it != timestamp_per_id.end(); ++it)
        new_ids.set(it->first.val());
    }
    rman.health_check(*this);
  }

  new_ids.sort_unique();

  return new_ids;
}


template< class Id_Type >
std::vector< Id_Type > Query_Statement::collect_non_ids
  (const File_Properties& file_prop, const File_Properties& attic_file_prop,
   Resource_Manager& rman, uint64 timestamp)
{
  if (key_nvalues.empty() && key_nregexes.empty())
    return std::vector< Id_Type >();

  Block_Backend< Tag_Index_Global, Tag_Object_Global< Id_Type > > tags_db
      (rman.get_transaction()->data_index(&file_prop));
  Optional< Block_Backend< Tag_Index_Global, Attic< Tag_Object_Global< Id_Type > > > > attic_tags_db
      (timestamp == NOW ? 0 :
        new Block_Backend< Tag_Index_Global, Attic< Tag_Object_Global< Id_Type > > >
        (rman.get_transaction()->data_index(&attic_file_prop)));

  std::vector< Id_Type > new_ids;

  // Handle Key-Non-Value pairs
  for (std::vector< std::pair< std::string, std::string > >::const_iterator knvit = key_nvalues.begin();
      knvit != key_nvalues.end(); ++knvit)
  {
    if (timestamp == NOW)
    {
      std::set< Tag_Index_Global > tag_req = get_kv_req(knvit->first, knvit->second);
      for (typename Block_Backend< Tag_Index_Global, Tag_Object_Global< Id_Type > >::Discrete_Iterator
          it2(tags_db.discrete_begin(tag_req.begin(), tag_req.end()));
          !(it2 == tags_db.discrete_end()); ++it2)
        new_ids.push_back(it2.handle().id());
    }
    else
    {
      std::map< Id_Type, std::pair< uint64, Uint31_Index > > timestamp_per_id
          = collect_attic_kv(knvit, timestamp, tags_db, *attic_tags_db.obj);

      for (typename std::map< Id_Type, std::pair< uint64, Uint31_Index > >::const_iterator
          it = timestamp_per_id.begin(); it != timestamp_per_id.end(); ++it)
        new_ids.push_back(it->first);
    }
    rman.health_check(*this);
  }

  // Handle Key-Regular-Expression-Non-Value pairs
  for (std::vector< std::pair< std::string, Regular_Expression* > >::const_iterator knrit = key_nregexes.begin();
      knrit != key_nregexes.end(); ++knrit)
  {
    if (timestamp == NOW)
    {
      std::set< std::pair< Tag_Index_Global, Tag_Index_Global > > range_req = get_k_req(knrit->first);
      for (typename Block_Backend< Tag_Index_Global, Tag_Object_Global< Id_Type > >::Range_Iterator
          it2(tags_db.range_begin
          (Default_Range_Iterator< Tag_Index_Global >(range_req.begin()),
           Default_Range_Iterator< Tag_Index_Global >(range_req.end())));
          !(it2 == tags_db.range_end()); ++it2)
      {
        if (knrit->second->matches(it2.index().value))
          new_ids.push_back(it2.handle().id());
      }
    }
    else
    {
      std::map< Id_Type, std::pair< uint64, Uint31_Index > > timestamp_per_id
          = collect_attic_kregv(knrit, timestamp, tags_db, *attic_tags_db.obj);

      for (typename std::map< Id_Type, std::pair< uint64, Uint31_Index > >::const_iterator
          it = timestamp_per_id.begin(); it != timestamp_per_id.end(); ++it)
        new_ids.push_back(it->first);
    }
    rman.health_check(*this);
  }

  sort(new_ids.begin(), new_ids.end());
  new_ids.erase(unique(new_ids.begin(), new_ids.end()), new_ids.end());

  return new_ids;
}


template< class Id_Type >
std::vector< Id_Type > Query_Statement::collect_non_ids
  (const File_Properties& file_prop, Resource_Manager& rman)
{
  if (key_nvalues.empty() && key_nregexes.empty())
    return std::vector< Id_Type >();

  Block_Backend< Tag_Index_Global, Id_Type > tags_db
      (rman.get_transaction()->data_index(&file_prop));

  std::vector< Id_Type > new_ids;

  // Handle Key-Non-Value pairs
  for (std::vector< std::pair< std::string, std::string > >::const_iterator knvit = key_nvalues.begin();
      knvit != key_nvalues.end(); ++knvit)
  {
    std::set< std::pair< Tag_Index_Global, Tag_Index_Global > > range_req = get_k_req(knvit->first);
    for (typename Block_Backend< Tag_Index_Global, Id_Type >::Range_Iterator
        it2(tags_db.range_begin
        (Default_Range_Iterator< Tag_Index_Global >(range_req.begin()),
         Default_Range_Iterator< Tag_Index_Global >(range_req.end())));
        !(it2 == tags_db.range_end()); ++it2)
    {
      if (it2.index().value == knvit->second)
        new_ids.push_back(it2.object());
    }

    rman.health_check(*this);
  }

  // Handle Key-Regular-Expression-Non-Value pairs
  for (std::vector< std::pair< std::string, Regular_Expression* > >::const_iterator knrit = key_nregexes.begin();
      knrit != key_nregexes.end(); ++knrit)
  {
    std::set< std::pair< Tag_Index_Global, Tag_Index_Global > > range_req = get_k_req(knrit->first);
    for (typename Block_Backend< Tag_Index_Global, Id_Type >::Range_Iterator
        it2(tags_db.range_begin
        (Default_Range_Iterator< Tag_Index_Global >(range_req.begin()),
         Default_Range_Iterator< Tag_Index_Global >(range_req.end())));
        !(it2 == tags_db.range_end()); ++it2)
    {
      if (it2.index().value != void_tag_value() && knrit->second->matches(it2.index().value))
        new_ids.push_back(it2.object());
    }

    rman.health_check(*this);
  }

  sort(new_ids.begin(), new_ids.end());
  new_ids.erase(unique(new_ids.begin(), new_ids.end()), new_ids.end());

  return new_ids;
}


void Query_Statement::get_elements_by_id_from_db
    (std::map< Uint31_Index, std::vector< Area_Skeleton > >& elements,
     const std::vector< Area_Skeleton::Id_Type >& ids, bool invert_ids,
     Resource_Manager& rman, File_Properties& file_prop)
{
  if (!invert_ids)
    collect_items_flat(*this, rman, file_prop,
			Id_Predicate< Area_Skeleton >(ids), elements);
  else
    collect_items_flat(*this, rman, file_prop,
			Not_Predicate< Area_Skeleton, Id_Predicate< Area_Skeleton > >
			(Id_Predicate< Area_Skeleton >(ids)), elements);
}


template< typename TIndex, typename TObject >
void clear_empty_indices
    (std::map< TIndex, std::vector< TObject > >& modify)
{
  for (typename std::map< TIndex, std::vector< TObject > >::iterator it = modify.begin();
      it != modify.end();)
  {
    if (!it->second.empty())
    {
      ++it;
      continue;
    }
    typename std::map< TIndex, std::vector< TObject > >::iterator next_it = it;
    if (++next_it == modify.end())
    {
      modify.erase(it);
      break;
    }
    else
    {
      TIndex idx = next_it->first;
      modify.erase(it);
      it = modify.find(idx);
    }
  }
}


template< typename Id_Type >
void filter_ids_by_ntags
  (const std::map< std::string, std::pair< std::vector< Regular_Expression* >, std::vector< std::string > > >& keys,
   const Block_Backend< Tag_Index_Local, Id_Type >& items_db,
   typename Block_Backend< Tag_Index_Local, Id_Type >::Range_Iterator& tag_it,
   uint32 coarse_index,
   std::vector< Id_Type >& new_ids)
{
  std::vector< Id_Type > removed_ids;
  std::string last_key, last_value;
  bool key_relevant = false;
  bool valid = false;
  std::map< std::string, std::pair< std::vector< Regular_Expression* >, std::vector< std::string > > >::const_iterator
      key_it = keys.begin();

  while ((!(tag_it == items_db.range_end())) &&
      (((tag_it.index().index) & 0x7fffff00) == coarse_index))
  {
    if (tag_it.index().key != last_key)
    {
      last_value = void_tag_value_space();

      if (key_relevant)
      {
        ++key_it;
        sort(removed_ids.begin(), removed_ids.end());
        removed_ids.erase(unique(removed_ids.begin(), removed_ids.end()), removed_ids.end());
        new_ids.erase(set_difference(new_ids.begin(), new_ids.end(),
                      removed_ids.begin(), removed_ids.end(), new_ids.begin()), new_ids.end());
      }
      key_relevant = false;

      last_key = tag_it.index().key;
      while (key_it != keys.end() && last_key > key_it->first)
        ++key_it;

      if (key_it == keys.end())
        break;

      if (last_key == key_it->first)
      {
        key_relevant = true;
        removed_ids.clear();
      }
    }

    if (key_relevant)
    {
      if (tag_it.index().value != last_value)
      {
        valid = false;
        for (std::vector< Regular_Expression* >::const_iterator rit = key_it->second.first.begin();
            rit != key_it->second.first.end(); ++rit)
          valid |= (tag_it.index().value != void_tag_value() && (*rit)->matches(tag_it.index().value));
        for (std::vector< std::string >::const_iterator rit = key_it->second.second.begin();
            rit != key_it->second.second.end(); ++rit)
          valid |= (*rit == tag_it.index().value);
        last_value = tag_it.index().value;
      }

      if (valid)
        removed_ids.push_back(tag_it.object());
    }

    ++tag_it;
  }
  while ((!(tag_it == items_db.range_end())) &&
      (((tag_it.index().index) & 0x7fffff00) == coarse_index))
    ++tag_it;

  sort(removed_ids.begin(), removed_ids.end());
  removed_ids.erase(unique(removed_ids.begin(), removed_ids.end()), removed_ids.end());
  new_ids.erase(set_difference(new_ids.begin(), new_ids.end(),
                removed_ids.begin(), removed_ids.end(), new_ids.begin()), new_ids.end());

  sort(new_ids.begin(), new_ids.end());
}


template< typename Id_Type >
void filter_ids_by_ntags
  (const std::map< std::string, std::pair< std::vector< Regular_Expression* >, std::vector< std::string > > >& keys,
   uint64 timestamp,
   const Block_Backend< Tag_Index_Local, Id_Type >& items_db,
   typename Block_Backend< Tag_Index_Local, Id_Type >::Range_Iterator& tag_it,
   const Block_Backend< Tag_Index_Local, Attic< Id_Type > >& attic_items_db,
   typename Block_Backend< Tag_Index_Local, Attic< Id_Type > >::Range_Iterator& attic_tag_it,
   uint32 coarse_index,
   std::vector< Id_Type >& new_ids)
{
  for (std::map< std::string, std::pair< std::vector< Regular_Expression* >, std::vector< std::string > > >::const_iterator
      key_it = keys.begin(); key_it != keys.end(); ++key_it)
  {
    std::map< Id_Type, std::pair< uint64, uint64 > > timestamps;
    for (typename std::vector< Id_Type >::const_iterator it = new_ids.begin(); it != new_ids.end(); ++it)
      timestamps[*it];

    while ((!(tag_it == items_db.range_end())) &&
        ((tag_it.index().index) & 0x7fffff00) == coarse_index &&
        tag_it.index().key < key_it->first)
      ++tag_it;
    while ((!(attic_tag_it == attic_items_db.range_end())) &&
        ((attic_tag_it.index().index) & 0x7fffff00) == coarse_index &&
        attic_tag_it.index().key < key_it->first)
      ++attic_tag_it;

    bool valid = false;
    std::string last_value = void_tag_value_space();
    while ((!(tag_it == items_db.range_end())) &&
        ((tag_it.index().index) & 0x7fffff00) == coarse_index &&
        tag_it.index().key == key_it->first)
    {
      if (std::binary_search(new_ids.begin(), new_ids.end(), tag_it.object()))
      {
        std::pair< uint64, uint64 >& timestamp_ref = timestamps[tag_it.object()];
        timestamp_ref.second = NOW;

        if (tag_it.index().value != last_value)
        {
          valid = false;
          for (std::vector< Regular_Expression* >::const_iterator rit = key_it->second.first.begin();
              rit != key_it->second.first.end(); ++rit)
            valid |= (tag_it.index().value != void_tag_value() && (*rit)->matches(tag_it.index().value));
          for (std::vector< std::string >::const_iterator rit = key_it->second.second.begin();
              rit != key_it->second.second.end(); ++rit)
            valid |= (*rit == tag_it.index().value);
          last_value = tag_it.index().value;
        }

        if (valid)
          timestamp_ref.first = NOW;
      }
      ++tag_it;
    }

    last_value = void_tag_value_space();
    while ((!(attic_tag_it == attic_items_db.range_end())) &&
        ((attic_tag_it.index().index) & 0x7fffff00) == coarse_index &&
        attic_tag_it.index().key == key_it->first)
    {
      if (std::binary_search(new_ids.begin(), new_ids.end(), Id_Type(attic_tag_it.object())))
      {
        std::pair< uint64, uint64 >& timestamp_ref = timestamps[attic_tag_it.object()];
        if (timestamp < attic_tag_it.object().timestamp &&
            (timestamp_ref.second == 0 || timestamp_ref.second > attic_tag_it.object().timestamp))
          timestamp_ref.second = attic_tag_it.object().timestamp;

        if (attic_tag_it.index().value != last_value)
        {
          valid = false;
          if (attic_tag_it.index().value != void_tag_value())
          {
            for (std::vector< Regular_Expression* >::const_iterator rit = key_it->second.first.begin();
                rit != key_it->second.first.end(); ++rit)
              valid |= (attic_tag_it.index().value != void_tag_value()
                  && (*rit)->matches(attic_tag_it.index().value));
            for (std::vector< std::string >::const_iterator rit = key_it->second.second.begin();
                rit != key_it->second.second.end(); ++rit)
              valid |= (*rit == attic_tag_it.index().value);
          }
          last_value = attic_tag_it.index().value;
        }

        if (valid && timestamp < attic_tag_it.object().timestamp &&
            (timestamp_ref.first == 0 || timestamp_ref.first > attic_tag_it.object().timestamp))
          timestamp_ref.first = attic_tag_it.object().timestamp;
      }
      ++attic_tag_it;
    }

    new_ids.clear();
    new_ids.reserve(timestamps.size());
    for (typename std::map< Id_Type, std::pair< uint64, uint64 > >::const_iterator
        it = timestamps.begin(); it != timestamps.end(); ++it)
    {
      if (!(0 < it->second.first && it->second.first <= it->second.second))
        new_ids.push_back(it->first);
    }
  }
  while ((!(tag_it == items_db.range_end())) &&
      ((tag_it.index().index) & 0x7fffff00) == coarse_index)
    ++tag_it;
  while ((!(attic_tag_it == attic_items_db.range_end())) &&
      ((attic_tag_it.index().index) & 0x7fffff00) == coarse_index)
    ++attic_tag_it;
}


template< class TIndex, class TObject >
void filter_by_ids(
    const std::map< uint32, std::vector< typename TObject::Id_Type > >& ids_by_coarse,
    std::map< TIndex, std::vector< TObject > >& items,
    std::map< TIndex, std::vector< Attic< TObject > > >* attic_items)
{
  std::map< TIndex, std::vector< TObject > > result;
  std::map< TIndex, std::vector< Attic< TObject > > > attic_result;

  typename std::map< TIndex, std::vector< TObject > >::const_iterator item_it = items.begin();

  if (!attic_items)
  {
    for (typename std::map< uint32, std::vector< typename TObject::Id_Type > >::const_iterator
        it = ids_by_coarse.begin(); it != ids_by_coarse.end(); ++it)
    {
      while ((item_it != items.end()) &&
          ((item_it->first.val() & 0x7fffff00) == it->first))
      {
        for (typename std::vector< TObject >::const_iterator eit = item_it->second.begin();
             eit != item_it->second.end(); ++eit)
        {
          if (binary_search(it->second.begin(), it->second.end(), eit->id))
            result[item_it->first.val()].push_back(*eit);
        }
        ++item_it;
      }
    }
  }
  else
  {
    typename std::map< TIndex, std::vector< Attic< TObject > > >::const_iterator attic_item_it
        = attic_items->begin();

    for (typename std::map< uint32, std::vector< typename TObject::Id_Type > >::const_iterator
        it = ids_by_coarse.begin(); it != ids_by_coarse.end(); ++it)
    {
      while ((item_it != items.end()) &&
          ((item_it->first.val() & 0x7fffff00) == it->first))
      {
        for (typename std::vector< TObject >::const_iterator eit = item_it->second.begin();
             eit != item_it->second.end(); ++eit)
        {
          if (binary_search(it->second.begin(), it->second.end(), eit->id))
            result[item_it->first.val()].push_back(*eit);
        }
        ++item_it;
      }

      while ((attic_item_it != attic_items->end()) &&
          ((attic_item_it->first.val() & 0x7fffff00) == it->first))
      {
        for (typename std::vector< Attic< TObject > >::const_iterator eit = attic_item_it->second.begin();
             eit != attic_item_it->second.end(); ++eit)
        {
          if (binary_search(it->second.begin(), it->second.end(), eit->id))
            attic_result[attic_item_it->first.val()].push_back(*eit);
        }
        ++attic_item_it;
      }
    }
  }

  items.swap(result);
  if (attic_items)
    attic_items->swap(attic_result);
}


template< class TIndex, class TObject >
void Query_Statement::filter_by_tags
    (std::map< TIndex, std::vector< TObject > >& items,
     std::map< TIndex, std::vector< Attic< TObject > > >* attic_items,
     uint64 timestamp, const File_Properties& file_prop, const File_Properties* attic_file_prop,
     Resource_Manager& rman, Transaction& transaction)
{
  if (keys.empty() && key_values.empty() && key_regexes.empty() && regkey_regexes.empty()
      && key_nregexes.empty() && key_nvalues.empty())
    return;

  // generate set of relevant coarse indices
  std::map< uint32, std::vector< typename TObject::Id_Type > > ids_by_coarse;
  generate_ids_by_coarse(ids_by_coarse, items);
  if (timestamp != NOW)
    generate_ids_by_coarse(ids_by_coarse, *attic_items);

  // formulate range query
  std::set< std::pair< Tag_Index_Local, Tag_Index_Local > > range_set;
  formulate_range_query(range_set, ids_by_coarse);

  // prepare straight keys
  std::map< std::string, std::pair< std::string, std::vector< Regular_Expression* > > > key_union;
  for (std::vector< std::string >::const_iterator it = keys.begin(); it != keys.end();
      ++it)
    key_union[*it];
  for (std::vector< std::pair< std::string, std::string > >::const_iterator it = key_values.begin(); it != key_values.end();
      ++it)
    key_union[it->first].first = it->second;
  for (std::vector< std::pair< std::string, Regular_Expression* > >::const_iterator
      it = key_regexes.begin(); it != key_regexes.end(); ++it)
    key_union[it->first].second.push_back(it->second);

  // iterate over the result
  std::map< TIndex, std::vector< TObject > > result;
  std::map< TIndex, std::vector< Attic< TObject > > > attic_result;
  uint coarse_count = 0;

  Block_Backend< Tag_Index_Local, typename TObject::Id_Type > items_db
      (transaction.data_index(&file_prop));
  typename Block_Backend< Tag_Index_Local, typename TObject::Id_Type >::Range_Iterator
    tag_it(items_db.range_begin
    (Default_Range_Iterator< Tag_Index_Local >(range_set.begin()),
     Default_Range_Iterator< Tag_Index_Local >(range_set.end())));

  if (timestamp == NOW)
  {
    for (typename std::map< uint32, std::vector< typename TObject::Id_Type > >::iterator
        it = ids_by_coarse.begin(); it != ids_by_coarse.end(); ++it)
    {
      if (++coarse_count >= 1024)
      {
        coarse_count = 0;
        rman.health_check(*this);
      }
      filter_ids_by_tags(key_union, regkey_regexes, items_db, tag_it, it->first, it->second);
    }
  }
  else
  {
    Block_Backend< Tag_Index_Local, Attic< typename TObject::Id_Type > > attic_items_db
        (transaction.data_index(attic_file_prop));
    typename Block_Backend< Tag_Index_Local, Attic< typename TObject::Id_Type > >::Range_Iterator
      attic_tag_it(attic_items_db.range_begin
      (Default_Range_Iterator< Tag_Index_Local >(range_set.begin()),
       Default_Range_Iterator< Tag_Index_Local >(range_set.end())));

    typename std::map< TIndex, std::vector< Attic< TObject > > >::const_iterator attic_item_it
        = attic_items->begin();

    for (typename std::map< uint32, std::vector< typename TObject::Id_Type > >::iterator
        it = ids_by_coarse.begin(); it != ids_by_coarse.end(); ++it)
    {
      if (++coarse_count >= 1024)
      {
        coarse_count = 0;
        rman.health_check(*this);
      }
      filter_ids_by_tags(key_union, regkey_regexes, timestamp, items_db, tag_it, attic_items_db, attic_tag_it,
                         it->first, it->second);
    }
  }

  if (key_nregexes.empty() && key_nvalues.empty())
  {
    filter_by_ids(ids_by_coarse, items, attic_items);
    return;
  }

  // prepare negated keys
  std::map< std::string, std::pair< std::vector< Regular_Expression* >, std::vector< std::string > > > nkey_union;
  for (std::vector< std::pair< std::string, Regular_Expression* > >::const_iterator
      it = key_nregexes.begin(); it != key_nregexes.end(); ++it)
    nkey_union[it->first].first.push_back(it->second);
  for (std::vector< std::pair< std::string, std::string > >::const_iterator
      it = key_nvalues.begin(); it != key_nvalues.end(); ++it)
    nkey_union[it->first].second.push_back(it->second);

  // iterate over the result
  result.clear();
  attic_result.clear();
  coarse_count = 0;
  typename Block_Backend< Tag_Index_Local, typename TObject::Id_Type >::Range_Iterator
      ntag_it(items_db.range_begin
      (Default_Range_Iterator< Tag_Index_Local >(range_set.begin()),
       Default_Range_Iterator< Tag_Index_Local >(range_set.end())));

  if (timestamp == NOW)
  {
    for (typename std::map< uint32, std::vector< typename TObject::Id_Type > >::iterator
        it = ids_by_coarse.begin(); it != ids_by_coarse.end(); ++it)
    {
      if (++coarse_count >= 1024)
      {
        coarse_count = 0;
        rman.health_check(*this);
      }
      filter_ids_by_ntags(nkey_union, items_db, ntag_it, it->first, it->second);
    }
  }
  else
  {
    Block_Backend< Tag_Index_Local, Attic< typename TObject::Id_Type > > attic_items_db
        (transaction.data_index(attic_file_prop));
    typename Block_Backend< Tag_Index_Local, Attic< typename TObject::Id_Type > >::Range_Iterator
      attic_ntag_it(attic_items_db.range_begin
      (Default_Range_Iterator< Tag_Index_Local >(range_set.begin()),
       Default_Range_Iterator< Tag_Index_Local >(range_set.end())));

    typename std::map< TIndex, std::vector< Attic< TObject > > >::const_iterator attic_item_it
        = attic_items->begin();

    for (typename std::map< uint32, std::vector< typename TObject::Id_Type > >::iterator
        it = ids_by_coarse.begin(); it != ids_by_coarse.end(); ++it)
    {
      if (++coarse_count >= 1024)
      {
        coarse_count = 0;
        rman.health_check(*this);
      }
      filter_ids_by_ntags(nkey_union, timestamp, items_db, ntag_it, attic_items_db, attic_ntag_it,
                          it->first, it->second);
    }
  }

  filter_by_ids(ids_by_coarse, items, attic_items);
}


template< class TIndex, class TObject >
void Query_Statement::filter_by_tags
    (std::map< TIndex, std::vector< TObject > >& items,
     const File_Properties& file_prop,
     Resource_Manager& rman, Transaction& transaction)
{
  if (keys.empty() && key_values.empty() && key_regexes.empty() && regkey_regexes.empty()
      && key_nregexes.empty() && key_nvalues.empty())
    return;

  // generate set of relevant coarse indices
  std::map< uint32, std::vector< typename TObject::Id_Type > > ids_by_coarse;
  generate_ids_by_coarse(ids_by_coarse, items);

  // formulate range query
  std::set< std::pair< Tag_Index_Local, Tag_Index_Local > > range_set;
  formulate_range_query(range_set, ids_by_coarse);

  // prepare straight keys
  std::map< std::string, std::pair< std::string, std::vector< Regular_Expression* > > > key_union;
  for (std::vector< std::string >::const_iterator it = keys.begin();
      it != keys.end(); ++it)
    key_union[*it];
  for (std::vector< std::pair< std::string, std::string > >::const_iterator it = key_values.begin();
      it != key_values.end(); ++it)
    key_union[it->first].first = it->second;
  for (std::vector< std::pair< std::string, Regular_Expression* > >::const_iterator
      it = key_regexes.begin(); it != key_regexes.end(); ++it)
    key_union[it->first].second.push_back(it->second);

  // iterate over the result
  std::map< TIndex, std::vector< TObject > > result;
  std::map< TIndex, std::vector< Attic< TObject > > > attic_result;
  uint coarse_count = 0;

  if (!key_union.empty() || !regkey_regexes.empty())
  {
    Block_Backend< Tag_Index_Local, typename TObject::Id_Type > items_db
        (transaction.data_index(&file_prop));
    typename Block_Backend< Tag_Index_Local, typename TObject::Id_Type >::Range_Iterator
        tag_it(items_db.range_begin
        (Default_Range_Iterator< Tag_Index_Local >(range_set.begin()),
        Default_Range_Iterator< Tag_Index_Local >(range_set.end())));

    typename std::map< TIndex, std::vector< TObject > >::const_iterator item_it
        = items.begin();

    {
      for (typename std::map< uint32, std::vector< typename TObject::Id_Type > >::iterator it = ids_by_coarse.begin();
          it != ids_by_coarse.end(); ++it)
      {
        if (++coarse_count >= 1024)
        {
          coarse_count = 0;
          rman.health_check(*this);
        }
        std::vector< typename TObject::Id_Type >& ids_by_coarse_ = it->second;

        filter_ids_by_tags_old(ids_by_coarse_, key_union, regkey_regexes, items_db, tag_it, it->first);

        while ((item_it != items.end()) &&
            ((item_it->first.val() & 0x7fffff00) == it->first))
        {
          for (typename std::vector< TObject >::const_iterator eit = item_it->second.begin();
              eit != item_it->second.end(); ++eit)
          {
            if (binary_search(ids_by_coarse_.begin(), ids_by_coarse_.end(), eit->id))
              result[item_it->first.val()].push_back(*eit);
          }
          ++item_it;
        }
      }
    }

    items.swap(result);
  }

  if (key_nregexes.empty() && key_nvalues.empty())
    return;

  // prepare negated keys
  std::map< std::string, std::pair< std::vector< Regular_Expression* >, std::vector< std::string > > > nkey_union;
  for (std::vector< std::pair< std::string, Regular_Expression* > >::const_iterator
      it = key_nregexes.begin(); it != key_nregexes.end(); ++it)
    nkey_union[it->first].first.push_back(it->second);
  for (std::vector< std::pair< std::string, std::string > >::const_iterator
      it = key_nvalues.begin(); it != key_nvalues.end(); ++it)
    nkey_union[it->first].second.push_back(it->second);

  // iterate over the result
  result.clear();
  attic_result.clear();
  coarse_count = 0;
  Block_Backend< Tag_Index_Local, typename TObject::Id_Type > items_db
      (transaction.data_index(&file_prop));
  typename Block_Backend< Tag_Index_Local, typename TObject::Id_Type >::Range_Iterator
      ntag_it(items_db.range_begin
      (Default_Range_Iterator< Tag_Index_Local >(range_set.begin()),
       Default_Range_Iterator< Tag_Index_Local >(range_set.end())));
  typename std::map< TIndex, std::vector< TObject > >::const_iterator item_it = items.begin();

  {
    for (typename std::map< uint32, std::vector< typename TObject::Id_Type > >::iterator it = ids_by_coarse.begin();
        it != ids_by_coarse.end(); ++it)
    {
      if (++coarse_count >= 1024)
      {
        coarse_count = 0;
        rman.health_check(*this);
      }
      std::vector< typename TObject::Id_Type >& ids_by_coarse_ = it->second;

      sort(ids_by_coarse_.begin(), ids_by_coarse_.end());

      filter_ids_by_ntags(nkey_union, items_db, ntag_it, it->first, ids_by_coarse_);

      while ((item_it != items.end()) &&
          ((item_it->first.val() & 0x7fffff00) == it->first))
      {
        for (typename std::vector< TObject >::const_iterator eit = item_it->second.begin();
             eit != item_it->second.end(); ++eit)
        {
          if (binary_search(ids_by_coarse_.begin(), ids_by_coarse_.end(), eit->id))
            result[item_it->first.val()].push_back(*eit);
        }
        ++item_it;
      }
    }
  }

  items.swap(result);
}


void Query_Statement::filter_by_tags(std::map< Uint31_Index, std::vector< Derived_Structure > >& items)
{
  for (std::map< Uint31_Index, std::vector< Derived_Structure > >::iterator it_idx = items.begin();
      it_idx != items.end(); ++it_idx)
  {
    std::vector< Derived_Structure > result;
    for (std::vector< Derived_Structure >::const_iterator it_elem = it_idx->second.begin();
        it_elem != it_idx->second.end(); ++it_elem)
    {
      std::vector< std::pair< std::string, std::string > >::const_iterator it_kv = key_values.begin();
      for (; it_kv != key_values.end(); ++it_kv)
      {
        std::vector< std::pair< std::string, std::string > >::const_iterator it_tag = it_elem->tags.begin();
        for (; it_tag != it_elem->tags.end(); ++it_tag)
        {
          if (it_tag->first == it_kv->first && it_tag->second == it_kv->second)
            break;
        }
        if (it_tag == it_elem->tags.end())
          break;
      }
      if (it_kv != key_values.end())
        continue;

      std::vector< std::string >::const_iterator it_k = keys.begin();
      for (; it_k != keys.end(); ++it_k)
      {
        std::vector< std::pair< std::string, std::string > >::const_iterator it_tag = it_elem->tags.begin();
        for (; it_tag != it_elem->tags.end(); ++it_tag)
        {
          if (it_tag->first == *it_k)
            break;
        }
        if (it_tag == it_elem->tags.end())
          break;
      }
      if (it_k != keys.end())
        continue;

      std::vector< std::pair< std::string, Regular_Expression* > >::const_iterator it_kr = key_regexes.begin();
      for (; it_kr != key_regexes.end(); ++it_kr)
      {
        std::vector< std::pair< std::string, std::string > >::const_iterator it_tag = it_elem->tags.begin();
        for (; it_tag != it_elem->tags.end(); ++it_tag)
        {
          if (it_tag->first == it_kr->first && it_kr->second->matches(it_tag->second))
            break;
        }
        if (it_tag == it_elem->tags.end())
          break;
      }
      if (it_kr != key_regexes.end())
        continue;

      std::vector< std::pair< std::string, std::string > >::const_iterator it_nkv = key_nvalues.begin();
      for (; it_nkv != key_nvalues.end(); ++it_nkv)
      {
        std::vector< std::pair< std::string, std::string > >::const_iterator it_tag = it_elem->tags.begin();
        for (; it_tag != it_elem->tags.end(); ++it_tag)
        {
          if (it_tag->first == it_nkv->first && it_tag->second == it_nkv->second)
            break;
        }
        if (it_tag != it_elem->tags.end())
          break;
      }
      if (it_nkv != key_nvalues.end())
        continue;

      std::vector< std::pair< std::string, Regular_Expression* > >::const_iterator it_nkr = key_nregexes.begin();
      for (; it_nkr != key_nregexes.end(); ++it_nkr)
      {
        std::vector< std::pair< std::string, std::string > >::const_iterator it_tag = it_elem->tags.begin();
        for (; it_tag != it_elem->tags.end(); ++it_tag)
        {
          if (it_tag->first == it_nkr->first && it_nkr->second->matches(it_tag->second))
            break;
        }
        if (it_tag != it_elem->tags.end())
          break;
      }
      if (it_nkr != key_nregexes.end())
        continue;

      result.push_back(*it_elem);
    }
    result.swap(it_idx->second);
  }
}

struct comparator
{
    template< typename Id_Type >
    bool operator()( Id_Type const& lhs, std::pair< Id_Type, Uint31_Index > const& rhs) const {
        return lhs < rhs.first;
   }

    template< typename Id_Type >
    bool operator()( std::pair< Id_Type, Uint31_Index > const& lhs, Id_Type const& rhs) const {
        return lhs.first < rhs;
    }
};



template< typename Skeleton, typename Id_Type, typename Index >
void Query_Statement::progress_1(std::vector< Id_Type >& ids, std::vector< Index >& range_vec,
                                 bool& invert_ids, uint64 timestamp,
                                 Answer_State& answer_state, Query_Filter_Strategy& check_keys_late,
                                 const File_Properties& file_prop, const File_Properties& attic_file_prop,
                                 Resource_Manager& rman)
{
  ids.clear();
  range_vec.clear();
  if (!key_values.empty()
      || (check_keys_late != prefer_ranges
          && (!keys.empty() || !key_regexes.empty() || !regkey_regexes.empty())))
  {
    bool result_valid = true;
    std::vector< std::pair< Id_Type, Uint31_Index > > id_idxs =
        collect_ids< Skeleton, Id_Type >(file_prop, attic_file_prop, rman, timestamp, check_keys_late, result_valid);

    if (!key_nvalues.empty() || (check_keys_late != prefer_ranges && !key_nregexes.empty()))
    {
/*
      std::vector< Id_Type > non_ids
                = collect_non_ids< Id_Type >(file_prop, attic_file_prop, rman, timestamp);

      std::vector< std::pair< Id_Type, Uint31_Index > > diff_ids(id_idxs.size());
      diff_ids.erase(std::set_difference(id_idxs.begin(), id_idxs.end(), non_ids.begin(), non_ids.end(),
                     diff_ids.begin(), comparator{}), diff_ids.end());

      ids.clear();
      range_vec.clear();
      for (typename std::vector< std::pair< Id_Type, Uint31_Index > >::const_iterator it = diff_ids.begin();
          it != diff_ids.end(); ++it)
      {
        ids.push_back(it->first);
        range_vec.push_back(it->second);
      }
*/


      auto non_ids  = collect_non_ids_hybrid< Id_Type >(file_prop, attic_file_prop, rman, timestamp);
      ids.clear();
      range_vec.clear();
      for (typename std::vector< std::pair< Id_Type, Uint31_Index > >::const_iterator it = id_idxs.begin();
          it != id_idxs.end(); ++it)
      {
        if (!(non_ids.get(it->first.val()))) {
          ids.push_back(it->first);
          range_vec.push_back(it->second);
        }
      }

    }
    else
    {
      Uint31_Index prev_second(0u);

      ids.reserve(id_idxs.size());

      for (typename std::vector< std::pair< Id_Type, Uint31_Index > >::const_iterator it = id_idxs.begin();
          it != id_idxs.end(); ++it)
      {
        ids.push_back(it->first);

        if (!(it->second == prev_second))
          range_vec.push_back(it->second);

        prev_second = it->second;
      }
    }

    if (ids.empty() && result_valid)
      answer_state = data_collected;
  }
  else if ((!key_nvalues.empty() || !key_nregexes.empty()) && check_keys_late != prefer_ranges)
  {
    invert_ids = true;
    std::vector< Id_Type > id_idxs =
        collect_non_ids< Id_Type >(file_prop, attic_file_prop, rman, timestamp);
    for (typename std::vector< Id_Type >::const_iterator it = id_idxs.begin();
        it != id_idxs.end(); ++it)
      ids.push_back(*it);
  }
}


template< class Id_Type >
void Query_Statement::progress_1(std::vector< Id_Type >& ids, bool& invert_ids,
                                 Answer_State& answer_state, Query_Filter_Strategy check_keys_late,
                                 const File_Properties& file_prop,
                                 Resource_Manager& rman)
{
  if (!key_values.empty()
      || (check_keys_late != prefer_ranges
          && (!keys.empty() || !key_regexes.empty() || !regkey_regexes.empty())))
  {
    collect_ids< Id_Type >(file_prop, rman, check_keys_late).swap(ids);
    if (!key_nvalues.empty() || !key_nregexes.empty() || !regkey_nregexes.empty())
    {
      std::vector< Id_Type > non_ids = collect_non_ids< Id_Type >(file_prop, rman);
      std::vector< Id_Type > diff_ids(ids.size(), Id_Type());
      diff_ids.erase(set_difference(ids.begin(), ids.end(), non_ids.begin(), non_ids.end(),
                     diff_ids.begin()), diff_ids.end());
      ids.swap(diff_ids);
    }
    if (ids.empty())
      answer_state = data_collected;
  }
  else if ((!key_nvalues.empty() || !key_nregexes.empty() || !regkey_nregexes.empty())
      && check_keys_late != prefer_ranges)
  {
    invert_ids = true;
    collect_non_ids< Id_Type >(file_prop, rman).swap(ids);
  }
}


template< class Id_Type >
void Query_Statement::collect_nodes(std::vector< Id_Type >& ids,
				 bool& invert_ids, Answer_State& answer_state, Set& into,
				 Resource_Manager& rman)
{
  for (std::vector< Query_Constraint* >::iterator it = constraints.begin();
      it != constraints.end() && answer_state < data_collected; ++it)
  {
    if ((*it)->collect_nodes(rman, into, ids, invert_ids))
      answer_state = data_collected;
  }
}


template< typename Id_Type >
void Query_Statement::collect_elems(int type, std::vector< Id_Type >& ids,
				 bool& invert_ids, Answer_State& answer_state, Set& into,
				 Resource_Manager& rman)
{
  for (std::vector< Query_Constraint* >::iterator it = constraints.begin();
      it != constraints.end() && answer_state < data_collected; ++it)
  {
    if ((*it)->collect(rman, into, type, ids, invert_ids))
      answer_state = data_collected;
  }
}


void Query_Statement::collect_elems(Answer_State& answer_state, Set& into, Resource_Manager& rman)
{
  for (std::vector< Query_Constraint* >::iterator it = constraints.begin();
      it != constraints.end() && answer_state < data_collected; ++it)
  {
    if ((*it)->collect(rman, into))
      answer_state = data_collected;
  }
}


template< typename Index >
std::set< std::pair< Index, Index > > intersect_ranges
    (const std::set< std::pair< Index, Index > >& range_a,
     std::vector< Index >& range_vec)
{
  std::set< std::pair< Index, Index > > result;

  unsigned long long sum = 0;
  for (typename std::set< std::pair< Index, Index > >::const_iterator it = range_a.begin();
       it != range_a.end(); ++it)
    sum += difference(it->first, it->second);

  if (sum/256 < range_vec.size())
    return range_a;

  std::sort(range_vec.begin(), range_vec.end());

  typename std::set< std::pair< Index, Index > >::const_iterator it_a = range_a.begin();
  typename std::vector< Index >::const_iterator it_vec = range_vec.begin();

  while (it_a != range_a.end() && it_vec != range_vec.end())
  {
    if (!(it_a->first < Index(it_vec->val() + 0x100)))
      ++it_vec;
    else if (!(*it_vec < it_a->second))
      ++it_a;
    else if (Index(it_vec->val() + 0x100) < it_a->second)
    {
      result.insert(std::make_pair(std::max(it_a->first, *it_vec), Index(it_vec->val() + 0x100)));
      ++it_vec;
    }
    else
    {
      result.insert(std::make_pair(std::max(it_a->first, *it_vec), it_a->second));
      ++it_a;
    }
  }

  return result;
}


void Query_Statement::apply_all_filters(
    Resource_Manager& rman, uint64 timestamp, Query_Filter_Strategy check_keys_late, Set& into)
{
  set_progress(5);
  rman.health_check(*this);

  for (std::vector< Query_Constraint* >::iterator it = constraints.begin();
      it != constraints.end(); ++it)
    (*it)->filter(rman, into);

  set_progress(6);
  rman.health_check(*this);

  filter_attic_elements(rman, timestamp, into.nodes, into.attic_nodes);
  filter_attic_elements(rman, timestamp, into.ways, into.attic_ways);
  filter_attic_elements(rman, timestamp, into.relations, into.attic_relations);

  set_progress(7);
  rman.health_check(*this);

  if (check_keys_late == prefer_ranges)
  {
    filter_by_tags(into.nodes, &into.attic_nodes, timestamp,
                   *osm_base_settings().NODE_TAGS_LOCAL, attic_settings().NODE_TAGS_LOCAL,
		   rman, *rman.get_transaction());
    filter_by_tags(into.ways, &into.attic_ways, timestamp,
                   *osm_base_settings().WAY_TAGS_LOCAL, attic_settings().WAY_TAGS_LOCAL,
		   rman, *rman.get_transaction());
    filter_by_tags(into.relations, &into.attic_relations, timestamp,
                   *osm_base_settings().RELATION_TAGS_LOCAL, attic_settings().RELATION_TAGS_LOCAL,
		   rman, *rman.get_transaction());
    if (rman.get_area_transaction())
      filter_by_tags(into.areas,
                     *area_settings().AREA_TAGS_LOCAL,
		     rman, *rman.get_transaction());
  }

  set_progress(8);
  rman.health_check(*this);

  for (std::vector< Query_Constraint* >::iterator it = constraints.begin();
      it != constraints.end(); ++it)
    (*it)->filter(*this, rman, into);
}


void Query_Statement::execute(Resource_Manager& rman)
{
  Cpu_Timer cpu(rman, 1);

  Answer_State node_answer_state = nothing;
  Answer_State way_answer_state = nothing;
  Answer_State relation_answer_state = nothing;
  Answer_State area_answer_state = nothing;
  Answer_State derived_answer_state = nothing;

  Set into;
  Set filtered;
  uint64 timestamp = rman.get_desired_timestamp();
  if (timestamp == 0)
    timestamp = NOW;

  set_progress(1);
  rman.health_check(*this);

  Query_Filter_Strategy check_keys_late = ids_required;
  for (std::vector< Query_Constraint* >::iterator it = constraints.begin(); it != constraints.end(); ++it)
    check_keys_late = std::max(check_keys_late, (*it)->delivers_data(rman));

  {
    std::vector< Node::Id_Type > node_ids;
    std::vector< Way::Id_Type > way_ids;
    std::vector< Relation::Id_Type > relation_ids;
    std::vector< Area_Skeleton::Id_Type > area_ids;
    bool invert_ids = false;

    std::set< std::pair< Uint32_Index, Uint32_Index > > range_req_32;
    std::vector< Uint32_Index > range_vec_32;
    std::set< std::pair< Uint31_Index, Uint31_Index > > way_range_req_31;
    std::vector< Uint31_Index > way_range_vec_31;
    std::set< std::pair< Uint31_Index, Uint31_Index > > relation_range_req_31;
    std::vector< Uint31_Index > relation_range_vec_31;

    if (type & QUERY_NODE)
    {
      progress_1< Node_Skeleton, Node::Id_Type, Uint32_Index >(
	  node_ids, range_vec_32, invert_ids, timestamp, node_answer_state, check_keys_late,
          *osm_base_settings().NODE_TAGS_GLOBAL, *attic_settings().NODE_TAGS_GLOBAL, rman);
      collect_nodes(node_ids, invert_ids, node_answer_state, into, rman);
    }
    if (type & QUERY_WAY)
    {
      progress_1< Way_Skeleton, Way::Id_Type, Uint31_Index >(
	  way_ids, way_range_vec_31, invert_ids, timestamp, way_answer_state, check_keys_late,
          *osm_base_settings().WAY_TAGS_GLOBAL, *attic_settings().WAY_TAGS_GLOBAL, rman);
      collect_elems(QUERY_WAY, way_ids, invert_ids, way_answer_state, into, rman);
    }
    if (type & QUERY_RELATION)
    {
      progress_1< Relation_Skeleton, Relation::Id_Type, Uint31_Index >(
	  relation_ids, relation_range_vec_31, invert_ids, timestamp, relation_answer_state, check_keys_late,
          *osm_base_settings().RELATION_TAGS_GLOBAL,  *attic_settings().RELATION_TAGS_GLOBAL, rman);
      collect_elems(QUERY_RELATION, relation_ids, invert_ids, relation_answer_state, into, rman);
    }
    if (type & QUERY_DERIVED)
    {
      collect_elems(derived_answer_state, into, rman);
      filter_by_tags(into.deriveds);
    }
    if (type & QUERY_AREA)
    {
      progress_1(area_ids, invert_ids, area_answer_state,
		 check_keys_late, *area_settings().AREA_TAGS_GLOBAL, rman);
      collect_elems(QUERY_AREA, area_ids, invert_ids, area_answer_state, into, rman);
    }

    set_progress(2);
    rman.health_check(*this);

    if (type & QUERY_NODE)
    {
      for (std::vector< Query_Constraint* >::iterator it = constraints.begin();
          it != constraints.end() && node_answer_state < data_collected; ++it)
      {
	std::vector< Node::Id_Type > constraint_node_ids;
	if ((*it)->get_node_ids(rman, constraint_node_ids))
	{
	  if (node_ids.empty())
	    node_ids.swap(constraint_node_ids);
	  else
	  {
	    std::vector< Node::Id_Type > new_ids(node_ids.size());
	    new_ids.erase(std::set_intersection(
	        node_ids.begin(), node_ids.end(), constraint_node_ids.begin(), constraint_node_ids.end(),
	        new_ids.begin()), new_ids.end());
	    node_ids.swap(new_ids);
	  }

	  if (node_ids.empty())
	    node_answer_state = data_collected;
	}
      }
    }
    if (type & QUERY_WAY)
    {
      for (std::vector< Query_Constraint* >::iterator it = constraints.begin();
          it != constraints.end() && way_answer_state < data_collected; ++it)
      {
	std::vector< Way::Id_Type > constraint_way_ids;
	if ((*it)->get_way_ids(rman, constraint_way_ids))
	{
	  if (way_ids.empty())
	    way_ids.swap(constraint_way_ids);
	  else
	  {
	    std::vector< Way::Id_Type > new_ids(way_ids.size());
	    new_ids.erase(std::set_intersection(
	        way_ids.begin(), way_ids.end(), constraint_way_ids.begin(), constraint_way_ids.end(),
	        new_ids.begin()), new_ids.end());
	    way_ids.swap(new_ids);
	  }

	  if (way_ids.empty())
	    way_answer_state = data_collected;
	}
      }
    }
    if (type & QUERY_RELATION)
    {
      for (std::vector< Query_Constraint* >::iterator it = constraints.begin();
          it != constraints.end() && relation_answer_state < data_collected; ++it)
      {
	std::vector< Relation::Id_Type > constraint_relation_ids;
	if ((*it)->get_relation_ids(rman, constraint_relation_ids))
	{
	  if (relation_ids.empty())
	    relation_ids.swap(constraint_relation_ids);
	  else
	  {
	    std::vector< Relation::Id_Type > new_ids(relation_ids.size());
	    new_ids.erase(std::set_intersection(
	        relation_ids.begin(), relation_ids.end(), constraint_relation_ids.begin(), constraint_relation_ids.end(),
	        new_ids.begin()), new_ids.end());
	    relation_ids.swap(new_ids);
	  }

	  if (relation_ids.empty())
	    relation_answer_state = data_collected;
	}
      }
    }
    if (type & QUERY_AREA)
    {
      for (std::vector< Query_Constraint* >::iterator it = constraints.begin();
          it != constraints.end() && area_answer_state < data_collected; ++it)
      {
	std::vector< Area_Skeleton::Id_Type > constraint_area_ids;
	if ((*it)->get_area_ids(rman, constraint_area_ids))
	{
	  if (area_ids.empty())
	    area_ids.swap(constraint_area_ids);
	  else
	  {
	    std::vector< Area_Skeleton::Id_Type > new_ids(area_ids.size());
	    new_ids.erase(std::set_intersection(
	        area_ids.begin(), area_ids.end(), constraint_area_ids.begin(), constraint_area_ids.end(),
	        new_ids.begin()), new_ids.end());
	    area_ids.swap(new_ids);
	  }

	  if (area_ids.empty())
	    area_answer_state = data_collected;
	}
      }
    }

    if (type & QUERY_NODE)
    {
      for (std::vector< Query_Constraint* >::iterator it = constraints.begin();
          it != constraints.end() && node_answer_state < data_collected; ++it)
      {
        std::set< std::pair< Uint32_Index, Uint32_Index > > range_req;
        if ((*it)->get_ranges(rman, range_req))
        {
          if (node_answer_state < ranges_collected)
            range_req.swap(range_req_32);
          else
            intersect_ranges(range_req_32, range_req).swap(range_req_32);
	  node_answer_state = ranges_collected;
        }
      }

      if (!range_vec_32.empty())
      {
        if (node_answer_state < ranges_collected)
        {
          node_answer_state = ranges_collected;
          range_req_32.clear();
          range_req_32.insert(std::make_pair(Uint32_Index(0u), Uint32_Index(0xffffffff)));
          intersect_ranges(range_req_32, range_vec_32).swap(range_req_32);
        }
        else
          intersect_ranges(range_req_32, range_vec_32).swap(range_req_32);
      }
    }
    if ((type & QUERY_WAY) && way_answer_state < data_collected)
    {
      for (std::vector< Query_Constraint* >::iterator it = constraints.begin();
          it != constraints.end(); ++it)
      {
        std::set< std::pair< Uint31_Index, Uint31_Index > > range_req;
	if ((*it)->get_way_ranges(rman, range_req))
        {
          if (way_answer_state < ranges_collected)
            range_req.swap(way_range_req_31);
          else
            intersect_ranges(way_range_req_31, range_req).swap(way_range_req_31);
          if (way_answer_state < ranges_collected)
            way_answer_state = ranges_collected;
        }
      }

      if (!way_range_vec_31.empty())
      {
        if (way_answer_state < ranges_collected)
        {
          way_answer_state = ranges_collected;
          way_range_req_31.clear();
          way_range_req_31.insert(std::make_pair(Uint31_Index(0u), Uint31_Index(0xffffffff)));
          intersect_ranges(way_range_req_31, way_range_vec_31).swap(way_range_req_31);
        }
        else
          intersect_ranges(way_range_req_31, way_range_vec_31).swap(way_range_req_31);
      }
    }
    if ((type & QUERY_RELATION) && relation_answer_state < data_collected)
    {
      for (std::vector< Query_Constraint* >::iterator it = constraints.begin();
          it != constraints.end(); ++it)
      {
        std::set< std::pair< Uint31_Index, Uint31_Index > > range_req;
	if ((*it)->get_relation_ranges(rman, range_req))
        {
          if (relation_answer_state < ranges_collected)
            range_req.swap(relation_range_req_31);
          else
            intersect_ranges(relation_range_req_31, range_req).swap(relation_range_req_31);
          if (relation_answer_state < ranges_collected)
            relation_answer_state = ranges_collected;
        }
      }

      if (!relation_range_vec_31.empty())
      {
        if (relation_answer_state < ranges_collected)
        {
          relation_answer_state = ranges_collected;
          relation_range_req_31.clear();
          relation_range_req_31.insert(std::make_pair(Uint31_Index(0u), Uint31_Index(0xffffffff)));
          intersect_ranges(relation_range_req_31, relation_range_vec_31).swap(relation_range_req_31);
        }
        else
          intersect_ranges(relation_range_req_31, relation_range_vec_31).swap(relation_range_req_31);
      }
    }

    set_progress(3);
    rman.health_check(*this);

    if (type & QUERY_NODE)
    {
      for (std::vector< Query_Constraint* >::iterator it = constraints.begin();
          it != constraints.end() && node_answer_state < data_collected; ++it)
      {
	if ((*it)->get_data(*this, rman, into, range_req_32, node_ids, invert_ids))
	  node_answer_state = data_collected;
      }
    }
    if (type & QUERY_WAY)
    {
      for (std::vector< Query_Constraint* >::iterator it = constraints.begin();
          it != constraints.end() && way_answer_state < data_collected; ++it)
      {
	if ((*it)->get_data(*this, rman, into, way_range_req_31, type & QUERY_WAY, way_ids, invert_ids))
	  way_answer_state = data_collected;
      }
    }
    if (type & QUERY_RELATION)
    {
      for (std::vector< Query_Constraint* >::iterator it = constraints.begin();
          it != constraints.end() && relation_answer_state < data_collected; ++it)
      {
	if ((*it)->get_data(*this, rman, into, relation_range_req_31, type & QUERY_RELATION,
            relation_ids, invert_ids))
	  relation_answer_state = data_collected;
      }
    }
    if (type & QUERY_AREA)
    {
      for (std::vector< Query_Constraint* >::iterator it = constraints.begin();
          it != constraints.end() && area_answer_state < data_collected; ++it)
      {
	if ((*it)->get_data(*this, rman, into, std::set< std::pair< Uint31_Index, Uint31_Index > >(),
            type & QUERY_AREA, area_ids, invert_ids))
	  area_answer_state = data_collected;
      }
    }

    set_progress(4);
    rman.health_check(*this);

    if (type & QUERY_NODE)
    {
      if (node_answer_state == nothing && node_ids.empty())
        runtime_error("Filters too weak in query statement: specify in addition a bbox, a tag filter, or similar.");
    }
    if (type & QUERY_WAY)
    {
      if (way_answer_state == nothing && way_ids.empty())
        runtime_error("Filters too weak in query statement: specify in addition a bbox, a tag filter, or similar.");
    }
    if (type & QUERY_RELATION)
    {
      if (relation_answer_state == nothing && relation_ids.empty())
        runtime_error("Filters too weak in query statement: specify in addition a bbox, a tag filter, or similar.");
    }
    if (type & QUERY_AREA)
    {
      if (area_answer_state == nothing && area_ids.empty())
        runtime_error("Filters too weak in query statement: specify in addition a bbox, a tag filter, or similar.");
    }

    if (type & QUERY_NODE)
    {
      if (node_answer_state < data_collected)
      {
        if (range_req_32.empty() && node_answer_state < ranges_collected && !invert_ids)
	{
          std::vector< Uint32_Index > req = get_indexes_< Uint32_Index, Node_Skeleton >(node_ids, rman);
          for (std::vector< Uint32_Index >::const_iterator it = req.begin(); it != req.end(); ++it)
            range_req_32.insert(std::make_pair(*it, ++Uint32_Index(*it)));
	}
        // TODO: <<<<<<<<<<<<<<<<< PROTOTYPE <<<<<<<<<<<<<<<<<<<<<<<<<<<
        
        bool use_nodes_tagged = !(key_values.empty() && keys.empty() && key_regexes.empty() && regkey_regexes.empty());

        if (range_req_32.empty())
          ::get_elements_by_id_from_db< Uint32_Index, Node_Skeleton >
              (into.nodes, into.attic_nodes,
               node_ids, invert_ids, range_req_32, 0, *this, rman,
               (use_nodes_tagged ? *osm_base_settings().NODES_TAGGED : *osm_base_settings().NODES), *attic_settings().NODES);
        else
        {
          Uint32_Index min_idx = range_req_32.begin()->first;
          while (::get_elements_by_id_from_db< Uint32_Index, Node_Skeleton >
              (into.nodes, into.attic_nodes,
              node_ids, invert_ids, range_req_32, &min_idx, *this, rman,
               (use_nodes_tagged ? *osm_base_settings().NODES_TAGGED : *osm_base_settings().NODES), *attic_settings().NODES))
          {
            Set to_filter;
            to_filter.nodes.swap(into.nodes);
            to_filter.attic_nodes.swap(into.attic_nodes);
            apply_all_filters(rman, timestamp, check_keys_late, to_filter);
            indexed_set_union(filtered.nodes, to_filter.nodes);
            indexed_set_union(filtered.attic_nodes, to_filter.attic_nodes);
          }
        }

        // TODO: <<<<<<<<<<<<<<<<< PROTOTYPE <<<<<<<<<<<<<<<<<<<<<<<<<<<
      }
    }
    if (type & QUERY_WAY)
    {
      if (way_answer_state < data_collected)
      {
        if (way_range_req_31.empty() && way_answer_state < ranges_collected && !invert_ids)
	{
          std::vector< Uint31_Index > req = get_indexes_< Uint31_Index, Way_Skeleton >(way_ids, rman);
          for (std::vector< Uint31_Index >::const_iterator it = req.begin(); it != req.end(); ++it)
            way_range_req_31.insert(std::make_pair(*it, inc(*it)));
	}
	if (way_range_req_31.empty())
  	  ::get_elements_by_id_from_db< Uint31_Index, Way_Skeleton >
	      (into.ways, into.attic_ways,
               way_ids, invert_ids, way_range_req_31, 0, *this, rman,
               *osm_base_settings().WAYS, *attic_settings().WAYS);
        else
        {
          Uint31_Index min_idx = way_range_req_31.begin()->first;
          while (::get_elements_by_id_from_db< Uint31_Index, Way_Skeleton >
              (into.ways, into.attic_ways,
              way_ids, invert_ids, way_range_req_31, &min_idx, *this, rman,
              *osm_base_settings().WAYS, *attic_settings().WAYS))
          {
            Set to_filter;
            to_filter.ways.swap(into.ways);
            to_filter.attic_ways.swap(into.attic_ways);
            apply_all_filters(rman, timestamp, check_keys_late, to_filter);
            indexed_set_union(filtered.ways, to_filter.ways);
            indexed_set_union(filtered.attic_ways, to_filter.attic_ways);
          }
        }             
             
             
      }
    }
    if (type & QUERY_RELATION)
    {
      if (relation_answer_state < data_collected)
      {
        if (relation_range_req_31.empty() && relation_answer_state < ranges_collected && !invert_ids)
	{
          std::vector< Uint31_Index > req = get_indexes_< Uint31_Index, Relation_Skeleton >(relation_ids, rman);
          for (std::vector< Uint31_Index >::const_iterator it = req.begin(); it != req.end(); ++it)
            relation_range_req_31.insert(std::make_pair(*it, inc(*it)));
	}
	if (relation_range_req_31.empty())
	  ::get_elements_by_id_from_db< Uint31_Index, Relation_Skeleton >
	      (into.relations, into.attic_relations,
               relation_ids, invert_ids, relation_range_req_31, 0, *this, rman,
               *osm_base_settings().RELATIONS, *attic_settings().RELATIONS);
        else
        {
          Uint31_Index min_idx = relation_range_req_31.begin()->first;
          while (::get_elements_by_id_from_db< Uint31_Index, Relation_Skeleton >
              (into.relations, into.attic_relations,
              relation_ids, invert_ids, relation_range_req_31, &min_idx, *this, rman,
              *osm_base_settings().RELATIONS, *attic_settings().RELATIONS))
          {
            Set to_filter;
            to_filter.relations.swap(into.relations);
            to_filter.attic_relations.swap(into.attic_relations);
            apply_all_filters(rman, timestamp, check_keys_late, to_filter);
            indexed_set_union(filtered.relations, to_filter.relations);
            indexed_set_union(filtered.attic_relations, to_filter.attic_relations);
          }
        }               
      }
    }
    if (type & QUERY_AREA)
    {
      if (area_answer_state < data_collected)
	get_elements_by_id_from_db(into.areas, area_ids, invert_ids, rman, *area_settings().AREAS);
    }
  }

  apply_all_filters(rman, timestamp, check_keys_late, into);
  indexed_set_union(into.nodes, filtered.nodes);
  indexed_set_union(into.attic_nodes, filtered.attic_nodes);
  indexed_set_union(into.ways, filtered.ways);
  indexed_set_union(into.attic_ways, filtered.attic_ways);
  indexed_set_union(into.relations, filtered.relations);
  indexed_set_union(into.attic_relations, filtered.attic_relations);
  indexed_set_union(into.areas, filtered.areas);
  indexed_set_union(into.deriveds, filtered.deriveds);
  
  set_progress(9);
  rman.health_check(*this);

  clear_empty_indices(into.nodes);
  clear_empty_indices(into.attic_nodes);
  clear_empty_indices(into.ways);
  clear_empty_indices(into.attic_ways);
  clear_empty_indices(into.relations);
  clear_empty_indices(into.deriveds);
  clear_empty_indices(into.areas);

  transfer_output(rman, into);
  rman.health_check(*this);
}

//-----------------------------------------------------------------------------

Generic_Statement_Maker< Has_Kv_Statement > Has_Kv_Statement::statement_maker("has-kv");

Has_Kv_Statement::Has_Kv_Statement
    (int line_number_, const std::map< std::string, std::string >& input_attributes, Parsed_Query& global_settings)
    : Statement(line_number_), regex(0), key_regex(0), straight(true), case_sensitive(false)
{
  std::map< std::string, std::string > attributes;

  attributes["k"] = "";
  attributes["regk"] = "";
  attributes["v"] = "";
  attributes["regv"] = "";
  attributes["modv"] = "";
  attributes["case"] = "sensitive";

  eval_attributes_array(get_name(), attributes, input_attributes);

  key = attributes["k"];
  value = attributes["v"];

  if (key.empty() && attributes["regk"].empty())
  {
    std::ostringstream temp("");
    temp<<"For the attribute \"k\" of the element \"has-kv\""
	<<" the only allowed values are non-empty strings.";
    add_static_error(temp.str());
  }

  if (attributes["case"] != "ignore")
  {
    if (attributes["case"] != "sensitive")
      add_static_error("For the attribute \"case\" of the element \"has-kv\""
	  " the only allowed values are \"sensitive\" or \"ignore\".");
    case_sensitive = true;
  }

  if (attributes["regk"] != "")
  {
    if (key != "")
    {
      std::ostringstream temp("");
      temp<<"In the element \"has-kv\" only one of the attributes \"k\" and \"regk\""
            " can be nonempty.";
      add_static_error(temp.str());
    }
    if (value != "")
    {
      std::ostringstream temp("");
      temp<<"In the element \"has-kv\" the attribute \"regk\" must be combined with \"regv\".";
      add_static_error(temp.str());
    }

    try
    {
      key_regex = Regular_Expression_Factory::get_regexp_engine(global_settings.get_regexp_engine(), attributes["regk"], case_sensitive);
      key = attributes["regk"];
    }
    catch (Regular_Expression_Error e)
    {
      add_static_error("Invalid regular expression: \"" + attributes["regk"] + "\"");
    }
  }

  if (attributes["regv"] != "")
  {
    if (value != "")
    {
      std::ostringstream temp("");
      temp<<"In the element \"has-kv\" only one of the attributes \"v\" and \"regv\""
            " can be nonempty.";
      add_static_error(temp.str());
    }

    try
    {
      regex = Regular_Expression_Factory::get_regexp_engine(global_settings.get_regexp_engine(), attributes["regv"], case_sensitive);
      value = attributes["regv"];
    }
    catch (Regular_Expression_Error e)
    {
      add_static_error("Invalid regular expression: \"" + attributes["regv"] + "\"");
    }
  }

  if (attributes["modv"].empty() || attributes["modv"] == "not")
  {
    if (attributes["modv"] == "not")
    {
      if (attributes["regk"].empty())
        straight = false;
      else
	add_static_error("In the element \"has-kv\" regular expressions on keys cannot be combined"
	  " with negation.");
    }
  }
  else
  {
    std::ostringstream temp("");
    temp<<"In the element \"has-kv\" the attribute \"modv\" can only be empty or std::set to \"not\".";
    add_static_error(temp.str());
  }
}


Has_Kv_Statement::~Has_Kv_Statement()
{
  delete regex;
  delete key_regex;
}
