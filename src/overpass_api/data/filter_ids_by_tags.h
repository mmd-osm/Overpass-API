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

#ifndef DE__OSM3S___OVERPASS_API__DATA__FILTER_IDS_BY_TAGS_H
#define DE__OSM3S___OVERPASS_API__DATA__FILTER_IDS_BY_TAGS_H


template< typename Id_Type >
void filter_ids_by_tags
  (const std::map< std::string, std::pair< std::string, std::vector< Regular_Expression* > > >& keys,
   const std::vector< std::pair< Regular_Expression*, Regular_Expression* > >& key_regexes,
   const Block_Backend< Tag_Index_Local, Id_Type >& items_db,
   typename Block_Backend< Tag_Index_Local, Id_Type >::Range_Iterator& tag_it,
   uint32 coarse_index,
   std::vector< Id_Type >& new_ids,
   int bitmask = 0x7fffff00)
{
  std::string last_key, last_value;
  bool key_relevant = false;
  bool valid = false;
  auto key_it = keys.begin();

  std::vector< Id_Type > old_ids;
  std::vector< uint64 > matched_by_key_regexes;
  std::vector< uint64 > matched_by_both_regexes;
  std::vector< std::vector< Id_Type > > matched_ids(key_regexes.size());

  while ((!(tag_it == items_db.range_end())) &&
      (((tag_it.index_handle().get_index()) & bitmask) == coarse_index))
  {
    if (tag_it.index_handle().get_key() != last_key)
    {
      last_value = void_tag_value_space();

      if (key_relevant)
        ++key_it;
      key_relevant = false;

      if (key_it == keys.end() && key_regexes.empty())
	break;

      last_key = tag_it.index_handle().get_key();
      if (key_it != keys.end() && last_key >= key_it->first)
      {
	if (last_key > key_it->first)
          // There are keys missing for all objects with this index. Drop all.
	  break;

	key_relevant = true;
	old_ids.clear();
        old_ids.swap(new_ids);
        if (!is_sorted(old_ids.begin(), old_ids.end()))
          sort(old_ids.begin(), old_ids.end());
      }

      matched_by_key_regexes.clear();
      for (uint64 i = 0; i < key_regexes.size(); ++i)
      {
	if (key_regexes[i].first->matches(last_key))
	  matched_by_key_regexes.push_back(i);
      }
    }

    auto object_value = tag_it.index_handle().get_value();

    if (object_value != last_value)
    {
      if (key_relevant)
      {
	valid = key_it->second.first.empty() || object_value == key_it->second.first;
	for (auto rit = key_it->second.second.begin();
	    valid && rit != key_it->second.second.end(); ++rit)
	  valid &= (*rit)->matches(object_value);
      }

      last_value = object_value;

      matched_by_both_regexes.clear();
      for (std::vector< uint64 >::const_iterator reg_it = matched_by_key_regexes.begin();
	  reg_it != matched_by_key_regexes.end(); ++reg_it)
      {
	if (last_value != void_tag_value() && key_regexes[*reg_it].second->matches(last_value))
	  matched_by_both_regexes.push_back(*reg_it);
      }
    }

    Id_Type object_id = tag_it.handle().id();

    if (key_relevant && valid && std::binary_search(old_ids.begin(), old_ids.end(), object_id))
      new_ids.push_back(object_id);

    if (!matched_by_both_regexes.empty() &&
	(std::binary_search(old_ids.begin(), old_ids.end(), object_id) ||
	 std::binary_search(new_ids.begin(), new_ids.end(), object_id)))
    {
      for (std::vector< uint64 >::const_iterator reg_it = matched_by_both_regexes.begin();
	  reg_it != matched_by_both_regexes.end(); ++reg_it)
	matched_ids[*reg_it].push_back(object_id);
    }

    ++tag_it;
  }
  while ((!(tag_it == items_db.range_end())) &&
      (((tag_it.index_handle().get_index()) & bitmask) == coarse_index))
    ++tag_it;

  if (key_relevant && key_it != keys.end())
    ++key_it;
  if (key_it != keys.end())
    // There are keys missing for all objects with this index. Drop all.
    new_ids.clear();

  if (!is_sorted(new_ids.begin(), new_ids.end()))
    std::sort(new_ids.begin(), new_ids.end());
  new_ids.erase(std::unique(new_ids.begin(), new_ids.end()), new_ids.end());

  for (typename std::vector< std::vector< Id_Type > >::const_iterator it = matched_ids.begin();
      it != matched_ids.end(); ++it)
  {
    old_ids.swap(new_ids);
    new_ids.clear();

    for (auto it2 = it->begin(); it2 != it->end(); ++it2)
    {
      if (std::binary_search(old_ids.begin(), old_ids.end(), *it2))
	new_ids.push_back(*it2);
    }

    if (!is_sorted(new_ids.begin(), new_ids.end()))
      std::sort(new_ids.begin(), new_ids.end());
    new_ids.erase(std::unique(new_ids.begin(), new_ids.end()), new_ids.end());
  }
}


template< typename Id_Type >
void filter_ids_by_tags_old
  (std::vector< Id_Type >& ids_by_coarse,
   const std::map< std::string, std::pair< std::string, std::vector< Regular_Expression* > > >& keys,
   const std::vector< std::pair< Regular_Expression*, Regular_Expression* > >& key_regexes,
   const Block_Backend< Tag_Index_Local, Id_Type >& items_db,
   typename Block_Backend< Tag_Index_Local, Id_Type >::Range_Iterator& tag_it,
   uint32 coarse_index)
{
  std::vector< Id_Type > new_ids = ids_by_coarse;

  filter_ids_by_tags(keys, key_regexes, items_db, tag_it, coarse_index & 0x7fffff00, new_ids, 0xffffff00);

  new_ids.swap(ids_by_coarse);

  filter_ids_by_tags(keys, key_regexes, items_db, tag_it, coarse_index | 0x80000000, new_ids, 0xffffff00);

  std::vector< Id_Type > old_ids;
  old_ids.swap(ids_by_coarse);
  set_union(old_ids.begin(), old_ids.end(), new_ids.begin(), new_ids.end(), back_inserter(ids_by_coarse));
}


template< typename Id_Type >
struct Tag_Entry_Listener
{
public:
  virtual bool notify_key(const std::string& key) = 0;
  virtual bool value_relevant(const std::string& value) const = 0;
  virtual void eval_id(Id_Type id, timestamp_t timestamp, bool value_relevant) = 0;
  virtual void filter_ids(std::vector< Id_Type >& new_ids) = 0;
  virtual ~Tag_Entry_Listener() = default;
};


template< typename Id_Type >
struct Tag_Entry_Listener_Value_Regex : public Tag_Entry_Listener< Id_Type >
{
public:
  Tag_Entry_Listener_Value_Regex(
      const std::string& key, const std::string& value, const std::vector< Regular_Expression* >& conditions,
      const std::vector< Id_Type >& old_ids)
      : key_(key), value_(value), conditions_(conditions), old_ids_(&old_ids) {}

  ~Tag_Entry_Listener_Value_Regex() override = default;

  bool notify_key(const std::string& key) override { return key == key_; }

  bool value_relevant(const std::string& value) const override
  {
    bool valid = value_.empty() || value_ == value;
    for (auto it = conditions_.begin(); valid && it != conditions_.end();
        ++it)
      valid &= (*it)->matches(value);
    return valid;
  }

  void eval_id(Id_Type id, timestamp_t timestamp, bool value_relevant) override
  {
    if (std::binary_search(old_ids_->begin(), old_ids_->end(), id))
    {
      std::pair< timestamp_t, timestamp_t >& timestamp_ref = timestamps[id];
      if (timestamp_ref.second == 0 || timestamp <= timestamp_ref.second)
      {
        timestamp_ref.second = timestamp;

        if (value_relevant)
	  timestamp_ref.first = timestamp;
      }
    }
  }

  void filter_ids(std::vector< Id_Type >& new_ids) override
  {
    std::vector< Id_Type > result;
    for (typename std::vector< Id_Type >::const_iterator it = new_ids.begin(); it != new_ids.end(); ++it)
    {
      std::pair< timestamp_t, timestamp_t >& timestamp_ref = timestamps[*it];
      if (0 < timestamp_ref.first && timestamp_ref.first <= timestamp_ref.second)
        result.push_back(*it);
    }
    std::sort(result.begin(), result.end());
    result.swap(new_ids);
  }

private:
  std::string key_;
  std::string value_;
  std::vector< Regular_Expression* > conditions_;
  const std::vector< Id_Type >* old_ids_;
  std::map< Id_Type, std::pair< timestamp_t, timestamp_t > > timestamps;
};


template< typename Id_Type >
struct Tag_Entry_Listener_Key_Regex : public Tag_Entry_Listener< Id_Type >
{
public:
  Tag_Entry_Listener_Key_Regex(
      Regular_Expression* key, Regular_Expression* value,
      const std::vector< Id_Type >& old_ids)
      : key_(key), value_(value), old_ids_(&old_ids) {}

  ~Tag_Entry_Listener_Key_Regex() override = default;

  bool notify_key(const std::string& key) override
  {
    commit_ids();
    return key_->matches(key);
  }

  bool value_relevant(const std::string& value) const override
  {
    return value != void_tag_value() && value_->matches(value);
  }

  void eval_id(Id_Type id, timestamp_t timestamp, bool value_relevant) override
  {
    if (std::binary_search(old_ids_->begin(), old_ids_->end(), id))
    {
      std::pair< timestamp_t, timestamp_t >& timestamp_ref = timestamps[id];
      timestamp_ref.second = timestamp;

      if (value_relevant)
	timestamp_ref.first = timestamp;
    }
  }

  void filter_ids(std::vector< Id_Type >& new_ids) override
  {
    commit_ids();

    std::sort(new_ids_.begin(), new_ids_.end());
    new_ids_.erase(std::unique(new_ids_.begin(), new_ids_.end()), new_ids_.end());

    std::vector< Id_Type > result(new_ids_.size());
    result.erase(std::set_intersection(new_ids.begin(), new_ids.end(), new_ids_.begin(), new_ids_.end(),
	result.begin()), result.end());

    result.swap(new_ids);
  }

  void commit_ids()
  {
    for (typename std::map< Id_Type, std::pair< timestamp_t, timestamp_t > >::const_iterator it = timestamps.begin();
	it != timestamps.end(); ++it)
    {
      if (0 < it->second.first && it->second.first <= it->second.second)
        new_ids_.push_back(it->first);
    }

    timestamps.clear();
  }

private:
  Regular_Expression* key_;
  Regular_Expression* value_;
  const std::vector< Id_Type >* old_ids_;
  std::vector< Id_Type > new_ids_;
  std::map< Id_Type, std::pair< timestamp_t, timestamp_t > > timestamps;
};


template< typename Id_Type >
void update_listeners_keys(
    std::vector< Tag_Entry_Listener< Id_Type >* >& tag_listeners,
    std::vector< std::pair< uint64, bool > >& relevant_listeners,
    const std::string& current_key)
{
  relevant_listeners.clear();
  for (uint64 i = 0; i < tag_listeners.size(); ++i)
  {
    if (tag_listeners[i]->notify_key(current_key))
      relevant_listeners.push_back(std::make_pair(i, false));
  }
}


template< typename Id_Type >
void update_listeners_values(
    std::vector< Tag_Entry_Listener< Id_Type >* >& tag_listeners,
    std::vector< std::pair< uint64, bool > >& relevant_listeners,
    const std::string& current_value)
{
  for (uint64 i = 0; i < relevant_listeners.size(); ++i)
    relevant_listeners[i].second = (current_value != void_tag_value() &&
	tag_listeners[relevant_listeners[i].first]->value_relevant(current_value));
}


template< typename Id_Type >
void filter_ids_by_tags
  (const std::map< std::string, std::pair< std::string, std::vector< Regular_Expression* > > >& keys,
   const std::vector< std::pair< Regular_Expression*, Regular_Expression* > >& key_regexes,
   uint64 timestamp,
   const Block_Backend< Tag_Index_Local, Id_Type >& items_db,
   typename Block_Backend< Tag_Index_Local, Id_Type >::Range_Iterator& tag_it,
   const Block_Backend< Tag_Index_Local, Attic< Id_Type > >& attic_items_db,
   typename Block_Backend< Tag_Index_Local, Attic< Id_Type > >::Range_Iterator& attic_tag_it,
   uint32 coarse_index,
   std::vector< Id_Type >& new_ids)
{
  std::vector< Tag_Entry_Listener_Key_Regex< Id_Type > > tag_key_listeners;
  for (auto it = key_regexes.begin(); it != key_regexes.end(); ++it)
    tag_key_listeners.push_back(Tag_Entry_Listener_Key_Regex< Id_Type >(it->first, it->second, new_ids));

  std::vector< Tag_Entry_Listener_Value_Regex< Id_Type > > tag_value_listeners;
  for (auto key_it = keys.begin();
       key_it != keys.end(); ++key_it)
    tag_value_listeners.push_back(Tag_Entry_Listener_Value_Regex< Id_Type >(
        key_it->first, key_it->second.first, key_it->second.second, new_ids));

  std::vector< Tag_Entry_Listener< Id_Type >* > tag_listeners;
  for (auto it = tag_key_listeners.begin();
       it != tag_key_listeners.end(); ++it)
    tag_listeners.push_back(&*it);
  for (auto it = tag_value_listeners.begin();
       it != tag_value_listeners.end(); ++it)
    tag_listeners.push_back(&*it);

  std::string current_key = void_tag_value();
  std::string current_value;
  std::vector< std::pair< uint64, bool > > relevant_listeners;
  while ((!(tag_it == items_db.range_end())) &&
      ((tag_it.index_handle().get_index()) & 0x7fffff00) == coarse_index)
  {
    if (current_key != tag_it.index_handle().get_key())
    {
      current_key = tag_it.index_handle().get_key();
      update_listeners_keys(tag_listeners, relevant_listeners, current_key);
      current_value = void_tag_value_space();
    }

    if (current_value != tag_it.index_handle().get_value())
    {
      current_value = tag_it.index_handle().get_value();
      update_listeners_values(tag_listeners, relevant_listeners, current_value);
    }

    if (!relevant_listeners.empty())
    {
      auto object_id = tag_it.handle().id();

      for (uint64 i = 0; i < relevant_listeners.size(); ++i)
	tag_listeners[relevant_listeners[i].first]->eval_id(
	    object_id, NOW, relevant_listeners[i].second);
    }

    ++tag_it;
  }

  current_key = void_tag_value();
  while ((!(attic_tag_it == attic_items_db.range_end())) &&
      ((attic_tag_it.index_handle().get_index()) & 0x7fffff00) == coarse_index)
  {
    if (current_key != attic_tag_it.index_handle().get_key())
    {
      current_key = attic_tag_it.index_handle().get_key();
      update_listeners_keys(tag_listeners, relevant_listeners, current_key);
      current_value = void_tag_value_space();
    }

    if (current_value != attic_tag_it.index_handle().get_value())
    {
      current_value = attic_tag_it.index_handle().get_value();
      update_listeners_values(tag_listeners, relevant_listeners, current_value);
    }

    auto object_timestamp = attic_tag_it.handle().get_timestamp();

    if (!relevant_listeners.empty() && timestamp < object_timestamp)
    {
      auto object_id = attic_tag_it.handle().id();

      for (uint64 i = 0; i < relevant_listeners.size(); ++i)
	tag_listeners[relevant_listeners[i].first]->eval_id(
	    object_id, object_timestamp, relevant_listeners[i].second);
    }

    ++attic_tag_it;
  }

  for (uint64 i = 0; i < tag_listeners.size(); ++i)
    tag_listeners[i]->filter_ids(new_ids);
}

#endif
