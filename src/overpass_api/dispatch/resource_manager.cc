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

#include "resource_manager.h"
#include "../data/abstract_processing.h"
#include "../data/utils.h"
#include "../statements/statement.h"

#include <sstream>


uint64 eval_set(const Set& set_)
{
  return eval_map(set_.nodes) + eval_map(set_.ways) + eval_map(set_.relations)
      + eval_map(set_.attic_nodes) + eval_map(set_.attic_ways)
      + eval_map(set_.attic_relations) + eval_map(set_.areas);
}


Set* Runtime_Stack_Frame::get_set(const std::string& set_name)
{
  auto it = sets.find(set_name);
  if (it != sets.end())
    return &it->second;

  auto it_diff = diff_sets.find(set_name);
  if (it_diff != diff_sets.end())
    return 0;

  if (parent)
    return parent->get_set(set_name);

  return 0;
}


Diff_Set* Runtime_Stack_Frame::get_diff_set(const std::string& set_name)
{
  auto it = diff_sets.find(set_name);
  if (it != diff_sets.end())
    return &it->second;

  auto it_set = sets.find(set_name);
  if (it_set != sets.end())
    return 0;

  if (parent)
    return parent->get_diff_set(set_name);

  return 0;
}


void Runtime_Stack_Frame::swap_set(const std::string& set_name, Set& set_)
{
  diff_sets.erase(set_name);
  key_values.erase(set_name);
  Set& to_swap = sets[set_name];
  set_.swap(to_swap);
  size_per_set[set_name] = eval_set(to_swap);
}


void Runtime_Stack_Frame::swap_diff_set(const std::string& set_name, Diff_Set& set_)
{
  sets.erase(set_name);
  key_values.erase(set_name);
  Diff_Set& to_swap = diff_sets[set_name];
  set_.swap(to_swap);
}


const std::string* Runtime_Stack_Frame::get_value(const std::string& set_name, const std::string& key)
{
  const std::map< std::string, std::string >* key_values = get_set_key_values(set_name);
  if (key_values)
  {
    auto kvit = key_values->find(key);
    return kvit == key_values->end() ? 0 : &kvit->second;
  }
  return 0;
}


const std::map< std::string, std::string >* Runtime_Stack_Frame::get_set_key_values(const std::string& set_name)
{
  auto it = key_values.find(set_name);
  if (it != key_values.end())
    return &it->second;

  auto it_set = sets.find(set_name);
  if (it_set != sets.end())
    return 0;
  auto it_diff = diff_sets.find(set_name);
  if (it_diff != diff_sets.end())
    return 0;

  if (parent)
    return parent->get_set_key_values(set_name);

  return 0;
}


void Runtime_Stack_Frame::set_value(const std::string& set_name, const std::string& key, const std::string& value)
{
  key_values[set_name][key] = value;
}


void Runtime_Stack_Frame::erase_set(const std::string& set_name)
{
  sets.erase(set_name);
  diff_sets.erase(set_name);
  key_values.erase(set_name);
  size_per_set[set_name] = 0;
}


void Runtime_Stack_Frame::clear_sets()
{
  sets.clear();
  diff_sets.clear();
  key_values.clear();
  size_per_set.clear();
}


void Runtime_Stack_Frame::copy_outward(const std::string& inner_set_name, const std::string& top_set_name)
{
  Set* from = 0;

  if (parent)
    from = parent->get_set(inner_set_name);

  if (from)
  {
    size_per_set[top_set_name] = eval_set(*from);
    sets[top_set_name] = *from;
  }
  else
  {
    size_per_set[top_set_name] = 0;
    sets[top_set_name].clear();
  }
  diff_sets.erase(top_set_name);
  key_values.erase(top_set_name);
}


void Runtime_Stack_Frame::move_outward(const std::string& inner_set_name, const std::string& top_set_name)
{
  Runtime_Stack_Frame* source = parent;

  while (source && source->parent)
  {
    auto it = source->sets.find(inner_set_name);
    if (it != source->sets.end())
      break;

    auto it_diff = source->diff_sets.find(inner_set_name);
    if (it_diff != source->diff_sets.end())
      break;

    source = source->parent;
  }

  if (source)
  {
    size_per_set[top_set_name] = source->size_per_set[inner_set_name];
    sets[top_set_name].clear();
    if (source == parent)
      source->swap_set(inner_set_name, sets[top_set_name]);
    else
      sets[top_set_name] = source->sets[inner_set_name];
  }
  diff_sets.erase(top_set_name);
  key_values.erase(top_set_name);
}

void Runtime_Stack_Frame::union_current_frame(const std::string& top_set_name, const std::string& inner_set_name)
{
  uint64 result_size_increase = 0;
  Set* source = get_set(top_set_name);

  if (source)
  {
    Set& target = sets[inner_set_name];

    result_size_increase += indexed_set_union(target.nodes, source->nodes);
    result_size_increase += indexed_set_union(target.attic_nodes, source->attic_nodes);

    result_size_increase += indexed_set_union(target.ways, source->ways);
    result_size_increase += indexed_set_union(target.attic_ways, source->attic_ways);

    result_size_increase += indexed_set_union(target.relations, source->relations);
    result_size_increase += indexed_set_union(target.attic_relations, source->attic_relations);

    result_size_increase += indexed_set_union(target.areas, source->areas);
    result_size_increase += indexed_set_union(target.deriveds, source->deriveds);

    result_size_increase += indexed_set_union(target.area_blocks, source->area_blocks);

    size_per_set[inner_set_name] += result_size_increase;   // eval_set(target);
  }
  diff_sets.erase(inner_set_name);

}


bool Runtime_Stack_Frame::union_inward(const std::string& top_set_name, const std::string& inner_set_name)
{
  uint64 result_size_increase = 0;
  Set* source = get_set(top_set_name);

  if (source && parent)
  {
    Set& target = parent->sets[inner_set_name];

    result_size_increase += indexed_set_union(target.nodes, source->nodes);
    result_size_increase += indexed_set_union(target.attic_nodes, source->attic_nodes);

    result_size_increase += indexed_set_union(target.ways, source->ways);
    result_size_increase += indexed_set_union(target.attic_ways, source->attic_ways);

    result_size_increase += indexed_set_union(target.relations, source->relations);
    result_size_increase += indexed_set_union(target.attic_relations, source->attic_relations);

    result_size_increase += indexed_set_union(target.areas, source->areas);
    result_size_increase += indexed_set_union(target.deriveds, source->deriveds);

    result_size_increase += indexed_set_union(target.area_blocks, source->area_blocks);

    parent->size_per_set[inner_set_name] += result_size_increase;   // eval_set(target);
  }

  if (parent) {
    parent->diff_sets.erase(inner_set_name);
    parent->key_values.erase(top_set_name);
  }

  return result_size_increase > 0;
}


void Runtime_Stack_Frame::copy_inward(const std::string& top_set_name, const std::string& inner_set_name)
{
  if (!parent)
    return;

  auto it = sets.find(top_set_name);
  if (it == sets.end())
  {
    Set* source = parent->get_set(top_set_name);
    if (!source)
      parent->sets[inner_set_name] = Set();
    else if (source != &parent->sets[inner_set_name])
      parent->sets[inner_set_name] = *source;
  }
  else
    parent->sets[inner_set_name] = it->second;

  parent->diff_sets.erase(inner_set_name);
  parent->key_values.erase(inner_set_name);
}


void Runtime_Stack_Frame::substract_from_inward(const std::string& top_set_name, const std::string& inner_set_name)
{
  Set* source = get_set(top_set_name);

  if (source && parent)
  {
    Set& target = parent->sets[inner_set_name];

    indexed_set_difference(target.nodes, source->nodes);
    indexed_set_difference(target.ways, source->ways);
    indexed_set_difference(target.relations, source->relations);

    indexed_set_difference(target.attic_nodes, source->attic_nodes);
    indexed_set_difference(target.attic_ways, source->attic_ways);
    indexed_set_difference(target.attic_relations, source->attic_relations);

    indexed_set_difference(target.areas, source->areas);
    indexed_set_difference(target.deriveds, source->deriveds);

    parent->size_per_set[inner_set_name] = eval_set(target);
  }

  if (parent) {
    parent->diff_sets.erase(inner_set_name);
    parent->key_values.erase(inner_set_name);
  }
}


void Runtime_Stack_Frame::move_all_inward()
{
  if (!parent)
    return;

  for (auto it = sets.begin(); it != sets.end(); ++it)
  {
    parent->swap_set(it->first, it->second);
    parent->diff_sets.erase(it->first);
    parent->key_values.erase(it->first);
  }
}


void Runtime_Stack_Frame::move_all_inward_except(const std::string& set_name)
{
  if (!parent)
    return;

  for (auto it = sets.begin(); it != sets.end(); ++it)
  {
    if (it->first != set_name)
    {
      parent->swap_set(it->first, it->second);
      parent->diff_sets.erase(it->first);
      parent->key_values.erase(it->first);
    }
  }
}


uint64 Runtime_Stack_Frame::total_size()
{
  uint64 result = 0;

  for (std::map< std::string, uint64 >::const_iterator it = size_per_set.begin(); it != size_per_set.end(); ++it)
    result += it->second;

  if (parent)
    result += parent->total_size();

  return result;
}


std::vector< std::pair< uint, uint > > Runtime_Stack_Frame::stack_progress() const
{
  std::vector< std::pair< uint, uint > > result;
  if (parent)
    parent->stack_progress().swap(result);

  result.push_back(std::make_pair(loop_count, loop_size));

  return result;
}

bool Runtime_Stack_Frame::set_exists_in_parents(const std::string& inner_set_name) const
{
  Runtime_Stack_Frame* source = parent;

  while (source != nullptr)
  {
    auto it = source->sets.find(inner_set_name);
    if (it != source->sets.end())
      return true;

    source = source->parent;
  }

  return false;
}


Resource_Manager::Resource_Manager(
    Transaction& transaction_, Parsed_Query* global_settings_,
    Error_Output* error_output_)
      : transaction(&transaction_), error_output(error_output_),
        area_transaction(0), area_updater_(0),
        global_settings(global_settings_), global_settings_owned(false),
	start_time(time(NULL)), last_ping_time(0), last_report_time(0),
	max_allowed_time(0), max_allowed_space(0)
{
  if (!global_settings)
  {
    global_settings = new Parsed_Query();
    global_settings_owned = true;
  }

  runtime_stack.push_back(new Runtime_Stack_Frame());
}


Resource_Manager::Resource_Manager(
    Transaction& transaction_, Parsed_Query& global_settings_, Error_Output* error_output_,
    Transaction& area_transaction_, Area_Usage_Listener* area_updater__)
    : transaction(&transaction_), error_output(error_output_),
      area_transaction(&area_transaction_), area_updater_(area_updater__),
      global_settings(&global_settings_), global_settings_owned(false),
      start_time(time(NULL)), last_ping_time(0), last_report_time(0),
      max_allowed_time(0), max_allowed_space(0)
{
  runtime_stack.push_back(new Runtime_Stack_Frame());
}


const Set* Resource_Manager::get_set(const std::string& set_name)
{
  if (runtime_stack.empty())
    return 0;

  return runtime_stack.back()->get_set(set_name);
}


const Diff_Set* Resource_Manager::get_diff_set(const std::string& set_name)
{
  if (runtime_stack.empty())
    return 0;

  return runtime_stack.back()->get_diff_set(set_name);
}


void Resource_Manager::swap_set(const std::string& set_name, Set& set_)
{
  if (runtime_stack.empty())
    runtime_stack.push_back(new Runtime_Stack_Frame());

  sort(set_);
  runtime_stack.back()->swap_set(set_name, set_);
}


void Resource_Manager::swap_diff_set(const std::string& set_name, Diff_Set& set_)
{
  if (runtime_stack.empty())
    runtime_stack.push_back(new Runtime_Stack_Frame());

  runtime_stack.back()->swap_diff_set(set_name, set_);
}


const std::string* Resource_Manager::get_value(const std::string& set_name, const std::string& key)
{
  if (runtime_stack.empty())
    return 0;

  return runtime_stack.back()->get_value(set_name, key);
}


const std::map< std::string, std::string >* Resource_Manager::get_set_key_values(const std::string& set_name)
{
  if (runtime_stack.empty())
    return 0;

  return runtime_stack.back()->get_set_key_values(set_name);
}


void Resource_Manager::set_value(const std::string& set_name, const std::string& key, const std::string& value)
{
  if (runtime_stack.empty())
    runtime_stack.push_back(new Runtime_Stack_Frame());

  runtime_stack.back()->set_value(set_name, key, value);
}


void Resource_Manager::erase_set(const std::string& set_name)
{
  if (runtime_stack.empty())
    runtime_stack.push_back(new Runtime_Stack_Frame());

  runtime_stack.back()->erase_set(set_name);
}


void Resource_Manager::clear_sets()
{
  if (!runtime_stack.empty())
    runtime_stack.back()->clear_sets();
}



void Resource_Manager::push_stack_frame()
{
  runtime_stack.push_back(new Runtime_Stack_Frame(runtime_stack.empty() ? 0 : runtime_stack.back()));
}


void Resource_Manager::copy_outward(const std::string& inner_set_name, const std::string& top_set_name)
{
  if (!runtime_stack.empty())
    runtime_stack.back()->copy_outward(inner_set_name, top_set_name);
}


void Resource_Manager::move_outward(const std::string& inner_set_name, const std::string& top_set_name)
{
  if (!runtime_stack.empty())
    runtime_stack.back()->move_outward(inner_set_name, top_set_name);
}

void Resource_Manager::union_current_frame(const std::string& top_set_name, const std::string& inner_set_name)
{
  if (!runtime_stack.empty())
    runtime_stack.back()->union_current_frame(top_set_name, inner_set_name);
}

bool Resource_Manager::union_inward(const std::string& top_set_name, const std::string& inner_set_name)
{
  if (runtime_stack.empty())
    return false;

  return runtime_stack.back()->union_inward(top_set_name, inner_set_name);
}


void Resource_Manager::copy_inward(const std::string& top_set_name, const std::string& inner_set_name)
{
  if (!runtime_stack.empty())
    runtime_stack.back()->copy_inward(top_set_name, inner_set_name);
}


void Resource_Manager::substract_from_inward(const std::string& top_set_name, const std::string& inner_set_name)
{
  if (!runtime_stack.empty())
    runtime_stack.back()->substract_from_inward(top_set_name, inner_set_name);
}


void Resource_Manager::move_all_inward()
{
  if (!runtime_stack.empty())
    runtime_stack.back()->move_all_inward();
}


void Resource_Manager::move_all_inward_except(const std::string& set_name)
{
  if (!runtime_stack.empty())
    runtime_stack.back()->move_all_inward_except(set_name);
}


void Resource_Manager::pop_stack_frame()
{
  if (!runtime_stack.empty())
  {
    delete runtime_stack.back();
    runtime_stack.pop_back();
  }
}

bool Resource_Manager::set_exists_in_parents(const std::string& set_name)
{
  if (runtime_stack.empty())
    return false;

  return runtime_stack.back()->set_exists_in_parents(set_name);
}


uint count_set(const Set& set_)
{
  uint size(0);
  for (auto it(set_.nodes.begin()); it != set_.nodes.end(); ++it)
    size += it->second.size();
  for (auto it(set_.ways.begin()); it != set_.ways.end(); ++it)
    size += it->second.size();
  for (auto it(set_.relations.begin()); it != set_.relations.end(); ++it)
    size += it->second.size();

  for (auto it(set_.attic_nodes.begin()); it != set_.attic_nodes.end(); ++it)
    size += it->second.size();
  for (auto it(set_.attic_ways.begin()); it != set_.attic_ways.end(); ++it)
    size += it->second.size();
  for (auto it(set_.attic_relations.begin()); it != set_.attic_relations.end(); ++it)
    size += it->second.size();

  for (auto it(set_.areas.begin()); it != set_.areas.end(); ++it)
    size += it->second.size();

  return size;
}


void Resource_Manager::count_loop()
{
  if (runtime_stack.empty())
    return;

  runtime_stack.back()->count_loop();
}


void Resource_Manager::log_and_display_error(const std::string& message)
{
  if (error_output)
    error_output->runtime_error(message);

  Logger logger(transaction->get_db_dir());
  logger.annotated_log(message);
}


bool Resource_Manager::health_check(const Statement& stmt, uint32 extra_time, uint64 extra_space)
{
  bool extra_space_uses_half_empty = false;

  uint32 elapsed_time = 0;
  if (max_allowed_time > 0)
    elapsed_time = time(NULL) - start_time + extra_time;

  if (elapsed_time >= last_ping_time + 5)
  {
    last_ping_time = elapsed_time;

    if (elapsed_time >= last_report_time + 15)
    {
      if (error_output)
      {
        error_output->display_statement_progress
            (elapsed_time, stmt.get_name(), stmt.get_progress(), stmt.get_line_number(),
	     runtime_stack.back()->stack_progress());
      }
      last_report_time = elapsed_time;
    }
  }

  uint64 size = 0;
  if (max_allowed_space > 0)
  {
    if (!runtime_stack.empty())
      size += runtime_stack.back()->total_size();
    extra_space_uses_half_empty = (extra_space*2 >= max_allowed_space - size);
    size += extra_space;
  }

  if (elapsed_time > max_allowed_time || size > max_allowed_space)
  {
    if (error_output)
    {
      error_output->display_statement_progress
          (elapsed_time, stmt.get_name(), stmt.get_progress(), stmt.get_line_number(),
           runtime_stack.back()->stack_progress());
    }

    Resource_Error error;
    error.timed_out = (elapsed_time > max_allowed_time);
    error.stmt_name = stmt.get_name();
    error.line_number = stmt.get_line_number();
    error.size = size;
    error.runtime = elapsed_time;

    Logger logger(transaction->get_db_dir());
    std::ostringstream out;
    if (elapsed_time > max_allowed_time)
      out<<"Timeout:";
    else
      out<<"Oversized:";
    out<<" runtime "<<error.runtime<<" seconds, size "<<error.size<<" bytes, "
        "in line "<<error.line_number<<", statement "<<error.stmt_name;
    logger.annotated_log(out.str());

    throw error;
  }

  return extra_space_uses_half_empty;
}


timestamp_t Resource_Manager::get_desired_timestamp() const
{
  return runtime_stack.empty() ? NOW : runtime_stack.back()->get_desired_timestamp();
}


Diff_Action::_ Resource_Manager::get_desired_action() const
{
  return runtime_stack.empty() ? Diff_Action::positive : runtime_stack.back()->get_desired_action();
}


timestamp_t Resource_Manager::get_diff_from_timestamp() const
{
  return runtime_stack.empty() ? NOW : runtime_stack.back()->get_diff_from_timestamp();
}


timestamp_t Resource_Manager::get_diff_to_timestamp() const
{
  return runtime_stack.empty() ? NOW : runtime_stack.back()->get_diff_to_timestamp();
}


void Resource_Manager::start_diff(timestamp_t comparison_timestamp, timestamp_t desired_timestamp)
{
  if (!runtime_stack.empty())
  {
    runtime_stack.back()->set_desired_action(Diff_Action::collect_lhs);
    runtime_stack.back()->set_diff_from_timestamp(comparison_timestamp);
    runtime_stack.back()->set_diff_to_timestamp(desired_timestamp);
    runtime_stack.back()->set_desired_timestamp(comparison_timestamp);
  }
}


void Resource_Manager::switch_diff_rhs(bool add_deletion_information)
{
  if (!runtime_stack.empty())
  {
    runtime_stack.back()->clear_sets();
    runtime_stack.back()->set_desired_action(
      add_deletion_information ? Diff_Action::collect_rhs_with_del : Diff_Action::collect_rhs_no_del);
    runtime_stack.back()->set_desired_timestamp(runtime_stack.back()->get_diff_to_timestamp());
  }
}


void Resource_Manager::switch_diff_show_from(const std::string& diff_set_name)
{
  if (!runtime_stack.empty())
  {
    runtime_stack.back()->set_desired_action(Diff_Action::show_old);
    runtime_stack.back()->set_desired_timestamp(runtime_stack.back()->get_diff_from_timestamp());

    Diff_Set* diff_set = runtime_stack.back()->get_diff_set(diff_set_name);
    if (diff_set)
    {
      Set set = diff_set->make_from_set();
      runtime_stack.back()->swap_set(diff_set_name, set);
    }
  }
}


void Resource_Manager::switch_diff_show_to(const std::string& diff_set_name)
{
  if (!runtime_stack.empty())
  {
    runtime_stack.back()->set_desired_action(Diff_Action::show_new);
    runtime_stack.back()->set_desired_timestamp(runtime_stack.back()->get_diff_to_timestamp());

    Diff_Set* diff_set = runtime_stack.back()->get_diff_set(diff_set_name);
    if (diff_set)
    {
      Set set = diff_set->make_to_set();
      runtime_stack.back()->swap_set(diff_set_name, set);
    }
  }
}


void Resource_Manager::set_desired_timestamp(timestamp_t timestamp)
{
  if (!runtime_stack.empty())
    runtime_stack.back()->set_desired_timestamp(timestamp);
}


void Resource_Manager::start_cpu_timer(uint index)
{
  if (cpu_start_time.size() <= index)
    cpu_start_time.resize(index+1);
  cpu_start_time[index] = std::chrono::high_resolution_clock::now();
}


void Resource_Manager::stop_cpu_timer(uint index)
{
  if (cpu_runtime.size() <= index)
    cpu_runtime.resize(index+1);
  if (index < cpu_start_time.size()) {
    auto t2 = std::chrono::high_resolution_clock::now();
    auto int_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - cpu_start_time[index]);
    cpu_runtime[index] += int_ms;
  }
}
