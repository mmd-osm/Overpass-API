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

#include <functional>
#include <sstream>



#include "../../template_db/block_backend.h"
#include "../../template_db/random_file.h"
#include "../core/settings.h"
#include "../data/abstract_processing.h"
#include "../data/collect_members.h"
#include "../data/filenames.h"
#include "changed.h"


bool Changed_Statement::area_query_exists_ = false;


Changed_Statement::Statement_Maker Changed_Statement::statement_maker;
Changed_Statement::Criterion_Maker Changed_Statement::criterion_maker;


Statement* Changed_Statement::Criterion_Maker::create_criterion(const Token_Node_Ptr& tree_it,
    const std::string& type, const std::string& into,
    Statement::Factory& stmt_factory, Parsed_Query& global_settings, Error_Output* error_output)
{
  std::string since;
  std::string until;
  std::string changeset;

  uint line_nr = tree_it->line_col.first;

  if (tree_it->token == "!" && tree_it->rhs)
  {
    changeset = decode_json(tree_it.rhs()->token, error_output);
    since = "auto";
    until = "auto";
  }
  else if (tree_it->token == ":" && tree_it->rhs)
  {
    since = decode_json(tree_it.rhs()->token, error_output);
    until = since;
  }
  else if (tree_it->token == "," && tree_it->lhs && tree_it.lhs()->token == ":" && tree_it.lhs()->rhs
      && tree_it->rhs)
  {
    since = decode_json(tree_it.lhs().rhs()->token, error_output);
    until = decode_json(tree_it.rhs()->token, error_output);
  }
  else if (tree_it->token == "changed")
  {
    since = "auto";
    until = "auto";
  }
  else if (tree_it->token == ":")
  {
    if (error_output)
      error_output->add_parse_error("Date required after \"changed\" with colon",
          tree_it->line_col.first);
    return 0;
  }
  else
  {
    if (error_output)
      error_output->add_parse_error("Unexpected token \"" + tree_it->token + "\" after \"changed\"",
          tree_it->line_col.first);
    return 0;
  }

  std::map< std::string, std::string > attributes;
  attributes["since"] = since;
  attributes["until"] = until;
  attributes["changeset"] = changeset;
  return new Changed_Statement(line_nr, attributes, global_settings);
}


template< class TIndex, class TObject >
void filter_elems(const std::vector< typename TObject::Id_Type >& ids, std::map< TIndex, std::vector< TObject > >& elems)
{
  for (typename std::map< TIndex, std::vector< TObject > >::iterator it = elems.begin();
      it != elems.end(); ++it)
  {
    std::vector< TObject > local_into;
    for (typename std::vector< TObject >::const_iterator iit = it->second.begin();
        iit != it->second.end(); ++iit)
    {
      if (std::binary_search(ids.begin(), ids.end(), iit->id))
	local_into.push_back(*iit);
    }
    it->second.swap(local_into);
  }
}

template< class TIndex, class TObject >
void filter_elems_fast(const IdSetHybrid<typename TObject::Id_Type::Id_Type>& ids,
                       std::map< TIndex, std::vector< TObject > >& elems)
{
  for (typename std::map< TIndex, std::vector< TObject > >::iterator it = elems.begin();
      it != elems.end(); ++it)
  {
    std::vector< TObject > local_into;
    for (typename std::vector< TObject >::const_iterator iit = it->second.begin();
        iit != it->second.end(); ++iit)
    {
      if (ids.get(iit->id.val()))
        local_into.push_back(*iit);
    }
    it->second.swap(local_into);
  }
}




template< typename Index, typename Skeleton, typename Id_Predicate >
std::vector< typename Skeleton::Id_Type > collect_changed_elements
    (uint64 since, uint64 until,
     const Id_Predicate& relevant, Resource_Manager& rman)
{
  std::set< std::pair< Timestamp, Timestamp > > range;
  range.insert(std::make_pair(Timestamp(since), Timestamp(until)));

  std::vector< typename Skeleton::Id_Type > ids;

  Block_Backend< Timestamp, Change_Entry< typename Skeleton::Id_Type > > changelog_db
      (rman.get_transaction()->data_index(changelog_file_properties< Skeleton >()));
  for (typename Block_Backend< Timestamp, Change_Entry< typename Skeleton::Id_Type > >::Range_Iterator
      it = changelog_db.range_begin(Default_Range_Iterator< Timestamp >(range.begin()),
            Default_Range_Iterator< Timestamp >(range.end()));
      !(it == changelog_db.range_end()); ++it)
  {
    if (relevant(it.handle().id()))
      ids.push_back(it.handle().id());
  }

  std::sort(ids.begin(), ids.end());
  ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
  return ids;
}

template< typename Index, typename Skeleton, typename Id_Predicate >
IdSetHybrid<typename Skeleton::Id_Type::Id_Type> collect_changed_elements_fast
    (uint64 since, uint64 until,
     const Id_Predicate& relevant, Resource_Manager& rman)
{
  std::set< std::pair< Timestamp, Timestamp > > range;
  range.insert(std::make_pair(Timestamp(since), Timestamp(until)));

  IdSetHybrid<typename Skeleton::Id_Type::Id_Type> ids;

  Block_Backend< Timestamp, Change_Entry< typename Skeleton::Id_Type > > changelog_db
      (rman.get_transaction()->data_index(changelog_file_properties< Skeleton >()));
  for (typename Block_Backend< Timestamp, Change_Entry< typename Skeleton::Id_Type > >::Range_Iterator
      it = changelog_db.range_begin(Default_Range_Iterator< Timestamp >(range.begin()),
            Default_Range_Iterator< Timestamp >(range.end()));
      !(it == changelog_db.range_end()); ++it)
  {
    if (relevant(it.handle().id()))
      ids.set(it.handle().id().val());
  }

  ids.sort_unique();

  return ids;
}

//-----------------------------------------------------------------------------

template< typename Id_Type >
struct Trivial_Id_Predicate
{
  bool operator()(Id_Type id) const { return true; }
};


template< typename Index, typename Skeleton >
struct Ids_In_Set_Predicate
{
  Ids_In_Set_Predicate(
      const std::map< Index, std::vector< Skeleton > >& current,
      const std::map< Index, std::vector< Attic< Skeleton > > >& attic);

  bool operator()(typename Skeleton::Id_Type id) const
  { return std::binary_search(set_ids.begin(), set_ids.end(), id); }

private:
  std::vector< typename Skeleton::Id_Type > set_ids;
};


template< typename Index, typename Skeleton >
Ids_In_Set_Predicate< Index, Skeleton >::Ids_In_Set_Predicate(
    const std::map< Index, std::vector< Skeleton > >& current,
    const std::map< Index, std::vector< Attic< Skeleton > > >& attic)
{
  for (typename std::map< Index, std::vector< Skeleton > >::const_iterator it_idx = current.begin();
      it_idx != current.end(); ++it_idx)
  {
    for (typename std::vector< Skeleton >::const_iterator it_elem = it_idx->second.begin();
        it_elem != it_idx->second.end(); ++it_elem)
      set_ids.push_back(it_elem->id);
  }

  for (typename std::map< Index, std::vector< Attic< Skeleton > > >::const_iterator it_idx = attic.begin();
      it_idx != attic.end(); ++it_idx)
  {
    for (typename std::vector< Attic< Skeleton > >::const_iterator it_elem = it_idx->second.begin();
        it_elem != it_idx->second.end(); ++it_elem)
      set_ids.push_back(it_elem->id);
  }

  std::sort(set_ids.begin(), set_ids.end());
  set_ids.erase(std::unique(set_ids.begin(), set_ids.end()), set_ids.end());
}

template< typename Index, typename Skeleton >
struct Ids_Dense_Predicate
{
  Ids_Dense_Predicate(
      const std::map< Index, std::vector< Skeleton > >& current,
      const std::map< Index, std::vector< Attic< Skeleton > > >& attic);

  bool operator()(typename Skeleton::Id_Type id) const
  { return ids.get(id.val()); }

private:
  IdSetHybrid<typename Skeleton::Id_Type::Id_Type> ids;
};


template< typename Index, typename Skeleton >
Ids_Dense_Predicate< Index, Skeleton >::Ids_Dense_Predicate(
    const std::map< Index, std::vector< Skeleton > >& current,
    const std::map< Index, std::vector< Attic< Skeleton > > >& attic)
{
  for (typename std::map< Index, std::vector< Skeleton > >::const_iterator it_idx = current.begin();
      it_idx != current.end(); ++it_idx)
  {
    for (typename std::vector< Skeleton >::const_iterator it_elem = it_idx->second.begin();
        it_elem != it_idx->second.end(); ++it_elem)
      ids.set(it_elem->id.val());
  }

  for (typename std::map< Index, std::vector< Attic< Skeleton > > >::const_iterator it_idx = attic.begin();
      it_idx != attic.end(); ++it_idx)
  {
    for (typename std::vector< Attic< Skeleton > >::const_iterator it_elem = it_idx->second.begin();
        it_elem != it_idx->second.end(); ++it_elem)
      ids.set(it_elem->id.val());
  }

  ids.sort_unique();
}



class Changed_Constraint final : public Query_Constraint
{
  public:
    Changed_Constraint(Changed_Statement& stmt_) : stmt(&stmt_) {}

    Query_Filter_Strategy delivers_data(Resource_Manager& rman) { return prefer_ranges; }

//     bool get_ranges
//         (Resource_Manager& rman, std::set< std::pair< Uint32_Index, Uint32_Index > >& ranges);
//     bool get_ranges
//         (Resource_Manager& rman, std::set< std::pair< Uint31_Index, Uint31_Index > >& ranges);

    bool get_node_ids
        (Resource_Manager& rman, std::vector< Node_Skeleton::Id_Type >& ids);
    bool get_way_ids
        (Resource_Manager& rman, std::vector< Way_Skeleton::Id_Type >& ids);
    bool get_relation_ids
        (Resource_Manager& rman, std::vector< Relation_Skeleton::Id_Type >& ids);

    void filter(Resource_Manager& rman, Set& into);
    virtual ~Changed_Constraint() {}
  private:
    std::ostream& print_constraint( std::ostream &os ) const override {
      return os << (stmt != nullptr ? stmt->dump_ql_in_query("") : "changed");
    }

    Changed_Statement* stmt;
};


template< typename Index, typename Skeleton >
std::vector< typename Skeleton::Id_Type > filter_ids_by_changeset(
    std::vector< typename Skeleton::Id_Type >& ids, uint32 changeset, Statement* stmt, Resource_Manager& rman)
{
  std::vector< typename Skeleton::Id_Type > result;

  std::vector< Index > req = get_indexes_< Index, Skeleton >(ids, rman, true);

  {
    Block_Backend< Index, OSM_Element_Metadata_Skeleton< typename Skeleton::Id_Type >,
        typename std::vector< Index >::const_iterator > current_meta_db
        (rman.get_transaction()->data_index(current_meta_file_properties< Skeleton >()));
    for (typename Block_Backend< Index, OSM_Element_Metadata_Skeleton< typename Skeleton::Id_Type >,
        typename std::vector< Index >::const_iterator >::Discrete_Iterator
        it = current_meta_db.discrete_begin(req.begin(), req.end());
        !(it == current_meta_db.discrete_end()); ++it)
    {
      auto current_changeset = it.handle().get_changeset();

      if (current_changeset == changeset)
      {
        auto current_ref = it.handle().get_ref();
        result.push_back(current_ref);
      }
    }
  }
  {
    Block_Backend< Index, OSM_Element_Metadata_Skeleton< typename Skeleton::Id_Type >,
        typename std::vector< Index >::const_iterator > attic_meta_db
        (rman.get_transaction()->data_index(attic_meta_file_properties< Skeleton >()));
    for (typename Block_Backend< Index, OSM_Element_Metadata_Skeleton< typename Skeleton::Id_Type >,
        typename std::vector< Index >::const_iterator >::Discrete_Iterator
        it = attic_meta_db.discrete_begin(req.begin(), req.end());
        !(it == attic_meta_db.discrete_end()); ++it)
    {
      auto current_changeset = it.handle().get_changeset();

      if (current_changeset == changeset)
      {
        auto current_ref = it.handle().get_ref();
        result.push_back(current_ref);
      }
    }
  }

  std::sort(result.begin(), result.end());
  result.erase(std::unique(result.begin(), result.end()), result.end());
  return result;

}


bool Changed_Constraint::get_node_ids(Resource_Manager& rman, std::vector< Node_Skeleton::Id_Type >& ids)
{
  uint32 changeset = stmt->changeset();

  // TODO: CACHING IS FOR TESTING PURPOSES ONLY!
  ids.assign(stmt->get_node_ids().begin(), stmt->get_node_ids().end());
  if (!ids.empty())
    return true;

  ids = collect_changed_elements< Uint32_Index, Node_Skeleton >(
      stmt->get_since(rman), stmt->get_until(rman), Trivial_Id_Predicate< Node_Skeleton::Id_Type >(), rman);
  if (changeset != 0)
    ids = filter_ids_by_changeset< Uint32_Index, Node_Skeleton >(ids, changeset, stmt, rman);

  stmt->get_node_ids().assign(ids.begin(), ids.end());

  return true;
}


bool Changed_Constraint::get_way_ids(Resource_Manager& rman, std::vector< Way_Skeleton::Id_Type >& ids)
{
  uint32 changeset = stmt->changeset();

  // TODO: CACHING IS FOR TESTING PURPOSES ONLY!
  ids.assign(stmt->get_way_ids().begin(), stmt->get_way_ids().end());
  if (!ids.empty())
    return true;

  ids = collect_changed_elements< Uint31_Index, Way_Skeleton >(
      stmt->get_since(rman), stmt->get_until(rman), Trivial_Id_Predicate< Way_Skeleton::Id_Type >(), rman);
  if (changeset != 0)
    ids = filter_ids_by_changeset< Uint31_Index, Way_Skeleton >(ids, changeset, stmt, rman);

  stmt->get_way_ids().assign(ids.begin(), ids.end());

  return true;
}


bool Changed_Constraint::get_relation_ids(Resource_Manager& rman, std::vector< Relation_Skeleton::Id_Type >& ids)
{
  uint32 changeset = stmt->changeset();

  // TODO: CACHING IS FOR TESTING PURPOSES ONLY!
  ids.assign(stmt->get_rel_ids().begin(), stmt->get_rel_ids().end());
  if (!ids.empty())
    return true;

  ids = collect_changed_elements< Uint31_Index, Relation_Skeleton >(
      stmt->get_since(rman), stmt->get_until(rman), Trivial_Id_Predicate< Relation_Skeleton::Id_Type >(), rman);
  if (changeset != 0)
    ids = filter_ids_by_changeset< Uint31_Index, Relation_Skeleton >(ids, changeset, stmt, rman);

  stmt->get_rel_ids().assign(ids.begin(), ids.end());

  return true;
}



void Changed_Constraint::filter(Resource_Manager& rman, Set& into)
{
/*
  if (!stmt->trivial())
  {
    std::vector< Node_Skeleton::Id_Type > ids =
        collect_changed_elements< Uint32_Index, Node_Skeleton >
        (stmt->get_since(rman), stmt->get_until(rman),
            Ids_In_Set_Predicate< Uint32_Index, Node_Skeleton >(into.nodes, into.attic_nodes), rman);

    filter_elems(ids, into.nodes);
    filter_elems(ids, into.attic_nodes);

  }

  if (!stmt->trivial())
  {
    std::vector< Way_Skeleton::Id_Type > ids =
        collect_changed_elements< Uint31_Index, Way_Skeleton >
        (stmt->get_since(rman), stmt->get_until(rman),
            Ids_In_Set_Predicate< Uint31_Index, Way_Skeleton >(into.ways, into.attic_ways), rman);

    filter_elems(ids, into.ways);
    filter_elems(ids, into.attic_ways);
  }

  if (!stmt->trivial())
  {
    std::vector< Relation_Skeleton::Id_Type > ids =
        collect_changed_elements< Uint31_Index, Relation_Skeleton >
        (stmt->get_since(rman), stmt->get_until(rman),
            Ids_In_Set_Predicate< Uint31_Index, Relation_Skeleton >(into.relations, into.attic_relations), rman);

    filter_elems(ids, into.relations);
    filter_elems(ids, into.attic_relations);
  }
*/

  if (!stmt->trivial())
  {
    auto ids =
        collect_changed_elements_fast< Uint32_Index, Node_Skeleton >
        (stmt->get_since(rman), stmt->get_until(rman),
            Ids_Dense_Predicate< Uint32_Index, Node_Skeleton >(into.nodes, into.attic_nodes), rman);

    filter_elems_fast(ids, into.nodes);
    filter_elems_fast(ids, into.attic_nodes);

  }

  if (!stmt->trivial())
  {
    auto ids =
        collect_changed_elements_fast< Uint31_Index, Way_Skeleton >
        (stmt->get_since(rman), stmt->get_until(rman),
            Ids_Dense_Predicate< Uint31_Index, Way_Skeleton >(into.ways, into.attic_ways), rman);

    filter_elems_fast(ids, into.ways);
    filter_elems_fast(ids, into.attic_ways);
  }

  if (!stmt->trivial())
  {
    auto ids =
        collect_changed_elements_fast< Uint31_Index, Relation_Skeleton >
        (stmt->get_since(rman), stmt->get_until(rman),
            Ids_Dense_Predicate< Uint31_Index, Relation_Skeleton >(into.relations, into.attic_relations), rman);

    filter_elems_fast(ids, into.relations);
    filter_elems_fast(ids, into.attic_relations);
  }

  into.areas.clear();
}

//-----------------------------------------------------------------------------

Changed_Statement::Changed_Statement
    (int line_number_, const std::map< std::string, std::string >& input_attributes, Parsed_Query& global_settings)
    : Output_Statement(line_number_), since(NOW), until(NOW), behave_trivial(false)
{
  std::map< std::string, std::string > attributes;

  attributes["into"] = "_";
  attributes["since"] = "auto";
  attributes["until"] = "auto";
  attributes["changeset"] = "";

  Statement::eval_attributes_array(get_name(), attributes, input_attributes);

  set_output(attributes["into"]);

  if (attributes["since"] == "init")
    behave_trivial = true;

  if (!behave_trivial && ((attributes["since"] == "auto") ^ (attributes["until"] == "auto")))
  {
    std::ostringstream temp;
    temp<<"The attributes \"since\" and \"until\" must be set either both or none.";
    add_static_error(temp.str());
  }

  std::string timestamp = attributes["since"];
  if (timestamp.size() >= 19)
    since = Timestamp(timestamp).timestamp;

  if (!behave_trivial && attributes["since"] != "auto" && (since == 0 || since == NOW))
    add_static_error("The attribute \"since\" must contain a timestamp exactly in the form \"yyyy-mm-ddThh:mm:ssZ\".");

  timestamp = attributes["until"];
  if (timestamp.size() >= 19)
    until = Timestamp(timestamp).timestamp;

  if (!behave_trivial && attributes["until"] != "auto" && (until == 0 || until == NOW))
    add_static_error("The attribute \"until\" must contain a timestamp exactly in the form \"yyyy-mm-ddThh:mm:ssZ\".");

  filter_changeset = 0;
  const auto changeset = attributes["changeset"];
  if (changeset != "") {
    filter_changeset = atol(changeset.c_str());
    if (filter_changeset <= 0) {
      add_static_error("The attribute \"changeset\" must be a positive number");
    }
  }
}


uint64 Changed_Statement::get_since(Resource_Manager& rman) const
{
  if (since == NOW && until == NOW)
    // We have zero arguments on changed.
  {
    if (rman.get_diff_from_timestamp() != rman.get_diff_to_timestamp())
      // We are on diff mode
      return rman.get_diff_from_timestamp() + (rman.get_diff_from_timestamp() == NOW ? 0 : 1);
    else if (rman.get_desired_timestamp() != NOW)
      // We are in attic mode
      return rman.get_desired_timestamp() + 1;
    else
      // We are in present mode.
      // Trigger an empty result on purpose
      return NOW;
  }
  else if (since == until)
    // We have one argument on changed only.
  {
    if (rman.get_diff_from_timestamp() != rman.get_diff_to_timestamp())
      // We are on diff mode
    {
      if (since == rman.get_diff_from_timestamp() || since == rman.get_diff_to_timestamp())
	// If the only parameter is equal to the time range then silently ignore it
	return rman.get_diff_from_timestamp() + 1;
      else
        // Trigger an empty result on purpose
        return NOW;
    }
    else if (rman.get_desired_timestamp() != NOW)
      // We are in attic mode
      return std::min(since, rman.get_desired_timestamp()) + 1;
    else
      // We are in present mode.
      return since + 1;
  }
  else
    // We have two arguments on changed.
    return since + 1;
}


uint64 Changed_Statement::get_until(Resource_Manager& rman) const
{
  if (since == NOW && until == NOW)
    // We have zero arguments on changed.
  {
    if (rman.get_diff_from_timestamp() != rman.get_diff_to_timestamp())
      // We are on diff mode
      return rman.get_diff_to_timestamp() + (rman.get_diff_to_timestamp() == NOW ? 0 : 1);
    else if (rman.get_desired_timestamp() != NOW)
      // We are in attic mode
      return NOW;
    else
      // We are in present mode.
      // Trigger an empty result on purpose
      return NOW;
  }
  else if (since == until)
    // We have one argument on changed only.
  {
    if (rman.get_diff_from_timestamp() != rman.get_diff_to_timestamp())
      // We are on diff mode
    {
      if (since == rman.get_diff_from_timestamp() || since == rman.get_diff_to_timestamp())
	// If the only parameter is equal to the time range then silently ignore it
	return rman.get_diff_to_timestamp() + (rman.get_diff_to_timestamp() == NOW ? 0 : 1);
      else
        // Trigger an empty result on purpose
        return NOW;
    }
    else if (rman.get_desired_timestamp() != NOW)
      // We are in attic mode
      return std::max(until, rman.get_desired_timestamp()) + 1;
    else
      // We are in present mode.
      return NOW;
  }
  else
    // We have two arguments on changed.
    return until + 1;
}


template< typename Index, typename Skeleton >
void get_elements(Changed_Statement& stmt, Resource_Manager& rman,
    std::map< Index, std::vector< Skeleton > >& current_result,
    std::map< Index, std::vector< Attic< Skeleton > > >& attic_result)
{
  std::vector< typename Skeleton::Id_Type > ids =
      collect_changed_elements< Index, Skeleton >(stmt.get_since(rman), stmt.get_until(rman),
          Trivial_Id_Predicate< typename Skeleton::Id_Type >(), rman);
  std::vector< Index > req = get_indexes_< Index, Skeleton >(ids, rman);

  if (rman.get_desired_timestamp() == NOW)
    collect_items_discrete(&stmt, rman, *current_skeleton_file_properties< Skeleton >(), req,
        Id_Predicate< Skeleton >(ids), current_result);
  else
  {
    collect_items_discrete_by_timestamp(&stmt, rman, req,
        Id_Predicate< Skeleton >(ids), current_result, attic_result);
    filter_attic_elements(rman, rman.get_desired_timestamp(), current_result, attic_result);
  }
}


void Changed_Statement::execute(Resource_Manager& rman)
{
  Set into;

  get_elements(*this, rman, into.nodes, into.attic_nodes);
  get_elements(*this, rman, into.ways, into.attic_ways);
  get_elements(*this, rman, into.relations, into.attic_relations);
  into.areas.clear();

  transfer_output(rman, into);
  rman.health_check(*this);
}


Changed_Statement::~Changed_Statement()
{
  for (std::vector< Query_Constraint* >::const_iterator it = constraints.begin();
      it != constraints.end(); ++it)
    delete *it;
}


Query_Constraint* Changed_Statement::get_query_constraint()
{
  constraints.push_back(new Changed_Constraint(*this));
  return constraints.back();
}
