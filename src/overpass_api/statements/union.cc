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

#include <algorithm>
#include <list>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "../data/abstract_processing.h"
#include "../data/utils.h"
#include "union.h"


Generic_Statement_Maker< Union_Statement > Union_Statement::statement_maker("union");


Union_Statement::Union_Statement
    (int line_number_, const std::map< std::string, std::string >& input_attributes, Parsed_Query& global_settings)
    : Output_Statement(line_number_)
{
  std::map< std::string, std::string > attributes;

  attributes["into"] = "_";

  eval_attributes_array(get_name(), attributes, input_attributes);

  set_output(attributes["into"]);
}


void Union_Statement::add_statement(Statement* statement, std::string text)
{
  assure_no_text(text, this->get_name());


  if (statement)
  {
    if (statement->get_name() == "newer")
      add_static_error("\"newer\" can appear only inside \"query\" statements.");
    else if (statement->get_result_name().empty())
      substatement_error(get_name(), statement);
    else
      substatements.push_back(statement);
  }
}

bool Union_Statement::union_fast_path(Resource_Manager& rman)
{
  /* Shortcut union operation for frequently occurring pattern
   *   (.element1; ...; ._elementn; .result;)->.result:
   *
   * Prerequisites:
   * - All union statements must be item statements
   * - Item statements may not copy their result to a named outputset (e.g. .set -> .another_set; is not supported)
   * - Only first union statement may be optionally ._;  (otherwise inputset ._ gets overwritten inside union)
   * - get_result_name() may not be "._" (otherwise data gets overwritten inside union)
   * - get_result_name() must appear at least once in item statements
   *
   * Enables simplified operation:
   * - don't create new stack frame
   * - ignore item statements where inputsets matches get_result_name()
   * - copy item inputset over via indexed_set_union(target.nodes, source->nodes);
   * - skip all other copying operations
   *
   * In addition, the following pattern has very experimental support:
   *
   *  ( make_area [.pivot]; .result;)->.result;
   *
   */


  // exclude any diff actions for now (diff, adiff, compare statement,...)
  if (!rman.get_desired_action() == Diff_Action::positive)
    return false;

  if (get_result_name() == "_")
    return false;

  bool result_name_found = false;

  for (auto s : substatements) {

    if (!(s->get_name() == "item" ||
         (s->get_name() == "make-area" && s == substatements.front())))
      return false;

    // reject .item -> .output; statements
    if (s->get_name() == "item" &&
        s->dump_compact_ql("").find("->.") != std::string::npos)
      return false;

    // TODO: make-area doesn't have proper dump_ql_in_query implementation, method returns an empty string only!

    const auto input_name = s->dump_ql_in_query("").replace(0, 1, "");   // avoid dynamic casts to Item_Statement!

    if (input_name == get_result_name())
      result_name_found = true;

    if (input_name == "_" && s != substatements.front())
      return false;
  }

  if (!result_name_found)
    return false;

  // TODO: in case the result inputset already exists in one of the parent stack frames, we need to move or copy those over to the
  // current stack frame. Otherwise, existing data in parent stack frames might be replaced by the current stack frame later on.
  // As long as this isn't properly implemented, we fall back to the original coding in this case.

  /* Example query - this query should return way 1000 twice. In the buggy version, way 1000 is returned only once.

  ( ( way(1000);  (._;.result;)->.result; ); );
  .result out ;

  ( ( way(1001);  (._;.result;)->.result; ); );
  .result out ;

   */

  if (rman.set_exists_in_parents(get_result_name())) {
    return false;
  }

  for (auto s : substatements) {
    const auto input_name = s->dump_ql_in_query("").replace(0, 1, "");

    if (input_name == get_result_name())
      continue;

    s->execute(rman);
    rman.union_current_frame(s->get_result_name(), get_result_name());
  }

  rman.health_check(*this);
  return true;
}


void Union_Statement::execute(Resource_Manager& rman)
{
  if (union_fast_path(rman))
    return;

  rman.push_stack_frame();
  rman.move_outward(get_result_name(), get_result_name());

  for (auto it(substatements.begin());
       it != substatements.end(); ++it)
  {
    (*it)->execute(rman);
    rman.union_inward((*it)->get_result_name(), get_result_name());
  }

  rman.move_all_inward_except(get_result_name());
  rman.pop_stack_frame();

  rman.health_check(*this);
}
