
/** Copyright 2021 mmd
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

#include "statement.h"
#include "statement_visitor.h"
#include "area_query.h"
#include "around.h"
#include "bbox_query.h"
#include "changed.h"
#include "complete.h"
#include "filter.h"
#include "foreach.h"
#include "id_query.h"
#include "newer.h"
#include "osm_script.h"
#include "polygon_query.h"
#include "query.h"
#include "retro.h"
#include "union.h"
#include "user.h"

// std::string Bicycle::compareTo(Vehicle* v) {
//    if (Bicycle* b = dynamic_cast<Bicycle*>(v)) {
//       return compareTo(b);
//    } else {
//       return "We're different vehicles.";
//    }
// }

bool DataRenderer::inside_optimize_union() {

  return !optimize_union.empty() && optimize_union.back();
}

bool DataRenderer::visit(Osm_Script_Statement& data) {

   std::cerr << "Osm_Script\n";

   auto substatements = data.get_substatements();

   for (const auto st : *substatements) {
     auto res = st->accept(*this);
   }

   return true;
};

bool DataRenderer::visit(Foreach_Statement& data) {

  std::cerr << "Foreach_Statement\n";
  std::cerr << data.dump_compact_ql("") << "\n";

  auto substatements = data.get_substatements();

  std::cerr << "Substatements:\n";

  for (const auto  st : *substatements) {
    std::cerr << " -> " << st->get_name() << "\n";
    auto res = st->accept(*this);
  }

  return true;
}

bool DataRenderer::visit(Complete_Statement& data) {

  std::cerr << "Complete_Statement\n";
  std::cerr << data.dump_compact_ql("") << "\n";

  auto substatements = data.get_substatements();

  std::cerr << "Substatements:\n";

  for (const auto  st : *substatements) {
    std::cerr << " -> " << st->get_name() << "\n";
    auto res = st->accept(*this);
  }

  return true;
}

bool DataRenderer::visit(Retro_Statement& data) {

  std::cerr << "Retro_Statement\n";
  std::cerr << data.dump_compact_ql("") << "\n";

  auto substatements = data.get_substatements();

  std::cerr << "Substatements:\n";

  for (const auto  st : *substatements) {
    std::cerr << " -> " << st->get_name() << "\n";
    auto res = st->accept(*this);
  }

  return true;
}



bool DataRenderer::visit(Union_Statement& data) {

  std::cerr << "Union\n";

   auto substatements = data.get_substatements();

   // Union statement must have at least one statement, and we want to optimize unions having Query statements only.
   bool relevant_union = substatements->size() > 1 && Statement_Has_Only_Type<Query_Statement>(*substatements);

   std::cerr << "   relevant union: " << relevant_union << "\n";

   optimize_union.push_back(relevant_union);

   for (const auto  st : *substatements) {
     auto res = st->accept(*this);
     if (!res) {
       optimize_union.back() = false;
     }
   }

   std::cerr << "optimize union: " << optimize_union.back() << "\n";

   /*
    *
    * Optimize:
    *
    * - For source = 0 .. n-1 in  union_statement.substatements
    *   - For dest = source + 1  .. n-1 in union_statement.substatements
    *     - Check source and dest Query Statement have the same get_type()
    *     - Check Source and dest have the same number of substatements
    *     - Compare all source query_statement substatements with all dest query statement substatements O(n^2)
    *       --> Are they equal?
    *       --> Can they be merged? (id:1) + (id:2) --> (id:1,2)
    *
    *       vector<pair<source_idx, target_idx>>: equal
    *       vector<pair<source_idx, target_idx>>: mergeable
    *
    *       Two query statements can only be merged, if all except one substatement are equal, and the remaining substatement can be merged
    *
    *       Statement subclasses need equal operator, and is_mergeable method
    *       Also, method to do the actual merge
    *
    *       Merging multiple k=v1..k=vn -->  k~v, where v = ^v1$|^v2$|...|^vn$   (v_i needs to be escaped for regex)
    *       Also, Query_Statement key_values needs to be adjusted: k=v1 removed in merge target, k~v added as key_regexes
    *
    *       After merging, start over, check if further eliminations are possible
    *
    */

   optimize_union.pop_back();

   return true;
};

bool DataRenderer::visit(Query_Statement& data) {

   std::cerr << "Query " << Query_Statement::to_string(data.get_type()) << "\n";

   // Hit a query statement which is not inside a support Union statement?
   if (!inside_optimize_union())
     return false;

   std::cerr << data.dump_compact_ql("") << "\n";

   std::cerr << "  output: " << data.get_result_name() << "\n";

   // Query may not write result to a named set, b/c this would get in the way of merging
   // similar query statements later on
   if (data.get_result_name() != "_")
     return false;

   std::cerr << "Substatements:\n";

   auto substatements = data.get_substatements();

   // TODO: what if substatements is empty (node;   <<-- via global bbox)

   // Check only permitted substatements

   bool only_supported_substatements = Statement_Has_Only_Type< Has_Kv_Statement,
                                                                Bbox_Query_Statement,
                                                                Newer_Statement,
                                                                User_Statement,
                                                                Polygon_Query_Statement,
                                                                Area_Query_Statement >(*substatements);

   std::cerr << "Only of Has_Kv_Statement? " << only_supported_substatements << "\n";

   if (!only_supported_substatements)
     return false;

   for (const auto  st : *substatements) {
     std::cerr << " -> " << st->get_name() << "\n";
     auto res = st->accept(*this);
     if (!res)
       return false;
   }

   std::cerr << "---\nConstraints:\n";

   auto constraints = data.get_constraints();

   std::cerr << Constraint_Has_Only_Type< Bbox_Query_Statement,
                                          Newer_Statement,
                                          User_Statement,
                                          Polygon_Query_Statement >(*constraints) << "\n";

   for (const auto cst : *constraints) {
     auto st = cst->get_statement();
     std::cerr << " -> " << st->get_name() << "\n";
     auto res = st->accept(*this);
     if (!res)
       return false;
   }

   std::cerr << "===\n";

   return true;
};

bool DataRenderer::visit(Id_Query_Statement& data) {

  std::cerr << "Id_Query_Statement\n";

  return true;

}

bool DataRenderer::visit(Has_Kv_Statement& data) {

  std::cerr << "Has_Kv_Statement: " << data.get_key() << " = " << data.get_value() << "\n";
  std::cerr << data.dump_compact_ql("") << "\n";

  if (!inside_optimize_union())
    return false;

  return true;
};

bool DataRenderer::visit(Around_Statement& data) {

  // Around statement is supported, if
  // (1) the source data is provided by a named set (which was populated before entering the Union statement)
  // (2) A list of one of multiple points, which are not depending on any other statement

  bool res = data.get_source_name() != "_" || !data.get_points().empty();

  std::cerr << "Around_Statement: " << res <<"\n";
  std::cerr << data.dump_compact_ql("") << "\n";

  if (!inside_optimize_union())
    return false;

  return res;
}


bool DataRenderer::visit(Area_Query_Statement& data) {

  // Area statement is supported, if
  // (1) Inputset is provided by a named set (populated before entering union statement)
  // (2) An area id, which is independent of any other statement
  //     (note that on-the-fly area creation would interfere with this assumption.
  //      however, it's not in the list of permitted statements for unions)

  bool res = data.get_input() != "_" || data.get_submitted_id() != 0;

  std::cerr << "Area_Query_Statement: " << res << "\n";
  std::cerr << data.dump_compact_ql("") << "\n";

  if (!inside_optimize_union())
    return false;

  return res;
}


void DataRenderer::render(Statement& object) {

  std::cerr << "Statement\n";
  object.accept(*this);
};

/*
template <class C>
AllOfType<C>::operator bool() const {

  for (const auto& obj: objects) {
    obj->accept(*this);
  }

  return counter == objects.size();
}
*/

template <class... Types>
Statement_Has_Only_Type<Types...>::operator bool() const {

  for (const auto& obj: objects) {
    obj->accept(*this);
  }

  return (!objects.empty() && counter == objects.size());
}

template <class... Types>
Constraint_Has_Only_Type<Types...>::operator bool() const {

  for (const auto& cst : constraints) {
    auto st = cst->get_statement();
    auto res = st->accept(*this);
  }

  return (!constraints.empty() && counter == constraints.size());
}

