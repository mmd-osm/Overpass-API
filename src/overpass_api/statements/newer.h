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

#ifndef DE__OSM3S___OVERPASS_API__STATEMENTS__NEWER_H
#define DE__OSM3S___OVERPASS_API__STATEMENTS__NEWER_H

#include <map>
#include <set>
#include <string>
#include <vector>
#include "../data/utils.h"
#include "../frontend/basic_formats.h"
#include "statement.h"


class Newer_Statement : public Statement
{
public:
  Newer_Statement(int line_number_, const std::map< std::string, std::string >& input_attributes,
                    Parsed_Query& global_settings);
  std::string get_name() const override { return "newer"; }
  std::string get_result_name() const override { return ""; }
  void execute(Resource_Manager& rman) override;
  ~Newer_Statement() override;

  struct Statement_Maker : public Generic_Statement_Maker< Newer_Statement >
  {
    Statement_Maker() : Generic_Statement_Maker< Newer_Statement >("newer") {}
  };
  static Statement_Maker statement_maker;

  struct Criterion_Maker : public Statement::Criterion_Maker
  {
    bool can_standalone(const std::string& type) override { return false; }
    Statement* create_criterion(const Token_Node_Ptr& tree_it,
        const std::string& type, const std::string& into,
        Statement::Factory& stmt_factory, Parsed_Query& global_settings, Error_Output* error_output) override;
    Criterion_Maker() { Statement::maker_by_ql_criterion()["newer"] = this; }
  };
  static Criterion_Maker criterion_maker;

  Query_Constraint* get_query_constraint() override;

  uint64 get_timestamp() const { return than_timestamp; }

  std::string dump_xml(const std::string& indent) const override
  {
    return indent + "<newer"
        + (than_timestamp != NOW ? std::string(" than=\"") + iso_string(than_timestamp) + "\"" : "")
        + "/>\n";
  }

  std::string dump_compact_ql(const std::string&) const override
  {
    return "all" + dump_ql_in_query("") + ";";
  }
  std::string dump_ql_in_query(const std::string&) const override
  {
    return std::string("(newer:")
        + (than_timestamp != NOW ? std::string("\"") + iso_string(than_timestamp) + "\"" : "")
        + ")";
  }
  std::string dump_pretty_ql(const std::string& indent) const override { return indent + dump_compact_ql(indent); }

private:
  uint64 than_timestamp;
  std::vector< Query_Constraint* > constraints;

  bool accept(Statement_Visitor& visitor)  override;
  bool accept(const Statement_Visitor& visitor) const override;
};

#endif
