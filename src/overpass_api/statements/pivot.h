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

#ifndef DE__OSM3S___OVERPASS_API__STATEMENTS__AREA_QUERY_H
#define DE__OSM3S___OVERPASS_API__STATEMENTS__AREA_QUERY_H

#include <map>
#include <string>
#include <vector>
#include "../data/utils.h"
#include "statement.h"


class Pivot_Statement final : public Output_Statement
{
  public:
    Pivot_Statement(int line_number_, const std::map< std::string, std::string >& attributes,
                    Parsed_Query& global_settings);
    std::string get_name() const override { return "pivot"; }
    void execute(Resource_Manager& rman) override;
    ~Pivot_Statement() override;

    struct Statement_Maker : public Generic_Statement_Maker< Pivot_Statement >
    {
      Statement_Maker() : Generic_Statement_Maker< Pivot_Statement >("pivot") {}
    };
    static Statement_Maker statement_maker;

    struct Criterion_Maker : public Statement::Criterion_Maker
    {
      bool can_standalone(const std::string& type) override { return type == "node"; }
      Statement* create_criterion(const Token_Node_Ptr& tree_it,
          const std::string& type, const std::string& into,
          Statement::Factory& stmt_factory, Parsed_Query& global_settings, Error_Output* error_output) override;
      Criterion_Maker() { Statement::maker_by_ql_criterion()["pivot"] = this; }
    };
    static Criterion_Maker criterion_maker;

    Query_Constraint* get_query_constraint() override;
    std::string get_input() const { return input; }

#ifdef HAVE_OVERPASS_XML
    std::string dump_xml(const std::string& indent) const override
    {
      return indent + "<pivot"
          + (input != "_" ? std::string(" from=\"") + input + "\"" : "")
          + dump_xml_result_name() + "/>\n";
    }
#endif

    std::string dump_compact_ql(const std::string&) const override
    {
      return "node" + dump_ql_in_query("") + dump_ql_result_name() + ";";
    }
    std::string dump_ql_in_query(const std::string&) const override
    {
      return std::string("(pivot")
          + (input != "_" ? std::string(".") + input : "")
          + ")";
    }
    std::string dump_pretty_ql(const std::string& indent) const override { return indent + dump_compact_ql(indent); }

  private:
    std::string input;
    std::vector< Query_Constraint* > constraints;
};

#endif
