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

#ifndef DE__OSM3S___OVERPASS_API__STATEMENTS__ITEM_H
#define DE__OSM3S___OVERPASS_API__STATEMENTS__ITEM_H

#include <map>
#include <string>
#include <vector>

#include "query.h"
#include "statement.h"


class Item_Statement final : public Output_Statement
{
  public:
    Item_Statement(int line_number_, const std::map< std::string, std::string >& attributes,
                   Parsed_Query& global_settings);
    std::string get_name() const override { return "item"; }
    void execute(Resource_Manager& rman) override;
    ~Item_Statement() override;

    static Generic_Statement_Maker< Item_Statement > statement_maker;

    Query_Constraint* get_query_constraint() override;

    std::string dump_xml(const std::string& indent) const override
    {
      return indent + "<item from=\"" + input + "\"" + dump_xml_result_name() + "/>\n";
    }

    std::string dump_compact_ql(const std::string&) const override { return "." + input + dump_ql_result_name() + ";"; }
    std::string dump_ql_in_query(const std::string&) const override { return "." + input; }
    std::string dump_pretty_ql(const std::string& indent) const override { return indent + dump_compact_ql(indent); }

    std::string get_input_name() const { return input; }

  private:
    std::string input;
    std::vector< Query_Constraint* > constraints;

    bool accept(Statement_Visitor& visitor)  override;
    bool accept(const Statement_Visitor& visitor) const override;
};

#endif
