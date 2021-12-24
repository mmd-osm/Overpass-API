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

#ifndef DE__OSM3S___OVERPASS_API__STATEMENTS__COMPLETE_H
#define DE__OSM3S___OVERPASS_API__STATEMENTS__COMPLETE_H

#ifdef HAVE_CONFIG_H
#include <config.h>
#undef VERSION
#endif

#include <map>
#include <string>
#include <vector>

#include "evaluator.h"
#include "statement.h"


/* === The block statement <em>retro</em> ===

''since v0.7.55''

The block statement <em>retro</em> executes its substatements
for the date given in the evaluator.

It is currently undefined
whether the content of variables from outside the block is available within the block and vice versa.
Future versions may transfer only derived elements,
keep the environments completely apart,
or morph elements from one point in time to the other.

The base syntax is

  retro (<Evaluator>)
  {
    <List of Substatements>
  }

where <Evaluator> is an evaulator and <List of Substatements> is a list of substatements.

*/

class Retro_Statement : public Statement
{
public:
  Retro_Statement(int line_number_, const std::map< std::string, std::string >& attributes,
                     Parsed_Query& global_settings);
  void add_statement(Statement* statement, std::string text) override;
  std::string get_name() const override { return "retro"; }
  std::string get_result_name() const override { return ""; }
  void execute(Resource_Manager& rman) override;
  ~Retro_Statement() override = default;

  static Generic_Statement_Maker< Retro_Statement > statement_maker;

#ifdef HAVE_OVERPASS_XML
  std::string dump_xml(const std::string& indent) const override
  {
    std::string result = indent + "<retro>\n"
          + (timestamp ? timestamp->dump_xml(indent + "  ") : "");

    for (auto it = substatements.begin(); it != substatements.end(); ++it)
      result += *it ? (*it)->dump_xml(indent + "  ") : "";

    return result + indent + "</retro>\n";
  }
#endif

  std::string dump_compact_ql(const std::string& indent) const override
  {
    std::string result = indent + "retro("
        + (timestamp ? timestamp->dump_compact_ql("") : "")
        + "){";

    for (auto it = substatements.begin(); it != substatements.end(); ++it)
      result += (*it)->dump_compact_ql(indent);
    result += "}";

    return result;
  }

  std::string dump_pretty_ql(const std::string& indent) const override
  {
    std::string result = indent + "retro ("
        + (timestamp ? timestamp->dump_pretty_ql("") : "")
        + ")\n"
        + indent + "{";

    for (auto it = substatements.begin(); it != substatements.end(); ++it)
      result += "\n" + (*it)->dump_pretty_ql(indent + "  ");
    result += "\n" + indent + "}";

    return result;
  }

private:
  Evaluator* timestamp;
  std::vector< Statement* > substatements;
};


#endif
