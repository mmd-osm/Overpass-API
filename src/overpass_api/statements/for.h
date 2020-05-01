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

#ifndef DE__OSM3S___OVERPASS_API__STATEMENTS__FOR_H
#define DE__OSM3S___OVERPASS_API__STATEMENTS__FOR_H

#include <map>
#include <string>
#include <vector>

#include "evaluator.h"
#include "statement.h"


/* === The block statement ''for'' ===

''since v0.7.55''

The block statement ''for'' divides its input into subsets
and executes all the statements in the loop body once for every subset.

The input set is broken down as follows:
For each element the given evaluator is evaluated
and elements with the same value are grouped together.
At the beginning of each loop execution,
the output set is filled with the relevant subset.

The base syntax is

  for (<Evaluator>)
  {
    <List of Substatements>
  }

The input and output set can be specified between ''for'' and the opening parenthesis,
i.e. you set the input set

  for.<Name of Input Set> (<Evaluator>)
  {
    <List of Substatements>
  }

or the output set

  for->.<Name of Output Set> (<Evaluator>)
  {
    <List of Substatements>
  }

or both

  for.<Name of Input Set>->.<Name of Output Set> (<Evaluator>)
  {
    <List of Substatements>
  }

Within the loop, the value of the evaluator is available via the property ''val'' of the output set.
I.e., with

  <Output Set>.val

you can access the value of the expression for this loop.

With the special evaluator ''keys()'', one can loop over all the keys that exist in the subset.
The respective subset for each key are the elements that have this key set.
Unlike for a usual evaluator, the sets are not mutually distinct in that case.
*/

class For_Statement : public Statement
{
  public:
    For_Statement(int line_number_, const std::map< std::string, std::string >& attributes,
                      Parsed_Query& global_settings);
    virtual void add_statement(Statement* statement, std::string text);
    virtual std::string get_name() const { return "for"; }
    virtual std::string get_result_name() const { return output; }
    virtual void execute(Resource_Manager& rman);
    virtual ~For_Statement() {}

    static Generic_Statement_Maker< For_Statement > statement_maker;

    virtual std::string dump_xml(const std::string& indent) const
    {
      std::string result = indent + "<for"
          + (input != "_" ? " from=\"" + input + "\"" : "")
          + (output != "_" ? " into=\"" + output + "\"" : "") + ">\n";

      result += evaluator ? evaluator->dump_xml(indent + "  ") : "";
      for (std::vector< Statement* >::const_iterator it = substatements.begin(); it != substatements.end(); ++it)
        result += *it ? (*it)->dump_xml(indent + "  ") : "";

      return result + indent + "</for>\n";
    }

    virtual std::string dump_compact_ql(const std::string& indent) const
    {
      std::string result = indent + "for"
          + (input != "_" ? "." + input : "") + (output != "_" ? "->." + output : "");

      result += "(" + (evaluator ? evaluator->dump_compact_ql(indent) :  "") + "){";
      for (std::vector< Statement* >::const_iterator it = substatements.begin(); it != substatements.end(); ++it)
        result += (*it)->dump_compact_ql(indent);
      result += "}";

      return result;
    }

    virtual std::string dump_pretty_ql(const std::string& indent) const
    {
      std::string result = indent + "for"
          + (input != "_" ? "." + input : "") + (output != "_" ? "->." + output : "") + "(";

      result += (evaluator ? evaluator->dump_pretty_ql(indent) :  "") + "){";
      for (std::vector< Statement* >::const_iterator it = substatements.begin(); it != substatements.end(); ++it)
        result += "\n" + (*it)->dump_pretty_ql(indent + "  ");
      result += "\n}";

      return result;
    }

  private:
    std::string input, output;
    Evaluator* evaluator;
    std::vector< Statement* > substatements;
};

#endif
