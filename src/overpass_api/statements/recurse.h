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

#ifndef DE__OSM3S___OVERPASS_API__STATEMENTS__RECURSE_H
#define DE__OSM3S___OVERPASS_API__STATEMENTS__RECURSE_H

#include "../../expat/escape_json.h"
#include "../../expat/escape_xml.h"
#include "query.h"
#include "statement.h"

#include <map>
#include <string>
#include <vector>


class Recurse_Statement final : public Output_Statement
{
  public:
    Recurse_Statement(int line_number_, const std::map< std::string, std::string >& input_attributes,
                      Parsed_Query& global_settings);
    std::string get_name() const override { return "recurse"; }
    void execute(Resource_Manager& rman) override;
    ~Recurse_Statement() override;

    struct Statement_Maker : public Generic_Statement_Maker< Recurse_Statement >
    {
      Statement_Maker() : Generic_Statement_Maker< Recurse_Statement >("recurse") {}
    };
    static Statement_Maker statement_maker;

    struct Criterion_Maker_1 : public Statement::Criterion_Maker
    {
      bool can_standalone(const std::string& type) override { return type != "nwr"; }
      Statement* create_criterion(const Token_Node_Ptr& tree_it,
          const std::string& type, const std::string& into,
          Statement::Factory& stmt_factory, Parsed_Query& global_settings, Error_Output* error_output) override;
      Criterion_Maker_1()
      {
        Statement::maker_by_ql_criterion()["w"] = this;
        Statement::maker_by_ql_criterion()["r"] = this;
        Statement::maker_by_ql_criterion()["bn"] = this;
        Statement::maker_by_ql_criterion()["bw"] = this;
        Statement::maker_by_ql_criterion()["br"] = this;
      }
    };
    static Criterion_Maker_1 criterion_maker_1;

    struct Criterion_Maker_2 : public Statement::Criterion_Maker
    {
      bool can_standalone(const std::string& type) override { return false; }
      Statement* create_criterion(const Token_Node_Ptr& tree_it,
          const std::string& type, const std::string& into,
          Statement::Factory& stmt_factory, Parsed_Query& global_settings, Error_Output* error_output) override;
      Criterion_Maker_2()
      {
        Statement::maker_by_ql_criterion()["<"] = this;
        Statement::maker_by_ql_criterion()["<<"] = this;
        Statement::maker_by_ql_criterion()[">"] = this;
        Statement::maker_by_ql_criterion()[">>"] = this;
      }
    };
    static Criterion_Maker_2 criterion_maker_2;

    Query_Constraint* get_query_constraint() override;
    unsigned int get_type() const { return type; }
    std::string get_input() const { return input; }

    const std::string* get_role() const { return (restrict_to_role ? &role : 0); }

    static std::string to_target_type(int type);
    static std::string to_xml_representation(int type);
    static std::string to_ql_representation(int type);

    std::string dump_xml(const std::string& indent) const override
    {
      return indent + "<recurse"
          + (input != "_" ? std::string(" from=\"") + input + "\"" : "")
          + " type=\"" + to_xml_representation(type) + "\""
          + (!role.empty() ? std::string(" role=\"") + escape_xml(role) + "\"" : "")
          + (restrict_to_role ? " role-restricted=\"yes\"" : "")
          + dump_xml_pos_restrictions()
          + dump_xml_result_name() + "/>\n";
    }

    std::string dump_ql_in_query(const std::string&) const override
    {
      return std::string("(") + to_ql_representation(type)
          + (input != "_" ? std::string(".") + input : "")
          + (restrict_to_role || !pos.empty() ? ":" : "")
          + (restrict_to_role ? std::string("\"") + escape_cstr(role) + "\"" : "")
          + dump_ql_pos_restrictions()
          + ")";
    }

    std::string dump_compact_ql(const std::string&) const override
    {
      std::string target_type = to_target_type(type);
      if (!target_type.empty())
        return target_type + "(" + to_ql_representation(type)
            + (input != "_" ? std::string(".") + input : "")
            + (restrict_to_role || !pos.empty() ? ":" : "")
            + (restrict_to_role ? std::string("\"") + escape_cstr(role) + "\"" : "")
            + dump_ql_pos_restrictions()
            + ")" + dump_ql_result_name() + ";";
      else
        return (input != "_" ? std::string(".") + input + " " : "")
            + to_ql_representation(type) + dump_ql_result_name() + ";";
    }
    std::string dump_pretty_ql(const std::string& indent) const override { return indent + dump_compact_ql(indent); }

    const std::vector< int >* get_pos() const { return pos.empty() ? 0 : &pos; }

  private:
    std::string input;
    unsigned int type;
    std::string role;
    bool restrict_to_role;
    std::vector< int > pos;
    std::vector< Query_Constraint* > constraints;

    std::string dump_ql_pos_restrictions() const
    {
      if (pos.empty())
        return "";
      std::string result = to_string(pos[0]);
      for (uint i = 1; i < pos.size(); ++i)
        result += "," + to_string(pos[i]);
      return result;
    }

    std::string dump_xml_pos_restrictions() const
    {
      if (pos.empty())
        return "";
      std::string result = " pos=\"" + to_string(pos[0]);
      for (uint i = 1; i < pos.size(); ++i)
        result += "," + to_string(pos[i]);
      return result + "\"";
    }

    bool accept(Statement_Visitor& visitor)  override;
    bool accept(const Statement_Visitor& visitor) const override;
};


#endif
