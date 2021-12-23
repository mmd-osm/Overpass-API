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

#ifndef DE__OSM3S___OVERPASS_API__STATEMENTS__POLYGON_QUERY_H
#define DE__OSM3S___OVERPASS_API__STATEMENTS__POLYGON_QUERY_H

#include "../data/collect_members.h"
#include "../data/utils.h"
#include "../data/way_geometry_store.h"
#include "statement.h"

#include <map>
#include <string>
#include <vector>


class Polygon_Query_Statement final : public Output_Statement
{
  public:
    Polygon_Query_Statement(int line_number_, const std::map< std::string, std::string >& attributes,
                            Parsed_Query& global_settings);
    std::string get_name() const override { return "polygon-query"; }
    void execute(Resource_Manager& rman) override;
    ~Polygon_Query_Statement() override;

    struct Statement_Maker : public Generic_Statement_Maker< Polygon_Query_Statement >
    {
      Statement_Maker() : Generic_Statement_Maker< Polygon_Query_Statement >("polygon-query") {}
    };
    static Statement_Maker statement_maker;

    struct Criterion_Maker : public Statement::Criterion_Maker
    {
      bool can_standalone(const std::string& type) override { return type == "node"; }
      Statement* create_criterion(const Token_Node_Ptr& tree_it,
          const std::string& type, const std::string& into,
          Statement::Factory& stmt_factory, Parsed_Query& global_settings, Error_Output* error_output) override;
      Criterion_Maker() { Statement::maker_by_ql_criterion()["poly"] = this; }
    };
    static Criterion_Maker criterion_maker;

    Query_Constraint* get_query_constraint() override;

    std::set< std::pair< Uint32_Index, Uint32_Index > > calc_ranges();

    template< typename Node_Skeleton >
    void collect_nodes(std::map< Uint32_Index, std::vector< Node_Skeleton > >& nodes, bool add_border);

    template< typename Way_Skeleton >
    void collect_ways
      (std::map< Uint31_Index, std::vector< Way_Skeleton > >& ways,
       const Way_Geometry_Store& way_geometries,
       bool add_border, const Statement& query, Resource_Manager& rman);

    bool covers_large_area() const { return covers_large_area_; }

#ifdef HAVE_OVERPASS_XML
    std::string dump_xml(const std::string& indent) const override
    {
      std::string result = indent + "<polygon-query bounds=\"";
      auto it = edges.begin();
      if (it != edges.end())
      {
        result += to_string(it->first) + " " + to_string(it->second);
        for (++it; it != edges.end(); ++it)
          result += "  " + to_string(it->first) + " " + to_string(it->second);
      }
      return result + "\"" + dump_xml_result_name() + "/>\n";
    }
#endif

    std::string dump_compact_ql(const std::string&) const override
    {
      return "node" + dump_ql_in_query("") + dump_ql_result_name() + ";";
    }
    std::string dump_ql_in_query(const std::string&) const override
    {
      std::string result = "(poly:\"";
      auto it = edges.begin();
      if (it != edges.end())
      {
        result += to_string(it->first) + " " + to_string(it->second);
        for (++it; it != edges.end(); ++it)
          result += "  " + to_string(it->first) + " " + to_string(it->second);
      }
      return result + "\")";
    }
    std::string dump_pretty_ql(const std::string& indent) const override { return indent + dump_compact_ql(indent); }

  private:
    std::vector< std::pair< double, double > > edges;
    std::vector< Aligned_Segment > segments;
    bool covers_large_area_;
    std::vector< Query_Constraint* > constraints;
};

#endif
