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

#ifndef DE__OSM3S___OVERPASS_API__STATEMENTS__AROUND_H
#define DE__OSM3S___OVERPASS_API__STATEMENTS__AROUND_H

#include <iostream>
#include <map>
#include <set>
#include <string>
#include <tuple>
#include <vector>
#include "../data/collect_members.h"
#include "../data/utils.h"
#include "../data/way_geometry_store.h"
#include "statement.h"


struct Prepared_BBox
{
  double min_lat;
  double min_lon;
  double max_lat;
  double max_lon;

  Prepared_BBox();
  void merge(Prepared_BBox&);
  bool intersects(const Prepared_BBox &) const;
  bool intersects(const std::vector < Prepared_BBox > &) const;
};

struct Prepared_Segment
{
  double first_lat;
  double first_lon;
  double second_lat;
  double second_lon;
  std::tuple< double, double, double > first_cartesian;
  std::tuple< double, double, double > second_cartesian;
  std::tuple< double, double, double > norm;

  Prepared_Segment(double first_lat, double first_lon, double second_lat, double second_lon);
};


struct Prepared_Point
{
  double lat;
  double lon;
  std::tuple< double, double, double > cartesian;

  Prepared_Point(double lat, double lon);
};


class Around_Statement : public Output_Statement
{
  public:
    Around_Statement(int line_number_, const std::map< std::string, std::string >& attributes,
                     Parsed_Query& global_settings);
    virtual std::string get_name() const { return "around"; }
    virtual void execute(Resource_Manager& rman);
    virtual ~Around_Statement();

    struct Statement_Maker : public Generic_Statement_Maker< Around_Statement >
    {
      Statement_Maker() : Generic_Statement_Maker< Around_Statement >("around") {}
    };
    static Statement_Maker statement_maker;

    struct Criterion_Maker : public Statement::Criterion_Maker
    {
      virtual bool can_standalone(const std::string& type) { return type == "node"; }
      virtual Statement* create_criterion(const Token_Node_Ptr& tree_it,
          const std::string& type, const std::string& into,
          Statement::Factory& stmt_factory, Parsed_Query& global_settings, Error_Output* error_output);
      Criterion_Maker() { Statement::maker_by_ql_criterion()["around"] = this; }
    };
    static Criterion_Maker criterion_maker;

    virtual Query_Constraint* get_query_constraint();

    std::string get_source_name() const { return input; }

    std::set< std::pair< Uint32_Index, Uint32_Index > > calc_ranges
        (const Set& input_nodes, Resource_Manager& rman) const;

    void calc_lat_lons(const Set& input_nodes, Statement& query, Resource_Manager& rman);

    bool is_inside(double lat, double lon) const;
    bool is_inside(double first_lat, double first_lon, double second_lat, double second_lon) const;
    bool is_inside(const std::vector< Quad_Coord >& way_geometry) const;

    double get_radius() const { return radius; }

    template< typename Node_Skeleton >
    void add_nodes(const std::map< Uint32_Index, std::vector< Node_Skeleton > >& nodes);

    template< typename Way_Skeleton >
    void add_ways(const std::map< Uint31_Index, std::vector< Way_Skeleton > >& ways,
		  const Way_Geometry_Store& way_geometries);

    bool matches_bboxes(double lat, double lon) const;
    bool matches_bboxes(const Prepared_BBox&) const;

    virtual std::string dump_xml(const std::string& indent) const
    {
      std::string result = indent + "<around"
          + (input != "_" ? std::string(" from=\"") + input + "\"" : "")
          + std::string(" radius=\"") + to_string(radius) + "\"";
      if (points.size() == 1)
        result += std::string(" lat=\"") + to_string(points.front().lat) + "\""
            + std::string(" lon=\"") + to_string(points.front().lon) + "\"";
      else if (!points.empty())
      {
        result += " polyline=\"";
        for (std::vector< Point_Double >::const_iterator it = points.begin(); it != points.end(); ++it)
          result += to_string(it->lat) + "," + to_string(it->lon) + ",";
        result[result.size()-1] = '\"';
      }
      return result + dump_xml_result_name() + "/>\n";
    }

    virtual std::string dump_compact_ql(const std::string&) const
    {
      return "node" + dump_ql_in_query("") + dump_ql_result_name() + ";";
    }
    virtual std::string dump_ql_in_query(const std::string&) const
    {
      std::string result = std::string("(around")
          + (input != "_" ? std::string(".") + input : "")
          + std::string(":") + to_string(radius);
      for (std::vector< Point_Double >::const_iterator it = points.begin(); it != points.end(); ++it)
        result += "," + to_string(it->lat) + "," + to_string(it->lon);
      return result + ")";
    }
    virtual std::string dump_pretty_ql(const std::string& indent) const { return indent + dump_compact_ql(indent); }

  private:
    std::string input;
    double radius;
    std::vector< Point_Double > points;

    std::map< Uint32_Index, std::vector< Point_Double > > radius_lat_lons;
    std::vector< std::pair< Prepared_BBox, Prepared_Point> > simple_lat_lons;
    std::vector< std::pair< Prepared_BBox, Prepared_Segment> > simple_segments;

    std::vector< Query_Constraint* > constraints;
    std::vector< Prepared_BBox > node_bboxes;
    std::vector< Prepared_BBox > way_bboxes;
};

#endif
