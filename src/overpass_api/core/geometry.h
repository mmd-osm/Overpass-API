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

#ifndef DE__OSM3S___OVERPASS_API__CORE__GEOMETRY_H
#define DE__OSM3S___OVERPASS_API__CORE__GEOMETRY_H


#include <cmath>
#include <vector>


struct Point_Double
{
public:
  Point_Double(double lat_, double lon_) : lat(lat_), lon(lon_) {}

  double lat;
  double lon;

  bool operator==(const Point_Double& rhs) const { return lat == rhs.lat && lon == rhs.lon; }
  bool operator!=(const Point_Double& rhs) const { return !(*this == rhs); }
  bool operator<(const Point_Double& rhs) const
  { return lat != rhs.lat ? lat < rhs.lat : lon < rhs.lon; }

  bool epsilon_equal(const Point_Double& rhs) const
  { return fabs(lat - rhs.lat) < 1e-7 && fabs(lon - rhs.lon) < 1e-7; }
};


struct Bbox_Double
{
public:
  Bbox_Double(double south_, double west_, double north_, double east_)
      : south(south_), west(west_), north(north_), east(east_) {}

  bool valid() const
  {
    return (south >= -90.0 && south <= north && north <= 90.0
        && east >= -180.0 && east <= 180.0 && west >= -180.0 && west <= 180.0);
  }

  bool redundant() const
  {
    return (south == -90.0 && north == 90.0 && west == -180.0 && east == 180.0);
  }

  double center_lat() const;
  double center_lon() const;

  double south, west, north, east;

  bool contains(const Point_Double& point) const;
  bool intersects(const Point_Double& from, const Point_Double& to) const;

  const static Bbox_Double invalid;
};


// All coordinates are always in latitude and longitude
class Opaque_Geometry
{
public:
  virtual ~Opaque_Geometry() = default;
  virtual Opaque_Geometry* clone() const = 0;

  virtual bool has_center() const = 0;
  virtual double center_lat() const = 0;
  virtual double center_lon() const = 0;

  // We require for a bounding box the following:
  // Usually, west is smaller than east.
  // If the object passes through all lines of longitude then the bounding box is west -180.0, east 180.0.
  // If the object crosses the date line but doesn't pass all lines of longitude then east is smaller than west.
  // For a single point, south and north are equal, and so are west and east.
  virtual bool has_bbox() const = 0;
  virtual double south() const = 0;
  virtual double north() const = 0;
  virtual double west() const = 0;
  virtual double east() const = 0;

  virtual bool has_line_geometry() const = 0;
  virtual const std::vector< Point_Double >* get_line_geometry() const { return 0; }

  virtual bool has_multiline_geometry() const = 0;
  virtual const std::vector< std::vector< Point_Double > >* get_multiline_geometry() const { return 0; }

  virtual bool has_components() const = 0;
  virtual const std::vector< Opaque_Geometry* >* get_components() const { return 0; }
  virtual std::vector< Opaque_Geometry* >* move_components() { return 0; }

  virtual unsigned int way_size() const = 0;
  virtual bool has_faithful_way_geometry() const = 0;
  virtual bool way_pos_is_valid(unsigned int pos) const = 0;
  virtual double way_pos_lat(unsigned int pos) const = 0;
  virtual double way_pos_lon(unsigned int pos) const = 0;

  virtual bool has_faithful_relation_geometry() const = 0;
  virtual bool relation_pos_is_valid(unsigned int member_pos) const = 0;
  virtual double relation_pos_lat(unsigned int member_pos) const = 0;
  virtual double relation_pos_lon(unsigned int member_pos) const = 0;
  virtual unsigned int relation_way_size(unsigned int member_pos) const = 0;
  virtual bool relation_pos_is_valid(unsigned int member_pos, unsigned int nd_pos) const = 0;
  virtual double relation_pos_lat(unsigned int member_pos, unsigned int nd_pos) const = 0;
  virtual double relation_pos_lon(unsigned int member_pos, unsigned int nd_pos) const = 0;

  virtual bool relevant_to_bbox(const Bbox_Double& bbox) const = 0;
};


class Null_Geometry final : public Opaque_Geometry
{
public:
  Null_Geometry() = default;
  Opaque_Geometry* clone() const override { return new Null_Geometry(); }

  bool has_center() const override { return false; }
  double center_lat() const override { return 0; }
  double center_lon() const override { return 0; }

  bool has_bbox() const override { return false; }
  double south() const override { return 0; }
  double north() const override { return 0; }
  double west() const override { return 0; }
  double east() const override { return 0; }

  bool has_line_geometry() const override { return false; }
  bool has_multiline_geometry() const override { return false; }
  bool has_components() const override { return false; }

  unsigned int way_size() const override { return 0; }
  bool has_faithful_way_geometry() const override { return false; }
  bool way_pos_is_valid(unsigned int pos) const override { return false; }
  double way_pos_lat(unsigned int pos) const override { return 0; }
  double way_pos_lon(unsigned int pos) const override { return 0; }

  bool has_faithful_relation_geometry() const override { return false; }
  bool relation_pos_is_valid(unsigned int member_pos) const override { return false; }
  double relation_pos_lat(unsigned int member_pos) const override { return 0; }
  double relation_pos_lon(unsigned int member_pos) const override { return 0; }
  unsigned int relation_way_size(unsigned int member_pos) const override { return 0; }
  bool relation_pos_is_valid(unsigned int member_pos, unsigned int nd_pos) const override { return false; }
  double relation_pos_lat(unsigned int member_pos, unsigned int nd_pos) const override { return 0; }
  double relation_pos_lon(unsigned int member_pos, unsigned int nd_pos) const override { return 0; }

  bool relevant_to_bbox(const Bbox_Double& bbox) const override { return false; }
};


class Point_Geometry final : public Opaque_Geometry
{
public:
  Point_Geometry(double lat_, double lon_) : pt(lat_, lon_) {}
  Opaque_Geometry* clone() const override { return new Point_Geometry(pt.lat, pt.lon); }

  bool has_center() const override { return true; }
  double center_lat() const override { return pt.lat; }
  double center_lon() const override { return pt.lon; }

  bool has_bbox() const override { return true; }
  double south() const override { return pt.lat; }
  double north() const override { return pt.lat; }
  double west() const override { return pt.lon; }
  double east() const override { return pt.lon; }

  bool has_line_geometry() const override { return false; }
  bool has_multiline_geometry() const override { return false; }
  bool has_components() const override { return false; }

  unsigned int way_size() const override { return 0; }
  bool has_faithful_way_geometry() const override { return false; }
  bool way_pos_is_valid(unsigned int pos) const override { return false; }
  double way_pos_lat(unsigned int pos) const override { return 0; }
  double way_pos_lon(unsigned int pos) const override { return 0; }

  bool has_faithful_relation_geometry() const override { return false; }
  bool relation_pos_is_valid(unsigned int member_pos) const override { return false; }
  double relation_pos_lat(unsigned int member_pos) const override { return 0; }
  double relation_pos_lon(unsigned int member_pos) const override { return 0; }
  unsigned int relation_way_size(unsigned int member_pos) const override { return 0; }
  bool relation_pos_is_valid(unsigned int member_pos, unsigned int nd_pos) const override { return false; }
  double relation_pos_lat(unsigned int member_pos, unsigned int nd_pos) const override { return 0; }
  double relation_pos_lon(unsigned int member_pos, unsigned int nd_pos) const override { return 0; }

  bool relevant_to_bbox(const Bbox_Double& bbox) const override;

private:
  Point_Double pt;
};


class Bbox_Geometry final : public Opaque_Geometry
{
public:
  Bbox_Geometry(double south, double west, double north, double east) : bbox(south, west, north, east) {}
  Bbox_Geometry(const Bbox_Double& bbox_) : bbox(bbox_) {}
  Opaque_Geometry* clone() const override { return new Bbox_Geometry(bbox); }

  bool has_center() const override { return true; }
  double center_lat() const override { return bbox.center_lat(); }
  double center_lon() const override { return bbox.center_lon(); }

  bool has_bbox() const override { return true; }
  double south() const override { return bbox.south; }
  double north() const override { return bbox.north; }
  double west() const override { return bbox.west; }
  double east() const override { return bbox.east; }

  bool has_line_geometry() const override { return false; }
  bool has_multiline_geometry() const override { return false; }
  bool has_components() const override { return false; }

  unsigned int way_size() const override { return 0; }
  bool has_faithful_way_geometry() const override { return false; }
  bool way_pos_is_valid(unsigned int pos) const override { return false; }
  double way_pos_lat(unsigned int pos) const override { return 0; }
  double way_pos_lon(unsigned int pos) const override { return 0; }

  bool has_faithful_relation_geometry() const override { return false; }
  bool relation_pos_is_valid(unsigned int member_pos) const override { return false; }
  double relation_pos_lat(unsigned int member_pos) const override { return 0; }
  double relation_pos_lon(unsigned int member_pos) const override { return 0; }
  unsigned int relation_way_size(unsigned int member_pos) const override { return 0; }
  bool relation_pos_is_valid(unsigned int member_pos, unsigned int nd_pos) const override { return false; }
  double relation_pos_lat(unsigned int member_pos, unsigned int nd_pos) const override { return 0; }
  double relation_pos_lon(unsigned int member_pos, unsigned int nd_pos) const override { return 0; }

  bool relevant_to_bbox(const Bbox_Double& bbox) const override { return false; }

private:
  Bbox_Double bbox;
};


class Linestring_Geometry final : public Opaque_Geometry
{
public:
  Linestring_Geometry(const std::vector< Point_Double >& points_) : points(points_), bounds(0) {}
  ~Linestring_Geometry() override { delete bounds; }
  Opaque_Geometry* clone() const override { return new Linestring_Geometry(points); }

  bool has_center() const override { return true; }
  double center_lat() const override;
  double center_lon() const override;

  bool has_bbox() const override { return true; }
  double south() const override;
  double north() const override;
  double west() const override;
  double east() const override;

  bool has_line_geometry() const override { return true; }
  const std::vector< Point_Double >* get_line_geometry() const override { return &points; }

  bool has_multiline_geometry() const override { return false; }
  bool has_components() const override { return false; }

  unsigned int way_size() const override { return points.size(); }
  bool has_faithful_way_geometry() const override { return true; }
  bool way_pos_is_valid(unsigned int pos) const override { return pos < points.size(); }
  double way_pos_lat(unsigned int pos) const override { return points[pos].lat; }
  double way_pos_lon(unsigned int pos) const override { return points[pos].lon; }

  bool has_faithful_relation_geometry() const override { return false; }
  bool relation_pos_is_valid(unsigned int member_pos) const override { return false; }
  double relation_pos_lat(unsigned int member_pos) const override { return 0; }
  double relation_pos_lon(unsigned int member_pos) const override { return 0; }
  unsigned int relation_way_size(unsigned int member_pos) const override { return 0; }
  bool relation_pos_is_valid(unsigned int member_pos, unsigned int nd_pos) const override { return false; }
  double relation_pos_lat(unsigned int member_pos, unsigned int nd_pos) const override { return 0; }
  double relation_pos_lon(unsigned int member_pos, unsigned int nd_pos) const override { return 0; }

  bool relevant_to_bbox(const Bbox_Double& bbox) const override;

private:
  std::vector< Point_Double > points;
  mutable Bbox_Double* bounds;
};


class Partial_Way_Geometry final : public Opaque_Geometry
{
public:
  Partial_Way_Geometry() : bounds(0), has_coords(false) {}
  Partial_Way_Geometry(const std::vector< Point_Double >& points_);
  ~Partial_Way_Geometry() override { delete bounds; }
  Opaque_Geometry* clone() const override { return new Partial_Way_Geometry(points); }

  bool has_center() const override { return has_coords; }
  double center_lat() const override;
  double center_lon() const override;

  bool has_bbox() const override { return has_coords; }
  double south() const override;
  double north() const override;
  double west() const override;
  double east() const override;

  bool has_line_geometry() const override { return valid_segments.size() == 1; }
  const std::vector< Point_Double >* get_line_geometry() const override
  { return valid_segments.size() == 1 ? &valid_segments.front() : 0; }

  bool has_multiline_geometry() const override { return true; }
  const std::vector< std::vector< Point_Double > >* get_multiline_geometry() const override
  { return &valid_segments; }

  bool has_components() const override { return false; }

  unsigned int way_size() const override { return points.size(); }
  bool has_faithful_way_geometry() const override { return true; }
  bool way_pos_is_valid(unsigned int pos) const override { return pos < points.size() && points[pos].lat < 100.; }
  double way_pos_lat(unsigned int pos) const override { return points[pos].lat; }
  double way_pos_lon(unsigned int pos) const override { return points[pos].lon; }

  void add_point(const Point_Double& point);

  bool has_faithful_relation_geometry() const override { return false; }
  bool relation_pos_is_valid(unsigned int member_pos) const override { return false; }
  double relation_pos_lat(unsigned int member_pos) const override { return 0; }
  double relation_pos_lon(unsigned int member_pos) const override { return 0; }
  unsigned int relation_way_size(unsigned int member_pos) const override { return 0; }
  bool relation_pos_is_valid(unsigned int member_pos, unsigned int nd_pos) const override { return false; }
  double relation_pos_lat(unsigned int member_pos, unsigned int nd_pos) const override { return 0; }
  double relation_pos_lon(unsigned int member_pos, unsigned int nd_pos) const override { return 0; }

  bool relevant_to_bbox(const Bbox_Double& bbox) const override;

private:
  std::vector< Point_Double > points;
  std::vector< std::vector< Point_Double > > valid_segments;
  mutable Bbox_Double* bounds;
  bool has_coords;
};


class Free_Polygon_Geometry final : public Opaque_Geometry
{
public:
  Free_Polygon_Geometry() : bounds(0) {}
  Free_Polygon_Geometry(const std::vector< std::vector< Point_Double > >& linestrings_);
  ~Free_Polygon_Geometry() override { delete bounds; }
  Opaque_Geometry* clone() const override { return new Free_Polygon_Geometry(linestrings); }

  bool has_center() const override { return true; }
  double center_lat() const override;
  double center_lon() const override;

  bool has_bbox() const override { return true; }
  double south() const override;
  double north() const override;
  double west() const override;
  double east() const override;

  bool has_line_geometry() const override { return false; }
  bool has_multiline_geometry() const override { return true; }
  const std::vector< std::vector< Point_Double > >* get_multiline_geometry() const override { return &linestrings; }
  bool has_components() const override { return false; }

  unsigned int way_size() const override { return 0; }
  bool has_faithful_way_geometry() const override { return false; }
  bool way_pos_is_valid(unsigned int pos) const override { return false; }
  double way_pos_lat(unsigned int pos) const override { return 0; }
  double way_pos_lon(unsigned int pos) const override { return 0; }

  bool has_faithful_relation_geometry() const override { return false; }
  bool relation_pos_is_valid(unsigned int member_pos) const override { return false; }
  double relation_pos_lat(unsigned int member_pos) const override { return 0; }
  double relation_pos_lon(unsigned int member_pos) const override { return 0; }
  unsigned int relation_way_size(unsigned int member_pos) const override { return 0; }
  bool relation_pos_is_valid(unsigned int member_pos, unsigned int nd_pos) const override { return false; }
  double relation_pos_lat(unsigned int member_pos, unsigned int nd_pos) const override { return 0; }
  double relation_pos_lon(unsigned int member_pos, unsigned int nd_pos) const override { return 0; }

  bool relevant_to_bbox(const Bbox_Double& bbox) const override;

  void add_linestring(const std::vector< Point_Double >& linestring);

private:
  std::vector< std::vector< Point_Double > > linestrings;
  mutable Bbox_Double* bounds;
};


class RHR_Polygon_Geometry final : public Opaque_Geometry
{
public:
  RHR_Polygon_Geometry(const Free_Polygon_Geometry& rhs);
  ~RHR_Polygon_Geometry() override { delete bounds; }
  Opaque_Geometry* clone() const override { return new RHR_Polygon_Geometry(linestrings); }

  bool has_center() const override { return true; }
  double center_lat() const override;
  double center_lon() const override;

  bool has_bbox() const override { return true; }
  double south() const override;
  double north() const override;
  double west() const override;
  double east() const override;

  bool has_line_geometry() const override { return false; }
  bool has_multiline_geometry() const override { return true; }
  const std::vector< std::vector< Point_Double > >* get_multiline_geometry() const override { return &linestrings; }
  bool has_components() const override { return false; }

  unsigned int way_size() const override { return 0; }
  bool has_faithful_way_geometry() const override { return false; }
  bool way_pos_is_valid(unsigned int pos) const override { return false; }
  double way_pos_lat(unsigned int pos) const override { return 0; }
  double way_pos_lon(unsigned int pos) const override { return 0; }

  bool has_faithful_relation_geometry() const override { return false; }
  bool relation_pos_is_valid(unsigned int member_pos) const override { return false; }
  double relation_pos_lat(unsigned int member_pos) const override { return 0; }
  double relation_pos_lon(unsigned int member_pos) const override { return 0; }
  unsigned int relation_way_size(unsigned int member_pos) const override { return 0; }
  bool relation_pos_is_valid(unsigned int member_pos, unsigned int nd_pos) const override { return false; }
  double relation_pos_lat(unsigned int member_pos, unsigned int nd_pos) const override { return 0; }
  double relation_pos_lon(unsigned int member_pos, unsigned int nd_pos) const override { return 0; }

  bool relevant_to_bbox(const Bbox_Double& bbox) const override;

  void add_linestring(const std::vector< Point_Double >& linestring);

private:
  RHR_Polygon_Geometry(const std::vector< std::vector< Point_Double > >& linestrings_)
      : linestrings(linestrings_), bounds(0) {}

  std::vector< std::vector< Point_Double > > linestrings;
  mutable Bbox_Double* bounds;
};


class Compound_Geometry final : public Opaque_Geometry
{
public:
  Compound_Geometry() : bounds(0) {}
  Compound_Geometry(const std::vector< Opaque_Geometry* >& components_) : components(components_), bounds(0) {}
  ~Compound_Geometry() override
  {
    delete bounds;
    for (auto it = components.begin(); it != components.end(); ++it)
      delete *it;
  }
  Opaque_Geometry* clone() const override;

  bool has_center() const override;
  double center_lat() const override;
  double center_lon() const override;

  bool has_bbox() const override;
  double south() const override;
  double north() const override;
  double west() const override;
  double east() const override;

  bool has_line_geometry() const override { return false; }

  bool has_multiline_geometry() const override { return false; }

  bool has_components() const override { return true; }
  const std::vector< Opaque_Geometry* >* get_components() const override { return &components; }
  std::vector< Opaque_Geometry* >* move_components() override { return &components; }

  void add_component(Opaque_Geometry* component);

  unsigned int way_size() const override { return 0; }
  bool has_faithful_way_geometry() const override { return false; }
  bool way_pos_is_valid(unsigned int pos) const override { return false; }
  double way_pos_lat(unsigned int pos) const override { return 0; }
  double way_pos_lon(unsigned int pos) const override { return 0; }

  bool has_faithful_relation_geometry() const override { return true; }
  bool relation_pos_is_valid(unsigned int member_pos) const override;
  double relation_pos_lat(unsigned int member_pos) const override;
  double relation_pos_lon(unsigned int member_pos) const override;
  unsigned int relation_way_size(unsigned int member_pos) const override;
  bool relation_pos_is_valid(unsigned int member_pos, unsigned int nd_pos) const override;
  double relation_pos_lat(unsigned int member_pos, unsigned int nd_pos) const override;
  double relation_pos_lon(unsigned int member_pos, unsigned int nd_pos) const override;

  bool relevant_to_bbox(const Bbox_Double& bbox) const override;

private:
  std::vector< Opaque_Geometry* > components;
  mutable Bbox_Double* bounds;
};


class Partial_Relation_Geometry final : public Opaque_Geometry
{
public:
  Partial_Relation_Geometry() : bounds(0), has_coords(false) {}
  Partial_Relation_Geometry(const std::vector< Opaque_Geometry* >& components_)
      : components(components_), bounds(0), has_coords(false)
  {
    for (std::vector< Opaque_Geometry* >::const_iterator it = components.begin();
        it != components.end() && !has_coords; ++it)
    {
      auto* pt = dynamic_cast< Point_Geometry* >(*it);
      has_coords |= (bool)pt;
      auto* way = dynamic_cast< Partial_Way_Geometry* >(*it);
      if (way)
        has_coords |= way->has_center();
    }
  }
  ~Partial_Relation_Geometry() override
  {
    delete bounds;
    for (auto it = components.begin(); it != components.end(); ++it)
      delete *it;
  }
  Opaque_Geometry* clone() const override;

  bool has_center() const override;
  double center_lat() const override;
  double center_lon() const override;

  bool has_bbox() const override;
  double south() const override;
  double north() const override;
  double west() const override;
  double east() const override;

  bool has_line_geometry() const override { return false; }

  bool has_multiline_geometry() const override { return false; }

  bool has_components() const override { return true; }
  const std::vector< Opaque_Geometry* >* get_components() const override { return &components; }
  std::vector< Opaque_Geometry* >* move_components() override { return &components; }

  void add_placeholder();
  void add_point(const Point_Double& point);
  void start_way();
  void add_way_point(const Point_Double& point);
  void add_way_placeholder();

  unsigned int way_size() const override { return 0; }
  bool has_faithful_way_geometry() const override { return false; }
  bool way_pos_is_valid(unsigned int pos) const override { return false; }
  double way_pos_lat(unsigned int pos) const override { return 0; }
  double way_pos_lon(unsigned int pos) const override { return 0; }

  bool has_faithful_relation_geometry() const override { return true; }
  bool relation_pos_is_valid(unsigned int member_pos) const override;
  double relation_pos_lat(unsigned int member_pos) const override;
  double relation_pos_lon(unsigned int member_pos) const override;
  unsigned int relation_way_size(unsigned int member_pos) const override;
  bool relation_pos_is_valid(unsigned int member_pos, unsigned int nd_pos) const override;
  double relation_pos_lat(unsigned int member_pos, unsigned int nd_pos) const override;
  double relation_pos_lon(unsigned int member_pos, unsigned int nd_pos) const override;

  bool relevant_to_bbox(const Bbox_Double& bbox) const override;

private:
  std::vector< Opaque_Geometry* > components;
  mutable Bbox_Double* bounds;
  bool has_coords;
};


struct Cartesian
{
  Cartesian(double x_, double y_, double z_) : x(x_), y(y_), z(z_) {}

  Cartesian(double lat, double lon)
  {
    static double deg_to_arc = acos(0)/90.0;
    double c = cos(lat*deg_to_arc);
    x = sin(lat*deg_to_arc);
    y = c*sin(lon*deg_to_arc);
    z = c*cos(lon*deg_to_arc);
  }

  Cartesian() : x(0), y(0), z(0) {}

public:
  double x;
  double y;
  double z;
};


double length(const Opaque_Geometry& geometry);

Opaque_Geometry* make_trace(const Opaque_Geometry& geometry);

Opaque_Geometry* make_hull(const Opaque_Geometry& geometry);

double great_circle_dist(double lat1, double lon1, double lat2, double lon2);


#endif
