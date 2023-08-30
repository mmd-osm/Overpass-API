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

#include <array>
#include <cmath>
#include <cstring>
#include <memory>
#include <vector>

#include "type_node.h"
#include "index_computations.h"


struct Point_Double
{
public:
  Point_Double() = delete;
  Point_Double(double lat_, double lon_) : lat(lat_), lon(lon_) {}
  Point_Double(Quad_Coord arg) : lat(::lat(arg.ll_upper, arg.ll_lower)), lon(::lon(arg.ll_upper, arg.ll_lower)) {}

  double lat;
  double lon;

  bool operator==(const Point_Double& rhs) const { return lat == rhs.lat && lon == rhs.lon; }
  bool operator!=(const Point_Double& rhs) const { return !(*this == rhs); }
  bool operator<(const Point_Double& rhs) const
  { return lat != rhs.lat ? lat < rhs.lat : lon < rhs.lon; }

  bool epsilon_equal(const Point_Double& rhs) const
  { return fabs(lat - rhs.lat) < 1e-7 && fabs(lon - rhs.lon) < 1e-7; }
};

namespace {

constexpr std::array<char, 200> fill_array() {
    std::array<char, 200> v{0};
    for (char i = 0; i < 100; ++i) {
        v[2 * i] = '0' + i / 10;
        v[2 * i + 1] = '0' + i % 10;
    }
    return v;
}

}

/* Store lat lon values multiplied by scaling factor */

struct Location
{
public:
  Location() = delete;
  explicit Location(double lat, double lon) :
        m_x(lat * scaling_factor + (lat > 0 ? 0.5 : -0.5)),   // like ilat, but without 91.0 offset
        m_y(lon * scaling_factor + (lon > 0 ? 0.5 : -0.5)) {}
  explicit Location(int32_t x, int32_t y) : m_x(x), m_y(y) {}

  explicit Location(Quad_Coord arg) : m_x(::lat_scaled(arg.ll_upper, arg.ll_lower)), m_y(::lon_scaled(arg.ll_upper, arg.ll_lower)) {}

  bool operator==(const Location& rhs) const { return m_x == rhs.m_x && m_y == rhs.m_y; }
  bool operator!=(const Location& rhs) const { return !(*this == rhs); }
  bool operator<(const Location& rhs) const
  { return m_x != rhs.m_x ? m_x < rhs.m_x : m_y < rhs.m_y; }

  double lat() const { return (double) m_x / (double) scaling_factor; }
  double lon() const { return (double) m_y / (double) scaling_factor; }

  bool undefined() const { return m_x == undefined_coord && m_y == undefined_coord; }

  int32_t x() const { return m_x; }
  int32_t y() const { return m_y; }


  // small table version, about 2.8 times faster than backwards linear version
  // https://lemire.me/blog/2021/11/18/converting-integers-to-fix-digit-representations-quickly/

  // out should be sufficiently large char array, e.g. char lat_buffer[16];
  // call as Location::as_string_view(location, lat_buffer);


  static std::string_view as_string_view(int32_t input, char *out) {
      static constexpr std::array<char, 200> table = fill_array();

      const char* start = out;

      uint32_t x;
      if (input < 0) {
        x = -input;
        *out++ = '-';
      }
      else {
        x = input;
      }

      const uint32_t a = x / 10'000'000;
      const uint32_t b = x % 10'000'000;

      if (a >= 100) {
        const uint32_t aa = a / 10;
        const char ab = '0' + (a % 10);
        memcpy(out, &table[2 * aa], 2);
        memcpy(out + 2, &ab, 1);
        out += 3;
      }
      else if (a >= 10) {
        memcpy(out, &table[2 * a], 2);
        out += 2;
      }
      else
      {
        *out++ = '0' + a;
      }

      const uint32_t ba = b / 1000;
      const uint32_t bb = b % 1000;

      const uint32_t baa = ba / 100;
      const uint32_t bab = ba % 100;

      const uint32_t bba = bb / 10;
      const char bbb = '0' + (bb % 10);
      //
      const char dec = '.';

      memcpy(out, &dec, 1);
      memcpy(out + 1, &table[2 * baa], 2);
      memcpy(out + 3, &table[2 * bab], 2);
      memcpy(out + 5, &table[2 * bba], 2);
      memcpy(out + 7, &bbb, 1);

      return std::string_view(start, 8 + out - start);
  }

  constexpr static int32_t scaling_factor = 10000000;
  constexpr static int32_t undefined_coord = 2147483647;

private:
  int32_t m_x = undefined_coord;
  int32_t m_y = undefined_coord;
};



struct Bbox_Double
{
public:
  Bbox_Double() = delete;

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
  virtual const std::vector< Point_Double >* get_line_geometry() const { return nullptr; }

  virtual bool has_multiline_geometry() const = 0;
  virtual const std::vector< std::vector< Point_Double > >* get_multiline_geometry() const { return nullptr; }

  virtual bool has_components() const = 0;
  virtual const std::vector< Opaque_Geometry* >* get_components() const { return nullptr; }
  virtual std::vector< Opaque_Geometry* >* move_components() { return nullptr; }

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

  ~Null_Geometry() override = default;

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
  Point_Geometry(const Uint31_Index idx, const Node_Skeleton& node) :
       pt(::lat(idx.val(), node.ll_lower),
          ::lon(idx.val(), node.ll_lower)) { }

  Opaque_Geometry* clone() const override { return new Point_Geometry(pt.lat, pt.lon); }

  ~Point_Geometry() override = default;

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

  ~Bbox_Geometry() override = default;

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
  Linestring_Geometry(const std::vector< Point_Double >& points_) : points(points_) {}
  Linestring_Geometry(std::vector< Point_Double >&& points_) : points(std::move(points_)) {}
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
  mutable Bbox_Double* bounds = nullptr;
};


class Partial_Way_Geometry final : public Opaque_Geometry
{
public:
  Partial_Way_Geometry() = default;
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
  { return valid_segments.size() == 1 ? &valid_segments.front() : nullptr; }

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
  mutable Bbox_Double* bounds = nullptr;
  bool has_coords = false;
};


class Free_Polygon_Geometry final : public Opaque_Geometry
{
public:
  Free_Polygon_Geometry() = default;
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
  mutable Bbox_Double* bounds = nullptr;
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
      : linestrings(linestrings_) {}

  std::vector< std::vector< Point_Double > > linestrings;
  mutable Bbox_Double* bounds = nullptr;
};


class Compound_Geometry final : public Opaque_Geometry
{
public:
  Compound_Geometry() = default;
  Compound_Geometry(const std::vector< Opaque_Geometry* >& components_) : components(components_) {}
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
  mutable Bbox_Double* bounds = nullptr;
};


class Partial_Relation_Geometry final : public Opaque_Geometry
{
public:
  Partial_Relation_Geometry() = default;
  Partial_Relation_Geometry(const std::vector< Opaque_Geometry* >& components_)
      : components(components_)
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
  mutable Bbox_Double* bounds = nullptr;
  bool has_coords = false;
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

  Cartesian() = default;

public:
  double x = 0;
  double y = 0;
  double z = 0;
};


double length(const Opaque_Geometry& geometry);

Opaque_Geometry* make_trace(const Opaque_Geometry& geometry);

Opaque_Geometry* make_hull(const Opaque_Geometry& geometry);

double great_circle_dist(double lat1, double lon1, double lat2, double lon2);


class Great_Circle
{
public:
  Great_Circle(const Point_Double& lhs, const Point_Double& rhs)
  {
//     std::cout<<"gc "<<lhs.lat<<' '<<lhs.lon<<' '<<rhs.lat<<' '<<rhs.lon
//         <<' '<<(lhs.lon < rhs.lon)<<(rhs.lon - lhs.lon > 180.)<<
//         (rhs.lon < lhs.lon)<<(lhs.lon - rhs.lon < 180.)<<'\n';
    
    double lhs_s = sin(lhs.lat/180.*M_PI);
    double lhs_cos = cos(lhs.lat/180.*M_PI);
    double lhs_cs = lhs_cos * sin(lhs.lon/180.*M_PI);
    double lhs_cc = lhs_cos * cos(lhs.lon/180.*M_PI);
    
    double rhs_s = sin(rhs.lat/180.*M_PI);
    double rhs_cos = cos(rhs.lat/180.*M_PI);
    double rhs_cs = rhs_cos * sin(rhs.lon/180.*M_PI);
    double rhs_cc = rhs_cos * cos(rhs.lon/180.*M_PI);
    
    ortho_s = lhs_cs * rhs_cc - lhs_cc * rhs_cs;
    ortho_cs = lhs_cc * rhs_s - lhs_s * rhs_cc;
    ortho_cc = lhs_s * rhs_cs - lhs_cs * rhs_s;
    if ((lhs.lon < rhs.lon && rhs.lon - lhs.lon > 180.)
        || (rhs.lon < lhs.lon && lhs.lon - rhs.lon < 180.))
    {
      ortho_s = -ortho_s;
      ortho_cs = -ortho_cs;
      ortho_cc = -ortho_cc;
    }
    double norm = sqrt(ortho_s*ortho_s + ortho_cs*ortho_cs + ortho_cc*ortho_cc);
    if (norm > 0)
    {
      ortho_s /= norm;
      ortho_cs /= norm;
      ortho_cc /= norm;
    }
//     std::cout<<ortho_s<<' '<<ortho_cs<<' '<<ortho_cc<<'\n'
//         <<(ortho_s*ortho_s + ortho_cs*ortho_cs + ortho_cc*ortho_cc)<<'\n'
//         <<asin(ortho_s)/M_PI*180.<<' '<<asin(ortho_cs/sqrt(1 - ortho_s*ortho_s))/M_PI*180.<<'\n';
  }

  double lat_of(double lon) const
  {
    //rotate ortho such that the longitude to use for cartesian computation is always zero
    double g_cc = ortho_cc*cos(lon/180.*M_PI) + ortho_cs*sin(lon/180.*M_PI);
    double norm_prod = sqrt(g_cc*g_cc + ortho_s*ortho_s);
    if (g_cc > norm_prod)
      return 90.;
    return asin(g_cc/norm_prod)/M_PI*180.;
  }

private:
  double ortho_s;
  double ortho_cs;
  double ortho_cc;
};


#endif
