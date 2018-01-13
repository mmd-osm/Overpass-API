#include "four_field_index.h"
#include "geometry.h"
#include "index_computations.h"

#include <cmath>
#include <map>


const Bbox_Double Bbox_Double::invalid(100.0, 200.0, 100.0, 200.0);


double Bbox_Double::center_lat() const
{
  return (south + north) / 2;
}


double Bbox_Double::center_lon() const
{
  if (west <= east)
    return (west + east) / 2;
  else if ((west + east) / 2 <= 0)
    return (west + east) / 2 + 180.0;
  else
    return (west + east) / 2 - 180.0;
}


bool Bbox_Double::contains(const Point_Double& point) const
{
  if (point.lat < south || point.lat > north)
    return false;
  
  if (east >= west)
    return point.lon >= west && point.lon <= east;
  
  return point.lon >= west || point.lon <= east;
}


bool Bbox_Double::intersects(const Point_Double& from, const Point_Double& to) const
{
  // TODO: correct behaviour over 180°

  double from_lon = from.lon;
  if (from.lat < south)
  {
    if (to.lat < south)
      return false;
    // Otherwise just adjust from.lat and from.lon
    from_lon += (to.lon - from.lon)*(south - from.lat)/(to.lat - from.lat);
  }
  else if (from.lat > north)
  {
    if (to.lat > north)
      return false;
    // Otherwise just adjust from.lat and from.lon
    from_lon += (to.lon - from.lon)*(north - from.lat)/(to.lat - from.lat);
  }

  double to_lon = to.lon;
  if (to.lat < south)
    // Adjust to.lat and to.lon
    to_lon += (from.lon - to.lon)*(south - to.lat)/(from.lat - to.lat);
  else if (to.lat > north)
    // Adjust to.lat and to.lon
    to_lon += (from.lon - to.lon)*(north - to.lat)/(from.lat - to.lat);

  // Now we know that both latitudes are between south and north.
  // Thus we only need to check whether the segment touches the bbox in its east-west-extension.
  if (from_lon < west && to_lon < west)
    return false;
  if (from_lon > east && to_lon > east)
    return false;
  
  return true;
}


Bbox_Double* calc_bounds(const std::vector< Point_Double >& points)
{
  double south = 100.0;
  double west = 200.0;
  double north = -100.0;
  double east = -200.0;
  
  for (std::vector< Point_Double >::const_iterator it = points.begin(); it != points.end(); ++it)
  {
    if (it->lat < 100.)
    {
      south = std::min(south, it->lat);
      west = std::min(west, it->lon);
      north = std::max(north, it->lat);
      east = std::max(east, it->lon);
    }
  }
  
  if (north == -100.0)
    return new Bbox_Double(Bbox_Double::invalid);
  else if (east - west > 180.0)
    // In this special case we should check whether the bounding box should rather cross the date line
  {
    double wrapped_west = 180.0;
    double wrapped_east = -180.0;
    
    for (std::vector< Point_Double >::const_iterator it = points.begin(); it != points.end(); ++it)
    {
      if (it->lat < 100.)
      {
        if (it->lon > 0)
	  wrapped_west = std::min(wrapped_west, it->lon);
        else
	  wrapped_east = std::max(wrapped_east, it->lon);
      }
    }
    
    if (wrapped_west - wrapped_east > 180.0)
      return new Bbox_Double(south, wrapped_west, north, wrapped_east);
    else
      // The points go around the world, hence a bounding box limit doesn't make sense.
      return new Bbox_Double(south, -180.0, north, 180.0);
  }
  else
    return new Bbox_Double(south, west, north, east);
}


double Linestring_Geometry::center_lat() const
{
  if (!bounds)
    bounds = calc_bounds(points);
  
  return bounds->center_lat();
}


double Linestring_Geometry::center_lon() const
{
  if (!bounds)
    bounds = calc_bounds(points);
  
  return bounds->center_lon();
}


double Linestring_Geometry::south() const
{
  if (!bounds)
    bounds = calc_bounds(points);
  
  return bounds->south;
}


double Linestring_Geometry::north() const
{
  if (!bounds)
    bounds = calc_bounds(points);
  
  return bounds->north;
}


double Linestring_Geometry::west() const
{
  if (!bounds)
    bounds = calc_bounds(points);
  
  return bounds->west;
}


double Linestring_Geometry::east() const
{
  if (!bounds)
    bounds = calc_bounds(points);
  
  return bounds->east;
}


double Partial_Way_Geometry::center_lat() const
{
  if (!has_coords)
    return 0;
    
  if (!bounds)
    bounds = calc_bounds(points);
  
  return bounds->center_lat();
}


double Partial_Way_Geometry::center_lon() const
{
  if (!has_coords)
    return 0;
    
  if (!bounds)
    bounds = calc_bounds(points);
  
  return bounds->center_lon();
}


double Partial_Way_Geometry::south() const
{
  if (!has_coords)
    return 0;
    
  if (!bounds)
    bounds = calc_bounds(points);
  
  return bounds->south;
}


double Partial_Way_Geometry::north() const
{
  if (!has_coords)
    return 0;
    
  if (!bounds)
    bounds = calc_bounds(points);
  
  return bounds->north;
}


double Partial_Way_Geometry::west() const
{
  if (!has_coords)
    return 0;
    
  if (!bounds)
    bounds = calc_bounds(points);
  
  return bounds->west;
}


double Partial_Way_Geometry::east() const
{
  if (!has_coords)
    return 0;
    
  if (!bounds)
    bounds = calc_bounds(points);
  
  return bounds->east;
}

  
void Partial_Way_Geometry::add_point(const Point_Double& point)
{
  delete bounds;
  bounds = 0;

  has_coords |= (point.lat < 100.);
  points.push_back(point);
}


Bbox_Double* calc_bounds(const std::vector< std::vector< Point_Double > >& linestrings)
{
  double south = 100.0;
  double west = 200.0;
  double north = -100.0;
  double east = -200.0;
  
  for (std::vector< std::vector< Point_Double > >::const_iterator iti = linestrings.begin();
      iti != linestrings.end(); ++iti)
  {
    for (std::vector< Point_Double >::const_iterator it = iti->begin(); it != iti->end(); ++it)
    {
      if (it->lat < 100.)
      {
        south = std::min(south, it->lat);
        west = std::min(west, it->lon);
        north = std::max(north, it->lat);
        east = std::max(east, it->lon);
      }
    }
  }
  
  if (north == -100.0)
    return new Bbox_Double(Bbox_Double::invalid);
  else if (east - west > 180.0)
    // In this special case we should check whether the bounding box should rather cross the date line
  {
    double wrapped_west = 180.0;
    double wrapped_east = -180.0;
    
    for (std::vector< std::vector< Point_Double > >::const_iterator iti = linestrings.begin();
        iti != linestrings.end(); ++iti)
    {
      for (std::vector< Point_Double >::const_iterator it = iti->begin(); it != iti->end(); ++it)
      {
        if (it->lat < 100.)
        {
          if (it->lon > 0)
            wrapped_west = std::min(wrapped_west, it->lon);
          else
            wrapped_east = std::max(wrapped_east, it->lon);
        }
      }
    }
    
    if (wrapped_west - wrapped_east > 180.0)
      return new Bbox_Double(south, wrapped_west, north, wrapped_east);
    else
      // The points go around the world, hence a bounding box limit doesn't make sense.
      return new Bbox_Double(south, -180.0, north, 180.0);
  }
  else
    return new Bbox_Double(south, west, north, east);
}


double Free_Polygon_Geometry::center_lat() const
{
  if (!bounds)
    bounds = calc_bounds(linestrings);
  
  return bounds->center_lat();
}


double Free_Polygon_Geometry::center_lon() const
{
  if (!bounds)
    bounds = calc_bounds(linestrings);
  
  return bounds->center_lon();
}


double Free_Polygon_Geometry::south() const
{
  if (!bounds)
    bounds = calc_bounds(linestrings);
  
  return bounds->south;
}


double Free_Polygon_Geometry::north() const
{
  if (!bounds)
    bounds = calc_bounds(linestrings);
  
  return bounds->north;
}


double Free_Polygon_Geometry::west() const
{
  if (!bounds)
    bounds = calc_bounds(linestrings);
  
  return bounds->west;
}


double Free_Polygon_Geometry::east() const
{
  if (!bounds)
    bounds = calc_bounds(linestrings);
  
  return bounds->east;
}


void Free_Polygon_Geometry::add_linestring(const std::vector< Point_Double >& linestring)
{
  if (linestring.size() < 2)
    return;
  
  delete bounds;
  bounds = 0;
  linestrings.push_back(linestring);
  if (linestrings.back().front() != linestrings.back().back())
    linestrings.back().push_back(linestrings.back().front());
}


Point_Double interpolation_point(
    double orth_x, double orth_y, double orth_z,
    double lhs_gc_x, double lhs_gc_z, double factor, double lhs_lon)
{
  static const double deg_to_arc = acos(0)/90.;
  
  double sin_factor = sin(factor);
  double cos_factor = cos(factor);
  double new_x = sin_factor * orth_x + cos_factor * lhs_gc_x;
  double new_y = sin_factor * orth_y;
  double new_z = sin_factor * orth_z + cos_factor * lhs_gc_z;
      
  double new_lat = asin(new_x)/deg_to_arc;
  double new_lon = atan2(new_y, new_z)/deg_to_arc + lhs_lon;
  if (new_lon < -180.)
    new_lon += 360.;
  else if (new_lon > 180.)
    new_lon -= 360.;
      
  return Point_Double(new_lat, new_lon);
}


namespace
{
  struct Interpolation_Collector
  {
    Interpolation_Collector(double orth_x, double orth_y, double orth_z,
        double lhs_gc_x, double lhs_gc_z, double dist, double lhs_lon, std::vector< Point_Double >& target);
        
    void collect_single_point(const Point_Double& pt) { target->push_back(pt); }
    void collect_single_point(double dist);
    void collect_sequence(double from, double to, double max_step);
    void collect_center(double from, double to, int divisor);
    
    double orth_x;
    double orth_y;
    double orth_z;
    double lhs_gc_x;
    double lhs_gc_z;
    double lhs_lon;
    double center;
    Point_Double ex_pt;
    double ex_gc_pt_lat;
    double acceptable_max_length;
    std::vector< Point_Double >* target;
  };
  
  
  Interpolation_Collector::Interpolation_Collector(double orth_x_, double orth_y_, double orth_z_,
      double lhs_gc_x_, double lhs_gc_z_, double dist, double lhs_lon_, std::vector< Point_Double >& target_)
      : orth_x(orth_x_), orth_y(orth_y_), orth_z(orth_z_),
      lhs_gc_x(lhs_gc_x_), lhs_gc_z(lhs_gc_z_), lhs_lon(lhs_lon_),
      center(lhs_gc_x == 0 ? dist : atan(orth_x/lhs_gc_x)),
      ex_pt(interpolation_point(orth_x, orth_y, orth_z, lhs_gc_x, lhs_gc_z,
          std::max(std::min(center, dist), 0.), lhs_lon)),
      ex_gc_pt_lat(ex_pt.lat),
      target(&target_)
  {
    static const double deg_to_arc = acos(0)/90.;
    acceptable_max_length = .0065536*cos(ex_pt.lat * deg_to_arc);
    
    double bounded_center = std::max(std::min(center, dist), 0.);
    if (acceptable_max_length < .0016384 && center != bounded_center)
    {
      Point_Double ex_gc_pt = interpolation_point(orth_x, orth_y, orth_z,
          lhs_gc_x, lhs_gc_z, center, lhs_lon);
      ex_gc_pt_lat = ex_gc_pt.lat;
    }
    center = bounded_center;
  }
  
  
  void Interpolation_Collector::collect_single_point(double dist)
  {
    target->push_back(interpolation_point(orth_x, orth_y, orth_z, lhs_gc_x, lhs_gc_z, dist, lhs_lon));
  }
  
  
  void Interpolation_Collector::collect_sequence(double from, double to, double max_step)
  {
    int num_sections = (int)((to - from)/max_step)+1;
    for (int j = 1; j < num_sections; ++j)
      target->push_back(interpolation_point(orth_x, orth_y, orth_z,
          lhs_gc_x, lhs_gc_z, from + (to - from)*j/num_sections, lhs_lon));
  }
  
  
  void Interpolation_Collector::collect_center(double from, double to, int divisor)
  {
    static const double deg_to_arc = acos(0)/90.;
    
    double max_length_threshold = .0065536/divisor;
    if (acceptable_max_length < max_length_threshold && divisor < 65536)
    {
      double dist_ex_s = acos(sqrt(1-1./divisor/divisor)/sin(ex_gc_pt_lat * deg_to_arc));
      double bound_l = std::max(std::min(center - dist_ex_s, to), from);
      double bound_r = std::max(std::min(center + dist_ex_s, to), from);

      collect_sequence(from, bound_l, max_length_threshold*deg_to_arc);
      if (bound_l > 0 && bound_l < to)
        collect_single_point(bound_l);
        
      collect_center(bound_l, bound_r, divisor*4);
        
      if (bound_r > 0 && bound_r < to)
        collect_single_point(bound_r);
      collect_sequence(bound_r, to, max_length_threshold*deg_to_arc);
    }
    else
    {
      if (acceptable_max_length < 1e-9)
        acceptable_max_length = 1e-9;
      acceptable_max_length *= deg_to_arc;
      
      collect_sequence(from, center, acceptable_max_length);
      if (center > from && center < to)
        collect_single_point(center);
      collect_sequence(center, to, acceptable_max_length);
    }
  }
}


void interpolate_segment(double lhs_lat, double lhs_lon, double rhs_lat, double rhs_lon,
    std::vector< Point_Double >& target)
{
  static const double deg_to_arc = acos(0)/90.;
  
  if (fabs(rhs_lat - lhs_lat) < .0065536 && fabs(rhs_lon - lhs_lon) < .0065536)
    target.push_back(Point_Double(rhs_lat, rhs_lon));
  else
  {
    if (fabs(rhs_lon - lhs_lon) > 180.)
      rhs_lon = (lhs_lon < 0 ? rhs_lon - 360. : rhs_lon + 360.);
    
    if (fabs(rhs_lon - lhs_lon) > 179.999999)
    {
      double pole_lat = (lhs_lat + rhs_lat >= 0 ? 90. : -90.);
      
      int num_sections = (int)(fabs(pole_lat - lhs_lat)/.0065536)+1;
      for (int j = 1; j < num_sections; ++j)
        target.push_back(Point_Double(lhs_lat + (pole_lat - lhs_lat)*j/num_sections, lhs_lon));
      target.push_back(Point_Double(pole_lat, lhs_lon));
      
      num_sections = (int)(fabs(rhs_lon - lhs_lon)/.0065536)+1;
      for (int j = 1; j < num_sections; ++j)
        target.push_back(Point_Double(pole_lat, lhs_lon + (rhs_lon - lhs_lon)*j/num_sections));
      target.push_back(Point_Double(pole_lat, rhs_lon));
      
      num_sections = (int)(fabs(rhs_lat - pole_lat)/.0065536)+1;
      for (int j = 1; j < num_sections; ++j)
        target.push_back(Point_Double(pole_lat + (rhs_lat - pole_lat)*j/num_sections, rhs_lon));
    }
    else
    {
      double lhs_cos = cos(lhs_lat * deg_to_arc);
      double lhs_gc_x = sin(lhs_lat * deg_to_arc);
      double lhs_gc_z = lhs_cos;
    
      double rhs_cos = cos(rhs_lat * deg_to_arc);
      double rhs_gc_x = sin(rhs_lat * deg_to_arc);
      double rhs_gc_y = sin((rhs_lon - lhs_lon) * deg_to_arc) * rhs_cos;
      double rhs_gc_z = cos((rhs_lon - lhs_lon) * deg_to_arc) * rhs_cos;
    
      double prod = lhs_gc_x * rhs_gc_x + lhs_gc_z * rhs_gc_z;
      double dist = acos(prod);
    
      double orth_x = rhs_gc_x - prod * lhs_gc_x;
      double orth_y = rhs_gc_y;
      double orth_z = rhs_gc_z - prod * lhs_gc_z;
      double lg_orth = sqrt(orth_x * orth_x + orth_y * orth_y + orth_z * orth_z);
      orth_x /= lg_orth;
      orth_y /= lg_orth;
      orth_z /= lg_orth;
    
      Interpolation_Collector collector(orth_x, orth_y, orth_z, lhs_gc_x, lhs_gc_z, dist, lhs_lon, target);
      collector.collect_center(0, dist, 4);
    }

    if (rhs_lon < -180.)
      rhs_lon += 360.;
    else if (rhs_lon > 180.)
      rhs_lon -= 360.;
    target.push_back(Point_Double(rhs_lat, rhs_lon));
  }
}


void add_segment(std::map< uint32, std::vector< unsigned int > >& segments_per_idx,
    const Point_Double& from, const Point_Double& to, unsigned int pos)
{
  uint32 lhs_ilat = ::ilat(from.lat);
  int32 lhs_ilon = ::ilon(from.lon);
  uint32 rhs_ilat = ::ilat(to.lat);
  int32 rhs_ilon = ::ilon(to.lon);
  
  segments_per_idx[(lhs_ilat & 0xffff0000) | (uint32(lhs_ilon)>>16)].push_back(pos);
  if ((lhs_ilon & 0xffff0000) != (rhs_ilon & 0xffff0000))
    segments_per_idx[(lhs_ilat & 0xffff0000) | (uint32(rhs_ilon)>>16)].push_back(pos);
  if ((lhs_ilat & 0xffff0000) != (rhs_ilat & 0xffff0000))
  {
    segments_per_idx[(rhs_ilat & 0xffff0000) | (uint32(lhs_ilon)>>16)].push_back(pos);
    if ((lhs_ilon & 0xffff0000) != (rhs_ilon & 0xffff0000))
      segments_per_idx[(rhs_ilat & 0xffff0000) | (uint32(rhs_ilon)>>16)].push_back(pos);
  }
}


void replace_segment(std::vector< unsigned int >& segments, unsigned int old_pos, unsigned int new_pos)
{
  for (std::vector< unsigned int >::iterator it = segments.begin(); it != segments.end(); ++it)
  {
    if (*it == old_pos)
      *it = new_pos;
  }
}


void replace_segment(std::map< uint32, std::vector< unsigned int > >& segments_per_idx,
    const Point_Double& from, const Point_Double& via, const Point_Double& to,
    unsigned int old_pos, unsigned int new_pos)
{
  uint32 lhs_ilat = ::ilat(from.lat) & 0xffff0000;
  int32 lhs_ilon = ::ilon(from.lon) & 0xffff0000;
  uint32 rhs_ilat = ::ilat(to.lat) & 0xffff0000;
  int32 rhs_ilon = ::ilon(to.lon) & 0xffff0000;
  uint32 via_ilat = ::ilat(via.lat) & 0xffff0000;
  int32 via_ilon = ::ilon(via.lon) & 0xffff0000;
  
  // The upper part is the one that possibly crosses the index boundary
  if (via_ilat == lhs_ilat && via_ilon == lhs_ilon)
    ++new_pos;
  
  replace_segment(segments_per_idx[lhs_ilat | (uint32(lhs_ilon)>>16)], old_pos, new_pos);
  if (lhs_ilon != rhs_ilon)
    replace_segment(segments_per_idx[lhs_ilat | (uint32(rhs_ilon)>>16)], old_pos, new_pos);
  if (lhs_ilat != rhs_ilat)
  {
    replace_segment(segments_per_idx[rhs_ilat | (uint32(lhs_ilon)>>16)], old_pos, new_pos);
    if (lhs_ilon != rhs_ilon)
    {
      if ((via_ilat == lhs_ilat && via_ilon == lhs_ilon) || (via_ilat == rhs_ilat && via_ilon == rhs_ilon))
        replace_segment(segments_per_idx[rhs_ilat | (uint32(rhs_ilon)>>16)], old_pos, new_pos);
      else
      {
        segments_per_idx[lhs_ilat | (uint32(rhs_ilon)>>16)].push_back(new_pos+1);
        segments_per_idx[rhs_ilat | (uint32(lhs_ilon)>>16)].push_back(new_pos+1);
        replace_segment(segments_per_idx[rhs_ilat | (uint32(rhs_ilon)>>16)], old_pos, new_pos+1);
      }
    }
  }
  
  if (via_ilat == lhs_ilat && via_ilon == lhs_ilon)
    segments_per_idx[lhs_ilat | (uint32(lhs_ilon)>>16)].push_back(new_pos-1);
  else if (via_ilat == rhs_ilat && via_ilon == rhs_ilon)
    segments_per_idx[rhs_ilat | (uint32(rhs_ilon)>>16)].push_back(new_pos+1);
}


bool try_intersect(const Point_Double& lhs_from, const Point_Double& lhs_to,
    const Point_Double& rhs_from, const Point_Double& rhs_to, Point_Double& isect)
{
  double rfmlt_lat = rhs_from.lat - lhs_to.lat;
  double rfmlt_lon = rhs_from.lon - lhs_to.lon;
  
  //The two segments are connected by a vertex
  if (!rfmlt_lat && !rfmlt_lon)
    return false;
  if (lhs_from == rhs_to || lhs_from == rhs_from || lhs_to == rhs_to)
    return false;
  
  double lfmlt_lat = lhs_from.lat - lhs_to.lat;
  double lfmlt_lon = lhs_from.lon - lhs_to.lon;
  double rfmrt_lat = rhs_from.lat - rhs_to.lat;
  double rfmrt_lon = rhs_from.lon - rhs_to.lon;
  double det = lfmlt_lat * rfmrt_lon - rfmrt_lat * lfmlt_lon;
  
  if (det == 0)
  {
    // Segments on parallel but distinct beams
    if (lfmlt_lat * rfmlt_lon - lfmlt_lon * rfmlt_lat != 0)
      return false;
    
    if (fabs(rfmlt_lat) > fabs(rfmlt_lon))
    {
      if (lhs_from.lat < lhs_to.lat)
      {
        if (rhs_from.lat < rhs_to.lat)
        {
          // Segments are non-overlapping
          if (lhs_to.lat < rhs_from.lat || rhs_to.lat < lhs_from.lat)
            return false;
          
          if (lhs_from.lat < rhs_from.lat)
            isect = rhs_from;
          else
            isect = lhs_from;
        }
        else
        {
          // Segments are non-overlapping
          if (lhs_to.lat < rhs_to.lat || rhs_from.lat < lhs_from.lat)
            return false;
          
          if (lhs_from.lat < rhs_to.lat)
            isect = rhs_to;
          else
            isect = lhs_from;
        }
      }
      else
      {
        if (rhs_from.lat < rhs_to.lat)
        {
          // Segments are non-overlapping
          if (lhs_from.lat < rhs_from.lat || rhs_to.lat < lhs_to.lat)
            return false;
          
          if (lhs_to.lat < rhs_from.lat)
            isect = rhs_from;
          else
            isect = lhs_to;
        }
        else
        {
          // Segments are non-overlapping
          if (lhs_from.lat < rhs_to.lat || rhs_from.lat < lhs_to.lat)
            return false;
          
          if (lhs_to.lat < rhs_to.lat)
            isect = rhs_to;
          else
            isect = lhs_to;
        }
      }
    }
    else
    {
      if (lhs_from.lon < lhs_to.lon)
      {
        if (rhs_from.lon < rhs_to.lon)
        {
          // Segments are non-overlapping
          if (lhs_to.lon < rhs_from.lon || rhs_to.lon < lhs_from.lon)
            return false;
          
          if (lhs_from.lon < rhs_from.lon)
            isect = rhs_from;
          else
            isect = lhs_from;
        }
        else
        {
          // Segments are non-overlapping
          if (lhs_to.lon < rhs_to.lon || rhs_from.lon < lhs_from.lon)
            return false;
          
          if (lhs_from.lon < rhs_to.lon)
            isect = rhs_to;
          else
            isect = lhs_from;
        }
      }
      else
      {
        if (rhs_from.lon < rhs_to.lon)
        {
          // Segments are non-overlapping
          if (lhs_from.lon < rhs_from.lon || rhs_to.lon < lhs_to.lon)
            return false;
          
          if (lhs_to.lon < rhs_from.lon)
            isect = rhs_from;
          else
            isect = lhs_to;
        }
        else
        {
          // Segments are non-overlapping
          if (lhs_from.lon < rhs_to.lon || rhs_from.lon < lhs_to.lon)
            return false;
          
          if (lhs_to.lon < rhs_to.lon)
            isect = rhs_to;
          else
            isect = lhs_to;
        }
      }
    }
    return true;
  }
  
  double x = (rfmrt_lon * rfmlt_lat - rfmrt_lat * rfmlt_lon)/det;
  double y = (lfmlt_lat * rfmlt_lon - lfmlt_lon * rfmlt_lat)/det;
  
  if (x <= -1e-7 || x >= 1+1e-7 || y <= -1e-7 || y >= 1+1e-7)
    return false;
  
  isect.lat = lhs_to.lat + x * lfmlt_lat;
  isect.lon = lhs_to.lon + x * lfmlt_lon;
  return true;
}


struct Idx_Per_Point_Double
{
  Idx_Per_Point_Double(const Point_Double& pt_, unsigned int idx_, unsigned int remote_idx_)
      : pt(pt_), idx(idx_), remote_idx(remote_idx_) {}
  
  Point_Double pt;
  unsigned int idx;
  unsigned int remote_idx;
  
  bool operator<(const Idx_Per_Point_Double& rhs) const
  {
    if (pt.lat != rhs.pt.lat)
      return pt.lat < rhs.pt.lat;
    if (pt.lon != rhs.pt.lon)
      return pt.lon < rhs.pt.lon;
    
    return idx < rhs.idx;
  }
};


struct Line_Divertion
{
  Line_Divertion(const Idx_Per_Point_Double& from_pt, const Idx_Per_Point_Double& to_pt)
      : from_idx(from_pt.idx), from_remote_idx(from_pt.remote_idx),
      to_idx(to_pt.idx), to_remote_idx(to_pt.remote_idx) {}
  Line_Divertion(unsigned int from_idx_, unsigned int to_idx_)
      : from_idx(from_idx_), from_remote_idx(from_idx_), to_idx(to_idx_), to_remote_idx(to_idx_) {}
  
  unsigned int from_idx;
  unsigned int from_remote_idx;
  unsigned int to_idx;
  unsigned int to_remote_idx;
  
  bool operator<(const Line_Divertion& rhs) const
  {
    if (from_idx != rhs.from_idx)
      return from_idx < rhs.from_idx;
    
    return from_remote_idx < rhs.from_remote_idx;
  }
};


void split_segments(
    std::vector< Point_Double >& all_segments,
    std::vector< unsigned int >& gap_positions,
    std::map< uint32, std::vector< unsigned int > >& segments_per_idx)
{
  for (std::map< uint32, std::vector< unsigned int > >::iterator idx_it = segments_per_idx.begin();
      idx_it != segments_per_idx.end(); ++idx_it)
  {
    Point_Double isect(100, 0);
    for (unsigned int i = 1; i < idx_it->second.size(); ++i)
    {
      for (unsigned int j = 0; j < i; ++j)
      {
        if (try_intersect(all_segments[idx_it->second[j]], all_segments[idx_it->second[j]+1],
              all_segments[idx_it->second[i]], all_segments[idx_it->second[i]+1], isect))
        {
          uint32 lhs_ilat = ::ilat(isect.lat);
          int32 lhs_ilon = ::ilon(isect.lon);
          
          if (((lhs_ilat & 0xffff0000) | (uint32(lhs_ilon)>>16)) == idx_it->first)
            // Ensure that the same intersection is processed only in one index
          {
            // Avoid rounding artifacts
            for (unsigned int k = 0; k < idx_it->second.size(); ++k)
            {
              if (isect.epsilon_equal(all_segments[idx_it->second[k]]))
              {
                isect.lat = all_segments[idx_it->second[k]].lat;
                isect.lon = all_segments[idx_it->second[k]].lon;
              }
              if (isect.epsilon_equal(all_segments[idx_it->second[k]+1]))
              {
                isect.lat = all_segments[idx_it->second[k]+1].lat;
                isect.lon = all_segments[idx_it->second[k]+1].lon;
              }
            }
            
            if (isect != all_segments[idx_it->second[j]] && isect != all_segments[idx_it->second[j]+1])
            {
              all_segments.push_back(all_segments[idx_it->second[j]]);
              all_segments.push_back(isect);
              all_segments.push_back(all_segments[idx_it->second[j]+1]);
              gap_positions.insert(std::lower_bound(
                  gap_positions.begin(), gap_positions.end(), idx_it->second[j]+1), idx_it->second[j]+1);
              gap_positions.push_back(all_segments.size());
              
              replace_segment(segments_per_idx, all_segments[idx_it->second[j]], isect,
                  all_segments[idx_it->second[j]+1], idx_it->second[j], all_segments.size()-3);
            }
            
            if (isect != all_segments[idx_it->second[i]] && isect != all_segments[idx_it->second[i]+1])
            {
              all_segments.push_back(all_segments[idx_it->second[i]]);
              all_segments.push_back(isect);
              all_segments.push_back(all_segments[idx_it->second[i]+1]);
              gap_positions.insert(std::lower_bound(
                  gap_positions.begin(), gap_positions.end(), idx_it->second[i]+1), idx_it->second[i]+1);
              gap_positions.push_back(all_segments.size());
              
              replace_segment(segments_per_idx, all_segments[idx_it->second[i]], isect,
                  all_segments[idx_it->second[i]+1], idx_it->second[i], all_segments.size()-3);
            }
          }
          
          //TODO: check and track common segments
        }
      }
    }
  }
}


void collect_divertions(const std::vector< Point_Double >& all_segments,
    uint32 idx, const std::vector< unsigned int >& segments,
    std::vector< Line_Divertion >& divertions)
{
  std::vector< Idx_Per_Point_Double > pos_per_pt;
  
  for (std::vector< unsigned int >::const_iterator seg_it = segments.begin();
      seg_it != segments.end(); ++seg_it)
  {
    uint32 lhs_ilat = ::ilat(all_segments[*seg_it].lat);
    int32 lhs_ilon = ::ilon(all_segments[*seg_it].lon);
    if (((lhs_ilat & 0xffff0000) | (uint32(lhs_ilon)>>16)) == idx)
      pos_per_pt.push_back(Idx_Per_Point_Double(all_segments[*seg_it], *seg_it, *seg_it+1));
    
    uint32 rhs_ilat = ::ilat(all_segments[*seg_it+1].lat);
    int32 rhs_ilon = ::ilon(all_segments[*seg_it+1].lon);
    if (((rhs_ilat & 0xffff0000) | (uint32(rhs_ilon)>>16)) == idx)
      pos_per_pt.push_back(Idx_Per_Point_Double(all_segments[*seg_it+1], *seg_it+1, *seg_it));
  }
  
  if (pos_per_pt.empty())
    return;
  
  std::sort(pos_per_pt.begin(), pos_per_pt.end());
  
  unsigned int same_since = 0;
  unsigned int i = 0;
  while (i <= pos_per_pt.size())
  {
    if (i == pos_per_pt.size() || pos_per_pt[i].pt != pos_per_pt[same_since].pt)
    {
      if (i - same_since == 2)
      {
        divertions.push_back(Line_Divertion(pos_per_pt[same_since], pos_per_pt[same_since+1]));
        divertions.push_back(Line_Divertion(pos_per_pt[same_since+1], pos_per_pt[same_since]));
      }
      else
      {
        std::vector< std::pair< double, unsigned int > > line_per_gradient;
        for (unsigned int j = same_since; j < i; ++j)
          line_per_gradient.push_back(std::make_pair(
              atan2(all_segments[pos_per_pt[j].remote_idx].lon - all_segments[pos_per_pt[j].idx].lon,
                  all_segments[pos_per_pt[j].remote_idx].lat - all_segments[pos_per_pt[j].idx].lat),
              j));
          
        std::sort(line_per_gradient.begin(), line_per_gradient.end());
        
        for (unsigned int j = 0; j < line_per_gradient.size(); j += 2)
        {
          if (j+3 < line_per_gradient.size()
              && line_per_gradient[j+1].first == line_per_gradient[j+2].first)
          {
            divertions.push_back(Line_Divertion(pos_per_pt[line_per_gradient[j].second],
                pos_per_pt[line_per_gradient[j+3].second]));
            divertions.push_back(Line_Divertion(pos_per_pt[line_per_gradient[j+1].second],
                pos_per_pt[line_per_gradient[j+2].second]));
            divertions.push_back(Line_Divertion(pos_per_pt[line_per_gradient[j+2].second],
                pos_per_pt[line_per_gradient[j+1].second]));
            divertions.push_back(Line_Divertion(pos_per_pt[line_per_gradient[j+3].second],
                pos_per_pt[line_per_gradient[j].second]));
            j += 2;
          }
          else
          {
            divertions.push_back(Line_Divertion(pos_per_pt[line_per_gradient[j].second],
                pos_per_pt[line_per_gradient[j+1].second]));
            divertions.push_back(Line_Divertion(pos_per_pt[line_per_gradient[j+1].second],
                pos_per_pt[line_per_gradient[j].second]));
          }
        }
      }
      
      same_since = i;
    }
    ++i;
  }
}


void assemble_linestrings(
    const std::vector< Point_Double >& all_segments, unsigned int gap_positions_size,
    std::vector< Line_Divertion >& divertions,
    std::vector< std::vector< Point_Double > >& linestrings)
{
  std::sort(divertions.begin(), divertions.end());
  
  unsigned int pos = 0;
  unsigned int count = 0;
  
  while (count < all_segments.size()+1-gap_positions_size)
  {
    while (divertions[2*pos + 1].from_remote_idx == pos)
    {
      ++pos;
      if (pos == all_segments.size()-1)
        pos = 0;
    }
    
    linestrings.push_back(std::vector< Point_Double >());
    std::vector< Point_Double >& cur_target = linestrings.back();
    
    int dir = 1;
    while (divertions[2*pos + dir].from_remote_idx != pos + (dir-1)/2)
    {
      if (cur_target.size() >= 2 && all_segments[pos].epsilon_equal(cur_target[cur_target.size()-2]))
        // Remove pairs of equal segments
        cur_target.pop_back();
      else if (cur_target.empty() || !all_segments[pos].epsilon_equal(cur_target.back()))
        cur_target.push_back(all_segments[pos]);
      
      ++count;
      divertions[2*pos + dir].from_remote_idx = pos + (dir-1)/2;
      
      pos = (int)pos + dir;
      
      Line_Divertion& divertion = divertions[2*pos + (1-dir)/2];
      dir = divertion.to_remote_idx - divertion.to_idx;
      pos = divertion.to_idx;
    }
    
    ++pos;
    if (pos == all_segments.size()-1)
      pos = 0;
    
    if (cur_target.size() <= 2)
      linestrings.pop_back();
    else
      cur_target.push_back(cur_target.front());
  }
}


struct RHR_Polygon_Area_Oracle : Area_Oracle
{
  RHR_Polygon_Area_Oracle(
      const std::vector< Point_Double >& all_segments_,
      const std::map< uint32, std::vector< unsigned int > >& segments_per_idx_)
      : all_segments(&all_segments_), segments_per_idx(&segments_per_idx_) {}
  
  virtual void build_area(bool sw_corner_inside, int32 value, bool* se_corner_inside, bool* nw_corner_inside);
  virtual Area_Oracle::point_status get_point_status(int32 value, double lat, double lon);
  
private:
  const std::vector< Point_Double >* all_segments;
  const std::map< uint32, std::vector< unsigned int > >* segments_per_idx;
  std::set< uint32 > inside_corners;
};


void RHR_Polygon_Area_Oracle::build_area(
    bool sw_corner_inside, int32 value, bool* se_corner_inside, bool* nw_corner_inside)
{
  std::map< uint32, std::vector< unsigned int > >::const_iterator spi_it = segments_per_idx->find(value);
  if (spi_it == segments_per_idx->end())
    return;
  
  if (sw_corner_inside)
    inside_corners.insert(value);
  
  if (::lon(uint32(value)<<16) > -179.99)
  {
    if (nw_corner_inside)
    {
      *nw_corner_inside = sw_corner_inside;
    
      for (std::vector< unsigned int >::const_iterator seg_it = spi_it->second.begin();
          seg_it != spi_it->second.end(); ++seg_it)
      {
        int32 lhs_ilon = ::ilon((*all_segments)[*seg_it].lon) & 0xffff0000;
        int32 rhs_ilon = ::ilon((*all_segments)[*seg_it+1].lon) & 0xffff0000;
      
        if ((lhs_ilon < (value<<16) && rhs_ilon == (value<<16))
            || (lhs_ilon == (value<<16) && rhs_ilon < (value<<16)))
        {
          double isect_lat = (*all_segments)[*seg_it].lat
              + ((*all_segments)[*seg_it+1].lat - (*all_segments)[*seg_it].lat)
                  *(::lon(uint32(value)<<16) - (*all_segments)[*seg_it].lon)
                  /((*all_segments)[*seg_it+1].lon - (*all_segments)[*seg_it].lon);
          if ((::ilat(isect_lat) & 0xffff0000) == (value & 0xffff0000))
            *nw_corner_inside = !*nw_corner_inside;
        }
      }
    }
  
    if (se_corner_inside)
    {
      *se_corner_inside = sw_corner_inside;
    
      for (std::vector< unsigned int >::const_iterator seg_it = spi_it->second.begin();
          seg_it != spi_it->second.end(); ++seg_it)
      {
        uint32 lhs_ilat = ::ilat((*all_segments)[*seg_it].lat) & 0xffff0000;
        uint32 rhs_ilat = ::ilat((*all_segments)[*seg_it+1].lat) & 0xffff0000;
      
        if ((lhs_ilat < (value & 0xffff0000) && rhs_ilat == (value & 0xffff0000))
            || (lhs_ilat == (value & 0xffff0000) && rhs_ilat < (value & 0xffff0000)))
        {
          double isect_lon = (*all_segments)[*seg_it].lon
              + ((*all_segments)[*seg_it+1].lon - (*all_segments)[*seg_it].lon)
                  *(::lat(value & 0xffff0000) - (*all_segments)[*seg_it].lat)
                  /((*all_segments)[*seg_it+1].lat - (*all_segments)[*seg_it].lat);
          if ((::ilon(isect_lon) & 0xffff0000) == (uint32(value)<<16))
            *se_corner_inside = !*se_corner_inside;
        }
      }
    }
  }
  else
  {
    if (nw_corner_inside)
    {
      *nw_corner_inside = sw_corner_inside;
    
      for (std::vector< unsigned int >::const_iterator seg_it = spi_it->second.begin();
          seg_it != spi_it->second.end(); ++seg_it)
      {
        double lhs_lon = (*all_segments)[*seg_it].lon;
        lhs_lon -= lhs_lon > 0 ? 360. : 0.;
        double rhs_lon = (*all_segments)[*seg_it+1].lon;
        rhs_lon -= rhs_lon > 0 ? 360. : 0.;
          
        if (::lon(uint32(value)<<16 > -180.))
        {
          int32 lhs_ilon = ::ilon(lhs_lon) & 0xffff0000;
          int32 rhs_ilon = ::ilon(rhs_lon) & 0xffff0000;
      
          if ((lhs_ilon < (value<<16) && rhs_ilon == (value<<16))
              || (lhs_ilon == (value<<16) && rhs_ilon < (value<<16)))
          {
            double isect_lat = (*all_segments)[*seg_it].lat
                + ((*all_segments)[*seg_it+1].lat - (*all_segments)[*seg_it].lat)
                    *(::lon(uint32(value)<<16) - (*all_segments)[*seg_it].lon)
                    /((*all_segments)[*seg_it+1].lon - (*all_segments)[*seg_it].lon);
            if ((::ilat(isect_lat) & 0xffff0000) == (value & 0xffff0000))
              *nw_corner_inside = !*nw_corner_inside;
          }
        }
        else
        {
          if ((lhs_lon <= -180. && rhs_lon > -180.) || (rhs_lon <= -180. && lhs_lon > -180.))
          {
            double isect_lat = (*all_segments)[*seg_it].lat
                + ((*all_segments)[*seg_it+1].lat - (*all_segments)[*seg_it].lat)
                    *(-180. - (*all_segments)[*seg_it].lon)
                    /((*all_segments)[*seg_it+1].lon - (*all_segments)[*seg_it].lon);
            if ((::ilat(isect_lat) & 0xffff0000) == (value & 0xffff0000))
              *nw_corner_inside = !*nw_corner_inside;
          }
        }
      }
    }
  
    if (se_corner_inside)
    {
      *se_corner_inside = sw_corner_inside;
    
      for (std::vector< unsigned int >::const_iterator seg_it = spi_it->second.begin();
          seg_it != spi_it->second.end(); ++seg_it)
      {
        uint32 lhs_ilat = ::ilat((*all_segments)[*seg_it].lat) & 0xffff0000;
        uint32 rhs_ilat = ::ilat((*all_segments)[*seg_it+1].lat) & 0xffff0000;
      
        if ((lhs_ilat < (value & 0xffff0000) && rhs_ilat == (value & 0xffff0000))
            || (lhs_ilat == (value & 0xffff0000) && rhs_ilat < (value & 0xffff0000)))
        {
          double lhs_lon = (*all_segments)[*seg_it].lon;
          lhs_lon -= lhs_lon > 0 ? 360. : 0.;
          double rhs_lon = (*all_segments)[*seg_it+1].lon;
          rhs_lon -= rhs_lon > 0 ? 360. : 0.;
          
          double isect_lon = (*all_segments)[*seg_it].lon
              + ((*all_segments)[*seg_it+1].lon - (*all_segments)[*seg_it].lon)
                  *(::lat(value & 0xffff0000) - (*all_segments)[*seg_it].lat)
                  /((*all_segments)[*seg_it+1].lat - (*all_segments)[*seg_it].lat);
          if (isect_lon >= -180. && (::ilon(isect_lon) & 0xffff0000) == (uint32(value)<<16))
            *se_corner_inside = !*se_corner_inside;
        }
      }
    }
  }
}


Area_Oracle::point_status RHR_Polygon_Area_Oracle::get_point_status(int32 value, double lat, double lon)
{
  if (value == 1)
    return 1;
  
  std::map< uint32, std::vector< unsigned int > >::const_iterator spi_it = segments_per_idx->find(value);
  if (spi_it == segments_per_idx->end())
    return 0;
  
  double border_lon = std::max(::lon(value<<16), -180.);
  double border_lat = ::lat(value & 0xffff0000);
  
  bool on_vertex = false;
  bool on_segment = false;
  bool is_inside = (inside_corners.find(value) != inside_corners.end());
  // is_inside is now true iff the sw corner is inside the area
  
  for (std::vector< unsigned int >::const_iterator seg_it = spi_it->second.begin();
      seg_it != spi_it->second.end(); ++seg_it)
  {
    double lhs_lat = (*all_segments)[*seg_it].lat;
    double rhs_lat = (*all_segments)[*seg_it+1].lat;
    
    double lhs_lon = (*all_segments)[*seg_it].lon;
    double rhs_lon = (*all_segments)[*seg_it+1].lon;
    if (lon < -179.9)
    {
      lhs_lon -= lhs_lon > 0 ? 360. : 0.;
      rhs_lon -= rhs_lon > 0 ? 360. : 0.;
    }
    else if (lon > 179.9)
    {
      lhs_lon += lhs_lon < 0 ? 360. : 0.;
      rhs_lon += rhs_lon < 0 ? 360. : 0.;
    }
    
    if (lhs_lat == rhs_lat)
    {
      if (lhs_lat < lat)
        is_inside ^= ((lhs_lon - lon)*(rhs_lon - lon) < 0);
      else if (lhs_lat == lat)
      {
        if (lon == lhs_lon || lon == rhs_lon)
          on_vertex = true;
        else
          on_segment |= ((lhs_lon - lon)*(rhs_lon - lon) < 0);
      }
      // no else required -- such a segment does not play a role
    }
    else if (lhs_lon == rhs_lon)
    {
      if ((lat == lhs_lat && lon == lhs_lon) || (lat == rhs_lat && lon == rhs_lon))
        on_vertex = true;
      else if (lon == lhs_lon && (lat - lhs_lat)*(lat - rhs_lat) < 0)
        on_segment = true;
      else if (lhs_lon < lon && (lhs_lat < border_lat || rhs_lat < border_lat))
        is_inside = !is_inside;
      // no else required -- such a segment does not play a role
    }
    else if ((lhs_lat - rhs_lat)*(lhs_lon - rhs_lon) < 0)
      // The segment runs from nw to se or se to nw
    {
      if ((lat == lhs_lat && lon == lhs_lon) || (lat == rhs_lat && lon == rhs_lon))
        on_vertex = true;
      else if ((lon - lhs_lon)*(lon - rhs_lon) < 0)
      {
        double isect_lat = lhs_lat + (rhs_lat - lhs_lat)*(lon - lhs_lon)/(rhs_lon - lhs_lon);
        if (isect_lat < lat)
          is_inside = !is_inside;
        else if (isect_lat == lat)
          on_segment = true;
      }
      else if (lon == lhs_lon && rhs_lon < lon)
      {
        if (lhs_lat < lat)
          is_inside = !is_inside;
      }
      else if (lon == rhs_lon && lhs_lon < lon)
      {
        if (rhs_lat < lat)
          is_inside = !is_inside;
      }
      else if ((border_lat - lhs_lat)*(border_lat - rhs_lat) < 0)
      {
        double isect_lon = lhs_lon + (rhs_lon - lhs_lon)*(border_lat - lhs_lat)/(rhs_lat - lhs_lat);
        if (border_lon < isect_lon && isect_lon < lon)
          is_inside = !is_inside;
      }
    }
    else
      // The segment runs from sw to ne or ne to sw
    {
      if ((border_lat - lhs_lat)*(border_lat - rhs_lat) < 0)
      {
        double isect_lon = lhs_lon + (rhs_lon - lhs_lon)*(border_lat - lhs_lat)/(rhs_lat - lhs_lat);
        if ((lhs_lon <= lon || rhs_lon <= lon) && (border_lon < isect_lon))
          is_inside = !is_inside;
      }
      if ((lat == lhs_lat && lon == lhs_lon) || (lat == rhs_lat && lon == rhs_lon))
      {
        on_vertex = true;
        continue;
      }
      
      if ((lon - lhs_lon)*(lon - rhs_lon) < 0)
      {
        double isect_lat = lhs_lat + (rhs_lat - lhs_lat)*(lon - lhs_lon)/(rhs_lon - lhs_lon);
        if (isect_lat < lat)
          is_inside = !is_inside;
        else if (isect_lat == lat)
          on_segment = true;
      }
      else if (lon == lhs_lon && rhs_lon < lon)
      {
        if (lhs_lat < lat)
          is_inside = !is_inside;
      }
      else if (lon == rhs_lon && lhs_lon < lon)
      {
        if (rhs_lat < lat)
          is_inside = !is_inside;
      }
    }
  }
  
  if (on_vertex)
    return 0x20 + 2*is_inside;
  if (on_segment)
    return 0x10 + 2*is_inside;
  
  return 2*is_inside;
}


RHR_Polygon_Geometry::RHR_Polygon_Geometry(const Free_Polygon_Geometry& rhs) : bounds(0)
{
  std::vector< std::vector< Point_Double > > input(*rhs.get_multiline_geometry());

  std::vector< Point_Double > all_segments;
  std::vector< unsigned int > gap_positions;
  
  gap_positions.push_back(0);
  for (std::vector< std::vector< Point_Double > >::const_iterator iti = input.begin(); iti != input.end(); ++iti)
  {
    all_segments.push_back((*iti)[0]);
    for (unsigned int i = 1; i < iti->size(); ++i)
      interpolate_segment((*iti)[i-1].lat, (*iti)[i-1].lon, (*iti)[i].lat, (*iti)[i].lon, all_segments);
    gap_positions.push_back(all_segments.size());
  }
  
  std::map< uint32, std::vector< unsigned int > > segments_per_idx;
  std::vector< unsigned int >::const_iterator gap_it = gap_positions.begin();
  for (unsigned int i = 0; i < all_segments.size(); ++i)
  {
    if (*gap_it == i)
      ++gap_it;
    else
      add_segment(segments_per_idx, all_segments[i-1], all_segments[i], i-1);
  }
  split_segments(all_segments, gap_positions, segments_per_idx);
  
  std::vector< Line_Divertion > divertions;
  for (unsigned int i = 0; i < gap_positions.size()-1; ++i)
  {
    divertions.push_back(Line_Divertion(gap_positions[i], gap_positions[i]-1));
    divertions.push_back(Line_Divertion(gap_positions[i]-1, gap_positions[i]));
  }
  for (std::map< uint32, std::vector< unsigned int > >::const_iterator idx_it = segments_per_idx.begin();
      idx_it != segments_per_idx.end(); ++idx_it)
    collect_divertions(all_segments, idx_it->first, idx_it->second, divertions);
  
  assemble_linestrings(all_segments, gap_positions.size(), divertions, linestrings);
  
  RHR_Polygon_Area_Oracle area_oracle(all_segments, segments_per_idx);
  Four_Field_Index four_field_idx(&area_oracle);
  
  for (std::map< uint32, std::vector< unsigned int > >::const_iterator idx_it = segments_per_idx.begin();
      idx_it != segments_per_idx.end(); ++idx_it)
    four_field_idx.add_point(::lat(idx_it->first | 0x8000), ::lon(idx_it->first<<16 | 0x8000), idx_it->first);
  
  four_field_idx.compute_inside_parts();
  
  for (std::vector< std::vector< Point_Double > >::iterator lstr_it = linestrings.begin();
      lstr_it != linestrings.end(); ++lstr_it)
  {
    if (lstr_it->size() > 2)
    {
      if ((*lstr_it)[0].lon < (*lstr_it)[1].lon)
      {
        if ((*lstr_it)[1].lon <= (*lstr_it)[2].lon)
        {
          if (four_field_idx.get_point_status((*lstr_it)[1].lat, (*lstr_it)[1].lon) & 0x3)
            std::reverse(lstr_it->begin(), lstr_it->end());
        }
        else
        {
          if (bool(four_field_idx.get_point_status((*lstr_it)[1].lat, (*lstr_it)[1].lon) & 0x3)
              ^ (((*lstr_it)[1].lat - (*lstr_it)[0].lat)/((*lstr_it)[1].lon -(*lstr_it)[0].lon)
                  < ((*lstr_it)[2].lat -(*lstr_it)[1].lat)/((*lstr_it)[2].lon -(*lstr_it)[1].lon)))
            std::reverse(lstr_it->begin(), lstr_it->end());
        }
      }
      else if ((*lstr_it)[2].lon < (*lstr_it)[1].lon)
      {
        if (!(four_field_idx.get_point_status((*lstr_it)[1].lat, (*lstr_it)[1].lon) & 0x3))
          std::reverse(lstr_it->begin(), lstr_it->end());
      }
      else if ((*lstr_it)[0].lon == (*lstr_it)[1].lon)
      {
        if (bool(four_field_idx.get_point_status((*lstr_it)[1].lat, (*lstr_it)[1].lon) & 0x3)
            ^ ((*lstr_it)[0].lat < (*lstr_it)[1].lat))
          std::reverse(lstr_it->begin(), lstr_it->end());
      }
      else if ((*lstr_it)[2].lon == (*lstr_it)[1].lon)
      {
        if (bool(four_field_idx.get_point_status((*lstr_it)[1].lat, (*lstr_it)[1].lon) & 0x3)
            ^ ((*lstr_it)[1].lat < (*lstr_it)[2].lat))
          std::reverse(lstr_it->begin(), lstr_it->end());
      }
      else
      {
        if (bool(four_field_idx.get_point_status((*lstr_it)[1].lat, (*lstr_it)[1].lon) & 0x3)
            ^ (((*lstr_it)[1].lat -(*lstr_it)[0].lat)/((*lstr_it)[1].lon -(*lstr_it)[0].lon)
                < ((*lstr_it)[2].lat -(*lstr_it)[1].lat)/((*lstr_it)[2].lon -(*lstr_it)[1].lon)))
          std::reverse(lstr_it->begin(), lstr_it->end());
      }
    }
  }
}


double RHR_Polygon_Geometry::center_lat() const
{
  if (!bounds)
    bounds = calc_bounds(linestrings);
  
  return bounds->center_lat();
}


double RHR_Polygon_Geometry::center_lon() const
{
  if (!bounds)
    bounds = calc_bounds(linestrings);
  
  return bounds->center_lon();
}


double RHR_Polygon_Geometry::south() const
{
  if (!bounds)
    bounds = calc_bounds(linestrings);
  
  return bounds->south;
}


double RHR_Polygon_Geometry::north() const
{
  if (!bounds)
    bounds = calc_bounds(linestrings);
  
  return bounds->north;
}


double RHR_Polygon_Geometry::west() const
{
  if (!bounds)
    bounds = calc_bounds(linestrings);
  
  return bounds->west;
}


double RHR_Polygon_Geometry::east() const
{
  if (!bounds)
    bounds = calc_bounds(linestrings);
  
  return bounds->east;
}


Opaque_Geometry* Compound_Geometry::clone() const
{
  std::vector< Opaque_Geometry* > cloned;
  for (std::vector< Opaque_Geometry* >::const_iterator it = components.begin(); it != components.end(); ++it)
    cloned.push_back((*it)->clone());
  return new Compound_Geometry(cloned);
}


Bbox_Double* calc_bounds(const std::vector< Opaque_Geometry* >& components)
{
  double south = 100.0;
  double west = 200.0;
  double north = -100.0;
  double east = -200.0;
  bool wrapped = false;
  
  for (std::vector< Opaque_Geometry* >::const_iterator it = components.begin(); it != components.end(); ++it)
  {
    if ((*it)->has_bbox())
    {
      south = std::min(south, (*it)->south());
      north = std::max(north, (*it)->north());
      west = std::min(west, (*it)->west());
      east = std::max(east, (*it)->east());
    
      wrapped |= ((*it)->east() < (*it)->west());
    }
  }
  
  if (north == -100.0)
    return new Bbox_Double(Bbox_Double::invalid);
  else if (wrapped || east - west > 180.0)
    // In this special case we should check whether the bounding box should rather cross the date line
  {
    double wrapped_west = 180.0;
    double wrapped_east = -180.0;
    
    for (std::vector< Opaque_Geometry* >::const_iterator it = components.begin(); it != components.end(); ++it)
    {
      if ((*it)->east() <= 0)
      {
	wrapped_east = std::max(wrapped_east, (*it)->west());
	if ((*it)->west() >= 0)
	  wrapped_west = std::min(wrapped_west, (*it)->west());
      }
      else if ((*it)->west() >= 0)
	wrapped_west = std::min(wrapped_west, (*it)->west());
      else
	// The components are too wildly distributed
	return new Bbox_Double(south, -180.0, north, 180.0);
    }
    
    if (wrapped_west - wrapped_east > 180.0)
      return new Bbox_Double(south, wrapped_west, north, wrapped_east);
    else
      // The components are too wildly distributed
      return new Bbox_Double(south, -180.0, north, 180.0);
  }
  else
    return new Bbox_Double(south, west, north, east);
}


bool Compound_Geometry::has_center() const
{
  if (!bounds)
    bounds = calc_bounds(components);
  
  return bounds->valid();
}


double Compound_Geometry::center_lat() const
{
  if (!bounds)
    bounds = calc_bounds(components);
  
  return bounds->center_lat();
}


double Compound_Geometry::center_lon() const
{
  if (!bounds)
    bounds = calc_bounds(components);
  
  return bounds->center_lon();
}


bool Compound_Geometry::has_bbox() const
{
  if (!bounds)
    bounds = calc_bounds(components);
  
  return bounds->valid();
}


double Compound_Geometry::south() const
{
  if (!bounds)
    bounds = calc_bounds(components);
  
  return bounds->south;
}


double Compound_Geometry::north() const
{
  if (!bounds)
    bounds = calc_bounds(components);
  
  return bounds->north;
}


double Compound_Geometry::west() const
{
  if (!bounds)
    bounds = calc_bounds(components);
  
  return bounds->west;
}


double Compound_Geometry::east() const
{
  if (!bounds)
    bounds = calc_bounds(components);
  
  return bounds->east;
}


bool Compound_Geometry::relation_pos_is_valid(unsigned int member_pos) const
{
  return (member_pos < components.size() && components[member_pos]
      && components[member_pos]->has_center());
}


double Compound_Geometry::relation_pos_lat(unsigned int member_pos) const
{
  if (member_pos < components.size() && components[member_pos])
    return components[member_pos]->center_lat();
  return 0;
}


double Compound_Geometry::relation_pos_lon(unsigned int member_pos) const
{
  if (member_pos < components.size() && components[member_pos])
    return components[member_pos]->center_lon();
  return 0;
}


unsigned int Compound_Geometry::relation_way_size(unsigned int member_pos) const
{
  if (member_pos < components.size() && components[member_pos])
    return components[member_pos]->way_size();
  return 0;
}


bool Compound_Geometry::relation_pos_is_valid(unsigned int member_pos, unsigned int nd_pos) const
{
  return (member_pos < components.size() && components[member_pos]
      && components[member_pos]->way_pos_is_valid(nd_pos));
}


double Compound_Geometry::relation_pos_lat(unsigned int member_pos, unsigned int nd_pos) const
{
  if (member_pos < components.size() && components[member_pos])
    return components[member_pos]->way_pos_lat(nd_pos);
  return 0;
}


double Compound_Geometry::relation_pos_lon(unsigned int member_pos, unsigned int nd_pos) const
{
  if (member_pos < components.size() && components[member_pos])
    return components[member_pos]->way_pos_lon(nd_pos);
  return 0;
}


void Compound_Geometry::add_component(Opaque_Geometry* component)
{ 
  delete bounds;
  bounds = 0;
  components.push_back(component);
}


bool Partial_Relation_Geometry::has_center() const
{
  if (!has_coords)
    return 0;
    
  if (!bounds)
    bounds = calc_bounds(components);
  
  return bounds->valid();
}


double Partial_Relation_Geometry::center_lat() const
{
  if (!has_coords)
    return 0;
    
  if (!bounds)
    bounds = calc_bounds(components);
  
  return bounds->center_lat();
}


double Partial_Relation_Geometry::center_lon() const
{
  if (!has_coords)
    return 0;
    
  if (!bounds)
    bounds = calc_bounds(components);
  
  return bounds->center_lon();
}


bool Partial_Relation_Geometry::has_bbox() const
{
  if (!has_coords)
    return false;
    
  if (!bounds)
    bounds = calc_bounds(components);
  
  return bounds->valid();
}


double Partial_Relation_Geometry::south() const
{
  if (!has_coords)
    return 0;
    
  if (!bounds)
    bounds = calc_bounds(components);
  
  return bounds->south;
}


double Partial_Relation_Geometry::north() const
{
  if (!has_coords)
    return 0;
    
  if (!bounds)
    bounds = calc_bounds(components);
  
  return bounds->north;
}


double Partial_Relation_Geometry::west() const
{
  if (!has_coords)
    return 0;
    
  if (!bounds)
    bounds = calc_bounds(components);
  
  return bounds->west;
}


double Partial_Relation_Geometry::east() const
{
  if (!has_coords)
    return 0;
    
  if (!bounds)
    bounds = calc_bounds(components);
  
  return bounds->east;
}


bool Partial_Relation_Geometry::relation_pos_is_valid(unsigned int member_pos) const
{
  return (member_pos < components.size() && components[member_pos]
      && components[member_pos]->has_center());
}


double Partial_Relation_Geometry::relation_pos_lat(unsigned int member_pos) const
{
  if (member_pos < components.size() && components[member_pos])
    return components[member_pos]->center_lat();
  return 0;
}


double Partial_Relation_Geometry::relation_pos_lon(unsigned int member_pos) const
{
  if (member_pos < components.size() && components[member_pos])
    return components[member_pos]->center_lon();
  return 0;
}


unsigned int Partial_Relation_Geometry::relation_way_size(unsigned int member_pos) const
{
  if (member_pos < components.size() && components[member_pos])
    return components[member_pos]->way_size();
  return 0;
}


bool Partial_Relation_Geometry::relation_pos_is_valid(unsigned int member_pos, unsigned int nd_pos) const
{
  return (member_pos < components.size() && components[member_pos]
      && components[member_pos]->way_pos_is_valid(nd_pos));
}


double Partial_Relation_Geometry::relation_pos_lat(unsigned int member_pos, unsigned int nd_pos) const
{
  if (member_pos < components.size() && components[member_pos])
    return components[member_pos]->way_pos_lat(nd_pos);
  return 0;
}


double Partial_Relation_Geometry::relation_pos_lon(unsigned int member_pos, unsigned int nd_pos) const
{
  if (member_pos < components.size() && components[member_pos])
    return components[member_pos]->way_pos_lon(nd_pos);
  return 0;
}


void Partial_Relation_Geometry::add_placeholder()
{ 
  components.push_back(new Null_Geometry());
}


void Partial_Relation_Geometry::add_point(const Point_Double& point)
{ 
  delete bounds;
  bounds = 0;
  if (point.lat < 100.)
  {
    has_coords = true;
    components.push_back(new Point_Geometry(point.lat, point.lon));
  }
  else
    components.push_back(new Null_Geometry());
}


void Partial_Relation_Geometry::start_way()
{ 
  components.push_back(new Partial_Way_Geometry());
}


void Partial_Relation_Geometry::add_way_point(const Point_Double& point)
{ 
  delete bounds;
  bounds = 0;
  
  Partial_Way_Geometry* geom = dynamic_cast< Partial_Way_Geometry* >(components.back());
  if (geom)
  {
    has_coords = true;
    geom->add_point(point);
  }
}


void Partial_Relation_Geometry::add_way_placeholder()
{ 
  Partial_Way_Geometry* geom = dynamic_cast< Partial_Way_Geometry* >(components.back());
  if (geom)
    geom->add_point(Point_Double(100., 200.));
}


double great_circle_dist(double lat1, double lon1, double lat2, double lon2)
{
  if (lat1 == lat2 && lon1 == lon2)
    return 0;
  static const double deg_to_arc = acos(0)/90.;
  double scalar_prod = cos((lat2-lat1)*deg_to_arc)
      + cos(lat1*deg_to_arc)*cos(lat2*deg_to_arc)*(cos((lon2-lon1)*deg_to_arc) - 1);
  if (scalar_prod > 1)
    scalar_prod = 1;
  static const double arc_to_meter = 10*1000*1000/acos(0);
  return acos(scalar_prod)*arc_to_meter;
}


double length(const Opaque_Geometry& geometry)
{
  double result = 0;
  
  if (geometry.has_components())
  {
    const std::vector< Opaque_Geometry* >* components = geometry.get_components();
    for (std::vector< Opaque_Geometry* >::const_iterator it = components->begin(); it != components->end(); ++it)
      result += (*it ? length(**it) : 0);
  }
  else if (geometry.has_line_geometry())
  {
    const std::vector< Point_Double >* line_geometry = geometry.get_line_geometry();
    for (unsigned int i = 1; i < line_geometry->size(); ++i)
      result += great_circle_dist((*line_geometry)[i-1].lat, (*line_geometry)[i-1].lon,
          (*line_geometry)[i].lat, (*line_geometry)[i].lon);
  }
  else if (geometry.has_faithful_way_geometry())
  {
    for (unsigned int i = 1; i < geometry.way_size(); ++i)
      result += (geometry.way_pos_is_valid(i-1) && geometry.way_pos_is_valid(i) ?
          great_circle_dist(geometry.way_pos_lat(i-1), geometry.way_pos_lon(i-1),
              geometry.way_pos_lat(i), geometry.way_pos_lon(i)) : 0);
  }
  
  return result;
}
