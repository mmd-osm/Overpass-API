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

#ifndef DE__OSM3S___OVERPASS_API__DATA__UTILS_H
#define DE__OSM3S___OVERPASS_API__DATA__UTILS_H


#include "../core/datatypes.h"

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <iomanip>
#include <map>
#include <sstream>
#include <string>
#include <type_traits> // For std::decay
#include <vector>

#include <fmt/core.h>

template< typename Index, typename Skeleton >
unsigned int count(const std::map< Index, std::vector< Skeleton > >& elems)
{
  uint result = 0;
  for (typename std::map< Index, std::vector< Skeleton > >::const_iterator it = elems.begin();
       it != elems.end(); ++it)
    result += it->second.size();
  return result;
}


template < typename T >
inline std::string to_string(T t)
{
  return std::to_string(t);
}

template < >
inline std::string to_string(std::string t)
{
  return t;
}

template < >
inline std::string to_string(double t)
{
  /*
  std::ostringstream out;
  out<<std::setprecision(14)<<t;
  return out.str();
  */
  return fmt::format("{:.14f}", t);

}


template < typename T >
std::string fixed_to_string_3(T t)
{
/*
  std::ostringstream out;
  out<<std::fixed<<std::setprecision(precision)<<t;
  return out.str();
*/
  return fmt::format("{:.3f}", t);
}

template < typename T >
std::string fixed_to_string_7(T t)
{
  return fmt::format("{:.7f}", t);
}



inline bool try_double(const std::string& input, double& result)
{
  if (input.empty())
    return false;

  const char* input_c = input.c_str();
  char* end_c = 0;
  errno = 0;
  result = strtod(input_c, &end_c);
  return input_c + input.size() == end_c;
}


inline bool try_starts_with_double(const std::string& input, double& result)
{
  if (input.empty())
    return false;

  const char* input_c = input.c_str();
  char* end_c = 0;
  errno = 0;
  result = strtod(input_c, &end_c);
  return !errno && input_c != end_c;
}


inline std::string double_suffix(const std::string& input)
{
  if (input.empty())
    return "";

  const char* input_c = input.c_str();
  char* end_c = 0;
  errno = 0;
  strtod(input_c, &end_c);

  if (!errno && input_c != end_c)
  {
    while (*end_c && isspace(*end_c))
      ++end_c;
    return end_c;
  }

  return "";
}


inline bool try_int64(const std::string& input, int64& result)
{
  if (input.empty())
    return false;

  const char* input_c = input.c_str();
  char* end_c = 0;
  errno = 0;
  result = strtoll(input_c, &end_c, 0);
  return input_c + input.size() == end_c;
}


inline bool string_represents_boolean_true(const std::string& val)
{
  if (val.size() == 1) {
    if (val[0] == '0')
      return false;
    if (val[0] == '1')
      return true;
  }

  double val_d = 0;
  if (try_double(val, val_d))
    return val_d != 0;
  return !val.empty();
}


template< typename Index, typename Object >
void sort_second(std::map< Index, std::vector< Object > >& items)
{
  for (typename std::map< Index, std::vector< Object > >::iterator it = items.begin(); it != items.end(); ++it)
    std::sort(it->second.begin(), it->second.end());
}


inline void sort(Set& set)
{
  sort_second(set.nodes);
  sort_second(set.attic_nodes);
  sort_second(set.ways);
  sort_second(set.attic_ways);
  sort_second(set.relations);
  sort_second(set.attic_relations);
  sort_second(set.areas);
  sort_second(set.deriveds);
  sort_second(set.area_blocks);
}



// https://vittorioromeo.info/index/blog/passing_functions_to_functions.html#fn_view_impl
// https://gist.github.com/twoscomplement/030818a6c38c5a983482dc3a385a3ab8
// https://godbolt.org/z/6LG-W7
// https://godbolt.org/z/-Xzk4N

template<typename>
struct TransientFunction; // intentionally not defined

template<typename R, typename ...Args>
struct TransientFunction<R(Args...)>
{
  using Dispatcher = R(*)(void*, Args...);

  Dispatcher m_Dispatcher; // A pointer to the static function that will call the
                           // wrapped invokable object
  void* m_Target;          // A pointer to the invokable object

  // Dispatch() is instantiated by the TransientFunction constructor,
  // which will store a pointer to the function in m_Dispatcher.
  template<typename S>
  static R Dispatch(void* target, Args... args)
  {
    return (*(S*)target)(args...);
  }

  template<typename T>
  TransientFunction(T&& target)
    : m_Dispatcher(&Dispatch<typename std::decay<T>::type>)
    , m_Target(&target)
  {
  }

  // Specialize for reference-to-function, to ensure that a valid pointer is
  // stored.
  using TargetFunctionRef = R(Args...);
  TransientFunction(TargetFunctionRef target)
    : m_Dispatcher(Dispatch<TargetFunctionRef>)
  {
    static_assert(sizeof(void*) == sizeof target,
    "It will not be possible to pass functions by reference on this platform. "
    "Please use explicit function pointers i.e. foo(target) -> foo(&target)");
    m_Target = (void*)target;
  }

  R operator()(Args... args) const
  {
    return m_Dispatcher(m_Target, args...);
  }
};


#endif
