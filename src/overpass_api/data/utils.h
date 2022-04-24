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
#include <variant>
#include <vector>

#include <fmt/core.h>

// Simple wrapper struct to indicate that double value should be formatted with 3 decimal digits
// currently only used by Angle_Eval_Task::calc_angle
struct Fixed_Point_3 {

  Fixed_Point_3(double value) : value(value) {}

  double value{};
};

struct Fixed_Point_7 {

  Fixed_Point_7(double value) : value(value) {}

  double value{};
};


template < typename T >
std::string fixed_to_string(T t, unsigned int precision)
{
  return fmt::format("{:.{}f}", t, precision);
}

template < typename T >
inline std::string to_string(T t)
{
  return std::to_string(t);
}

template < >
inline std::string to_string(std::string t) = delete;


template < >
inline std::string to_string(double t)
{
  return fmt::format("{:.14g}", t);
}

template < >
inline std::string to_string(Fixed_Point_3 v)
{
  return fixed_to_string(v.value, 3);
}

template < >
inline std::string to_string(Fixed_Point_7 v)
{
  return fixed_to_string(v.value, 7);
}


// Note: careful with std::string and bool when using Eval_Variant
// Always define string literals with ""s suffix, otherwise string will be implicitly converted to bool
// https://stackoverflow.com/questions/44086269/why-does-my-variant-convert-a-stdstring-to-a-bool
// Fixed in C++20

using Eval_Variant = std::variant<int64, double, bool, std::string, Fixed_Point_3, Fixed_Point_7>;

inline std::string eval_variant_to_string(Eval_Variant&& v) {

  switch (v.index()) {

    case 0:
      return to_string(std::get<0>(v));

    case 1:
      return to_string(std::get<1>(v));

    case 2:
      return to_string(std::get<2>(v));

    case 3:
      return std::get<3>(v);

    case 4:
      return to_string(std::get<4>(v));

    case 5:
      return to_string(std::get<5>(v));
  }

  return "";
}

inline std::string eval_variant_to_string(const Eval_Variant& v) {

  switch (v.index()) {

    case 0:
      return to_string(std::get<0>(v));

    case 1:
      return to_string(std::get<1>(v));

    case 2:
      return to_string(std::get<2>(v));

    case 3:
      return std::get<3>(v);

    case 4:
      return to_string(std::get<4>(v));

    case 5:
      return to_string(std::get<5>(v));
  }

  return "";
}

template< typename Index, typename Skeleton >
unsigned int count(const std::map< Index, std::vector< Skeleton > >& elems)
{
  uint result = 0;
  for (auto it = elems.begin();
       it != elems.end(); ++it)
    result += it->second.size();
  return result;
}



inline bool try_double(const std::string& input, double& result)
{
  if (input.empty())
    return false;

  const char* input_c = input.c_str();
  char* end_c = nullptr;
  errno = 0;
  result = strtod(input_c, &end_c);
  return input_c + input.size() == end_c;
}


inline bool try_double(const Eval_Variant& v, double& result)
{
  switch(v.index()) {

  case 0:
    result = std::get<int64>(v);
    return true;

  case 1:
    result = std::get<double>(v);
    return true;

  case 2:
    result = std::get<bool>(v);
    return true;

  case 3:
    return try_double(std::get<std::string>(v), result);

  case 4:
    result = std::get<Fixed_Point_3>(v).value;
    return true;

  case 5:
    result = std::get<Fixed_Point_7>(v).value;
    return true;
  }

  return false;
}


inline bool try_starts_with_double(const std::string& input, double& result)
{
  if (input.empty())
    return false;

  const char* input_c = input.c_str();
  char* end_c = nullptr;
  errno = 0;
  result = strtod(input_c, &end_c);
  return !errno && input_c != end_c;
}

inline bool try_starts_with_double(const Eval_Variant& v, double& result)
{
  switch(v.index()) {

  case 0:
    result = std::get<int64>(v);
    return true;

  case 1:
    result = std::get<double>(v);
    return true;

  case 2:
    result = std::get<bool>(v);
    return true;

  case 3:
    return try_starts_with_double(std::get<std::string>(v), result);

  case 4:
    result = std::get<Fixed_Point_3>(v).value;
    return true;

  case 5:
    result = std::get<Fixed_Point_7>(v).value;
    return true;
  }

  return false;
}


inline std::string double_suffix(const std::string& input)
{
  if (input.empty())
    return "";

  const char* input_c = input.c_str();
  char* end_c = nullptr;
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

inline std::string double_suffix(const Eval_Variant& v)
{
  switch(v.index()) {

  case 0:
    return "";

  case 1:
    return "";

  case 2:
    return "";

  case 3:
    return double_suffix(std::get<std::string>(v));

  case 4:
    return "";

  case 5:
    return "";
  }

  return "";
}



inline bool try_int64(const std::string& input, int64& result)
{
  if (input.empty())
    return false;

  const char* input_c = input.c_str();
  char* end_c = nullptr;
  errno = 0;
  result = strtoll(input_c, &end_c, 0);
  return input_c + input.size() == end_c;
}

inline bool try_int64(const Eval_Variant& v, int64& result)
{
  switch(v.index()) {

  case 0:
    result = std::get<int64>(v);
    return true;

  case 1:
    return false;

  case 2:
    result = std::get<bool>(v);
    return true;

  case 3:
    return try_int64(std::get<std::string>(v), result);

  case 4:
    return false;

  case 5:
    return false;
  }

  return false;
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

inline bool eval_variant_represents_boolean_true(const Eval_Variant& v)
{
  switch(v.index()) {
  case 0:  //int64
    return std::get<int64>(v) == 1;

  case 1:  // double
    return std::get<double>(v) == 1;

  case 2:  // bool
    return std::get<bool>(v);

  case 3: // String
    return string_represents_boolean_true(std::get<std::string>(v));

  case 4:
    return std::get<Fixed_Point_3>(v).value == 1;

  case 5:
    return std::get<Fixed_Point_7>(v).value == 1;

  }

  return false;
}


template< typename Index, typename Object >
void sort_second(std::map< Index, std::vector< Object > >& items)
{
  for (auto it = items.begin(); it != items.end(); ++it)
    if (!std::is_sorted(it->second.begin(), it->second.end()))
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
