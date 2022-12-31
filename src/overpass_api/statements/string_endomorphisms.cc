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

#include "../data/tag_store.h"
#include "../data/utils.h"
#include "string_endomorphisms.h"

using namespace std::string_literals;


String_Endom_Statement_Maker< Evaluator_Number > Evaluator_Number::statement_maker;
String_Endom_Evaluator_Maker< Evaluator_Number > Evaluator_Number::evaluator_maker;


Eval_Variant Evaluator_Number::process(const Eval_Variant& rhs_s) const
{
  int64 rhs_l = 0;
  if (try_int64(rhs_s, rhs_l))
    return (rhs_l);

  double rhs_d = 0;
  if (try_starts_with_double(rhs_s, rhs_d))
    return (rhs_d);

  return "NaN"s;
}


//-----------------------------------------------------------------------------


String_Endom_Statement_Maker< Evaluator_Is_Num > Evaluator_Is_Num::statement_maker;
String_Endom_Evaluator_Maker< Evaluator_Is_Num > Evaluator_Is_Num::evaluator_maker;


Eval_Variant Evaluator_Is_Num::process(const Eval_Variant& rhs_s) const
{
  int64 rhs_l = 0;
  if (try_int64(rhs_s, rhs_l))
    return true;

  double rhs_d = 0;
  if (try_starts_with_double(rhs_s, rhs_d))
    return true;

  return false;
}


//-----------------------------------------------------------------------------


String_Endom_Statement_Maker< Evaluator_Suffix > Evaluator_Suffix::statement_maker;
String_Endom_Evaluator_Maker< Evaluator_Suffix > Evaluator_Suffix::evaluator_maker;


Eval_Variant Evaluator_Suffix::process(const Eval_Variant& rhs_s) const
{
  return double_suffix(rhs_s);
}


//-----------------------------------------------------------------------------


String_Endom_Statement_Maker< Evaluator_Abs > Evaluator_Abs::statement_maker;
String_Endom_Evaluator_Maker< Evaluator_Abs > Evaluator_Abs::evaluator_maker;


Eval_Variant Evaluator_Abs::process(const Eval_Variant& rhs_s) const
{
  int64 rhs_l = 0;
  if (try_int64(rhs_s, rhs_l))
    return (std::abs(rhs_l));

  double rhs_d = 0;
  if (try_starts_with_double(rhs_s, rhs_d))
    return (std::abs(rhs_d));

  return "NaN"s;
}


//-----------------------------------------------------------------------------


String_Endom_Statement_Maker< Evaluator_Sin > Evaluator_Sin::statement_maker;
String_Endom_Evaluator_Maker< Evaluator_Sin > Evaluator_Sin::evaluator_maker;


Eval_Variant Evaluator_Sin::process(const Eval_Variant& rhs_s) const
{
  int64 rhs_l = 0;
  if (try_int64(rhs_s, rhs_l))
    return (sin(rhs_l));

  double rhs_d = 0;
  if (try_starts_with_double(rhs_s, rhs_d))
    return (sin(rhs_d));

  return "NaN"s;
}

//-----------------------------------------------------------------------------

struct DateTime
{
  DateTime(std::string v) {

    //First run: try for year, month, day, hour, minute, second
    skip_non_digits(v);
    year = get_next_value(v);
    skip_non_digits(v);
    month = get_next_value(v);
    skip_non_digits(v);
    day = get_next_value(v);
    skip_non_digits(v);
    hour = get_next_value(v);
    skip_non_digits(v);
    minute = get_next_value(v);
    skip_non_digits(v);
    second = get_next_value(v);
  }

  bool is_valid() const noexcept {
    return (!(year < 1000 || month > 12 || day > 31 || hour > 23 || minute > 59 || second > 60));
  }

  unsigned int year = 0;
  unsigned int month = 0;
  unsigned int day = 0;
  unsigned int hour = 0;
  unsigned int minute = 0;
  unsigned int second = 0;

  private:
    std::string::size_type pos = 0;

    unsigned int get_next_value(const std::string& v) noexcept
    {
      unsigned int res = 0;
      while (pos < v.size() && isdigit(v[pos]))
      {
        res = 10*res + (v[pos] - '0');
        ++pos;
      }
      return res;
    }

    void skip_non_digits(const std::string& v) noexcept
    {
      while (pos < v.size() && !isdigit(v[pos]))
        ++pos;
    }
};

//-----------------------------------------------------------------------------

String_Endom_Statement_Maker< Evaluator_Date > Evaluator_Date::statement_maker;
String_Endom_Evaluator_Maker< Evaluator_Date > Evaluator_Date::evaluator_maker;


Eval_Variant Evaluator_Date::process(const Eval_Variant& v) const
{
  const DateTime dt(eval_variant_to_string(v));

  if (!dt.is_valid())
    return "NaD"s;

  return to_string(dt.year + dt.month/16. + dt.day/(16.*32)
      + dt.hour/(16.*32*32) + dt.minute/(16.*32*32*64) + dt.second/(16.*32*32*64*64));
}


//-----------------------------------------------------------------------------


String_Endom_Statement_Maker< Evaluator_Is_Date > Evaluator_Is_Date::statement_maker;
String_Endom_Evaluator_Maker< Evaluator_Is_Date > Evaluator_Is_Date::evaluator_maker;


Eval_Variant Evaluator_Is_Date::process(const Eval_Variant& v) const
{
  const DateTime dt(eval_variant_to_string(v));

  return (dt.is_valid());
}
