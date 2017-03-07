/** Copyright 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016 Roland Olbricht et al.
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

#ifndef DE__OSM3S___OVERPASS_API__DATA__REGULAR_EXPRESSION_H
#define DE__OSM3S___OVERPASS_API__DATA__REGULAR_EXPRESSION_H

#include "sys/types.h"
#include "locale.h"
#include "regex.h"

#include <iostream>
#include <string>
#include <unicode/regex.h>


struct Regular_Expression_Error
{
  public:
    Regular_Expression_Error(int errno_) : error_no(errno_) {}
    int error_no;
};


class Regular_Expression
{
  public:

  enum Strategy { call_library, match_anything, match_nonempty };

    Regular_Expression() {}

    Regular_Expression(const std::string& regex, bool case_sensitive) {

      if (regex == ".*")
        strategy = match_anything;
      else if (regex == ".")
        strategy = match_nonempty;
      else
        strategy = call_library;

      is_cache_available = false;
      prev_line = "";
      prev_result = false;
    }

    virtual ~Regular_Expression() { };

    virtual bool matches(const std::string& line) const = 0;

  protected:
    mutable bool is_cache_available;
    mutable std::string prev_line;
    mutable bool prev_result;
    Strategy strategy;
};


class Regular_Expression_POSIX : public Regular_Expression
{
  public:

    
    Regular_Expression_POSIX(const std::string& regex, bool case_sensitive) :
        Regular_Expression(regex, case_sensitive)
    {
      if (strategy == call_library)
      {
        setlocale(LC_ALL, "C.UTF-8");
        int case_flag = case_sensitive ? 0 : REG_ICASE;
        int error_no = regcomp(&preg, regex.c_str(), REG_EXTENDED|REG_NOSUB|case_flag);
        if (error_no != 0)
          throw Regular_Expression_Error(error_no);
      }
    }
    
    ~Regular_Expression_POSIX()
    {
      if (strategy == call_library)
        regfree(&preg);
    }
    
    inline bool matches(const std::string& line) const
    {
      if (strategy == match_anything)
        return true;
      else if (strategy == match_nonempty)
        return !line.empty();

      if (is_cache_available && line == prev_line)
        return prev_result;

      bool result = (regexec(&preg, line.c_str(), 0, 0, 0) == 0);

      is_cache_available = true;
      prev_result = result;
      prev_line = line;

      return (result);
    }
    
  private:
    regex_t preg;
};


// Link: -licuuc -licui18n

class Regular_Expression_ICU : public Regular_Expression
{
  public:


    Regular_Expression_ICU(const std::string& regex, bool case_sensitive) :
        Regular_Expression(regex, case_sensitive), matcher(0)
    {
      if (strategy == call_library)
      {
        setlocale(LC_ALL, "C.UTF-8");

        UErrorCode status  = U_ZERO_ERROR;

        uint32_t flags = 0;
        flags |= case_sensitive ? 0 : UREGEX_CASE_INSENSITIVE;

        UnicodeString regexp_U = UnicodeString(regex.c_str());

        matcher = new RegexMatcher(regexp_U, flags, status);
        if (U_FAILURE(status)) {
          throw Regular_Expression_Error(status);
        }

        status = U_ZERO_ERROR;
        matcher->setTimeLimit(1000, status);
        if (U_FAILURE(status)) {
          throw Regular_Expression_Error(status);
        }
      }
    }

    ~Regular_Expression_ICU()
    {
      if (strategy == call_library)
        delete matcher;
    }

    inline bool matches(const std::string& line) const
    {
      if (strategy == match_anything)
        return true;
      else if (strategy == match_nonempty)
        return !line.empty();

      if (is_cache_available && line == prev_line)
        return prev_result;

      UnicodeString stringToTest_U = UnicodeString(line.c_str());

      matcher->reset(stringToTest_U);

      UErrorCode status  = U_ZERO_ERROR;

      bool result = (matcher->find(0, status));

      if (U_FAILURE(status)) {
        throw Regular_Expression_Error(status);
      }

      is_cache_available = true;
      prev_result = result;
      prev_line = line;

      return (result);
    }

  private:
    RegexMatcher *matcher;
};

class Regular_Expression_Factory
{

public:

  static Regular_Expression* get_regexp_engine(const std::string engine, const std::string& regex, bool case_sensitive )
  {
    if (engine == "ICU")
      return new Regular_Expression_ICU(regex, case_sensitive);

    // always fall back to POSIX
    return new Regular_Expression_POSIX(regex, case_sensitive);
  }
};


#endif
