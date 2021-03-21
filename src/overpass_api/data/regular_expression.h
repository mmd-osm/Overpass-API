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

#ifndef DE__OSM3S___OVERPASS_API__DATA__REGULAR_EXPRESSION_H
#define DE__OSM3S___OVERPASS_API__DATA__REGULAR_EXPRESSION_H

#ifdef HAVE_CONFIG_H
#include <config.h>
#undef VERSION
#endif

#include "sys/types.h"
#include "locale.h"
#include "regex.h"

#include <iostream>
#include <string>

#ifdef HAVE_ICU
#include <unicode/regex.h>

using icu::UnicodeString;
using icu::RegexMatcher;
#endif

#ifdef HAVE_PCRE
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>
#endif



struct Regular_Expression_Error : public std::runtime_error
{
  public:
    Regular_Expression_Error(const std::string& arg) : std::runtime_error("Regular expression error: " + arg) { }

};



class Regular_Expression
{
  public:

  enum class Strategy { call_library, match_anything, match_nonempty };

    Regular_Expression() = delete;

    Regular_Expression(const std::string& regex, bool case_sensitive) {


      if (regex == ".*")
        strategy = Strategy::match_anything;
      else if (regex == ".")
        strategy = Strategy::match_nonempty;
      else
        strategy = Strategy::call_library;

      is_cache_available = false;
      prev_line = "";
      prev_result = false;
    }

    virtual ~Regular_Expression() { };

    virtual bool matches(const std::string& line) const = 0;

  private:
    Regular_Expression(const Regular_Expression&);
    const Regular_Expression& operator=(const Regular_Expression&);

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
      if (strategy == Strategy::call_library)
      {
        setlocale(LC_ALL, "C.UTF-8");
        int case_flag = case_sensitive ? 0 : REG_ICASE;
        int error_no = regcomp(&preg, regex.c_str(), REG_EXTENDED|REG_NOSUB|case_flag);
        if (error_no != 0) {
          const int MAX_ERROR_MSG = 0x1000;
          char error_message[MAX_ERROR_MSG];
          size_t size = regerror(error_no, &preg, error_message, MAX_ERROR_MSG);
          throw Regular_Expression_Error(std::string(error_message, size));
        }
      }
    }
    
    ~Regular_Expression_POSIX()
    {
      if (strategy == Strategy::call_library)
        regfree(&preg);
    }

    inline bool matches(const std::string& line) const
    {
      if (strategy == Strategy::match_anything)
        return true;
      else if (strategy == Strategy::match_nonempty)
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




#ifdef HAVE_ICU

class Regular_Expression_ICU : public Regular_Expression
{
  public:

    Regular_Expression_ICU(const std::string& regex, bool case_sensitive) :
        Regular_Expression(regex, case_sensitive), matcher(0)
    {

      if (strategy == Strategy::call_library)
      {
        setlocale(LC_ALL, "C.UTF-8");

        UErrorCode status  = U_ZERO_ERROR;

        uint32_t flags = 0;
        flags |= case_sensitive ? 0 : UREGEX_CASE_INSENSITIVE;

        UnicodeString regexp_U = UnicodeString(regex.c_str());

        matcher = new RegexMatcher(regexp_U, flags, status);
        if (U_FAILURE(status)) {
          throw Regular_Expression_Error(u_errorName(status));
        }

        status = U_ZERO_ERROR;
        matcher->setTimeLimit(1000, status);
        if (U_FAILURE(status)) {
          throw Regular_Expression_Error(u_errorName(status));
        }
      }
    }

    ~Regular_Expression_ICU()
    {
      if (strategy == Strategy::call_library)
        delete matcher;
    }

    inline bool matches(const std::string& line) const
    {
      if (strategy == Strategy::match_anything)
        return true;
      else if (strategy == Strategy::match_nonempty)
        return !line.empty();

      if (is_cache_available && line == prev_line)
        return prev_result;

      UnicodeString stringToTest_U = UnicodeString(line.c_str());

      matcher->reset(stringToTest_U);

      UErrorCode status  = U_ZERO_ERROR;

      bool result = (matcher->find(0, status));

      if (U_FAILURE(status)) {
        throw Regular_Expression_Error(u_errorName(status));
      }

      is_cache_available = true;
      prev_result = result;
      prev_line = line;

      return (result);
    }

  private:
    RegexMatcher *matcher;
};

#endif

#ifdef HAVE_PCRE

class Regular_Expression_PCRE : public Regular_Expression
{
  public:

    Regular_Expression_PCRE(const std::string& regex, bool case_sensitive, bool enable_jit) :
        Regular_Expression(regex, case_sensitive), re(nullptr), match_data(nullptr),
        mcontext(nullptr), jit_stack(nullptr), pcre2_jit_on(false)
    {

      if (strategy == Strategy::call_library)
      {
        setlocale(LC_ALL, "C.UTF-8");

        int errornumber;
        PCRE2_SIZE erroroffset;

        uint32_t flags = PCRE2_UTF;
        flags |= case_sensitive ? 0 : PCRE2_CASELESS;

        re = pcre2_compile(
          reinterpret_cast<PCRE2_SPTR>(regex.data()),          /* the pattern */
          regex.size(),                                        /* pattern length */
          flags,                                               /* options */
          &errornumber,                                        /* for error number */
          &erroroffset,                                        /* for error offset */
          NULL);                                               /* use default compile context */

        if (re == nullptr)
          {
          PCRE2_UCHAR buffer[256];
          size_t size = pcre2_get_error_message(errornumber, buffer, sizeof(buffer));
          throw Regular_Expression_Error(std::string(reinterpret_cast<const char*>(buffer), size));
        }

        if (enable_jit) {
          pcre2_config(PCRE2_CONFIG_JIT, &pcre2_jit_on);
        }

        if (pcre2_jit_on) {
          int rc = pcre2_jit_compile(re, PCRE2_JIT_COMPLETE);
          if (rc != 0) {
            throw Regular_Expression_Error("jit_compile issue");
          }

          jit_stack = pcre2_jit_stack_create(32*1024, 512*1024, NULL);
          if (jit_stack == nullptr) {
            throw Regular_Expression_Error("jit_stack issue");
          }

          mcontext = pcre2_match_context_create(NULL);
          if (mcontext == nullptr) {
            throw Regular_Expression_Error("mcontext issue");
          }

          pcre2_jit_stack_assign(mcontext, NULL, jit_stack);
        }

        match_data = pcre2_match_data_create_from_pattern(re, NULL);
        if (match_data == nullptr) {
          throw Regular_Expression_Error("pcre2_match_data issue");
        }
      }
    }

    ~Regular_Expression_PCRE()
    {
      if (strategy == Strategy::call_library)

        if (re != nullptr) {
          pcre2_code_free(re);
        }

        if (match_data != nullptr) {
          pcre2_match_data_free(match_data);
        }

        if (jit_stack != nullptr) {
          pcre2_jit_stack_free(jit_stack);
        }

        if (mcontext != nullptr) {
           pcre2_match_context_free(mcontext);
        }
    }

    inline bool matches(const std::string& line) const
    {
      if (strategy == Strategy::match_anything)
        return true;
      else if (strategy == Strategy::match_nonempty)
        return !line.empty();

      if (is_cache_available && line == prev_line)
        return prev_result;

      bool result;

      uint32_t options = 0;

      int rc;

      if (pcre2_jit_on) {
        rc = pcre2_jit_match(
          re,                                            /* the compiled pattern */
          reinterpret_cast<PCRE2_SPTR>(line.data()),     /* the subject string */
          line.size(),                                   /* the length of the subject */
          0,                                             /* starting offset in the subject */
          options,                                       /* options */
          match_data,                                    /* block for storing the result */
          NULL);                                         /* use default match context */
      }
      else {
        rc = pcre2_match(
          re,                                            /* the compiled pattern */
          reinterpret_cast<PCRE2_SPTR>(line.data()),     /* the subject string */
          line.size(),                                   /* the length of the subject */
          0,                                             /* starting offset in the subject */
          options,                                       /* options */
          match_data,                                    /* block for storing the result */
          NULL);                                         /* use default match context */
      }

      if (rc < 0)  {
        switch(rc)
          {
          case PCRE2_ERROR_NOMATCH:
              result = false;
              break;
          default:
            throw Regular_Expression_Error("PCRE2 failed");
          }
      } else {
        result = true;
      }

      is_cache_available = true;
      prev_result = result;
      prev_line = line;

      return (result);
    }

  private:
    pcre2_code *re;
    pcre2_match_data *match_data;
    pcre2_match_context *mcontext;
    pcre2_jit_stack *jit_stack;
    bool pcre2_jit_on;
};


#endif


class Regular_Expression_Factory
{

public:

  static Regular_Expression* get_regexp_engine(const std::string engine, const std::string& regex, bool case_sensitive )
  {
    if (engine == "ICU") {
#ifdef HAVE_ICU
      return new Regular_Expression_ICU(regex, case_sensitive);
#else
      throw std::runtime_error("ICU support not available");
#endif
    } else if (engine == "PCRE") {
#ifdef HAVE_PCRE
      return new Regular_Expression_PCRE(regex, case_sensitive, false);
#else
      throw std::runtime_error("PCRE support not available");
#endif
   } else if (engine == "PCREJIT") {
#ifdef HAVE_PCRE
      return new Regular_Expression_PCRE(regex, case_sensitive, true);
#else
      throw std::runtime_error("PCRE support not available");
#endif
    }

    // always fall back to POSIX
    return new Regular_Expression_POSIX(regex, case_sensitive);
  }
};


#endif
