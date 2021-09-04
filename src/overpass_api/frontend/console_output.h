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

#ifndef DE__OSM3S___OVERPASS_API__FRONTEND__CONSOLE_OUTPUT_H
#define DE__OSM3S___OVERPASS_API__FRONTEND__CONSOLE_OUTPUT_H

#include "../core/datatypes.h"


struct Console_Output : public Error_Output
{
  Console_Output(uint log_level_) : encoding_errors(false), parse_errors(false),
  static_errors(false), log_level(log_level_) {}

  void add_encoding_error(const std::string& error) override;
  void add_parse_error(const std::string& error, int line_number) override;
  void add_static_error(const std::string& error, int line_number) override;

  void add_encoding_remark(const std::string& error) override;
  void add_parse_remark(const std::string& error, int line_number) override;
  void add_static_remark(const std::string& error, int line_number) override;

  void runtime_error(const std::string& error) override;
  void runtime_remark(const std::string& error) override;

  void display_statement_progress
      (uint timer, const std::string& name, int progress, int line_number,
       const std::vector< std::pair< uint, uint > >& stack) override;

  bool display_encoding_errors() override { return encoding_errors; }
  bool display_parse_errors() override { return parse_errors; }
  bool display_static_errors() override { return static_errors; }

  ~Console_Output() override = default;

private:
  bool encoding_errors;
  bool parse_errors;
  bool static_errors;
  uint log_level;
};

#endif
