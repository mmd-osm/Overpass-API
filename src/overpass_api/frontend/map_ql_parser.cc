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

#include "../../expat/map_ql_input.h"
#include "../core/datatypes.h"
#include "../core/parsed_query.h"
#include "../dispatch/scripting_core.h"
#include "../statements/osm_script.h"
#include "../statements/statement.h"
#include "../statements/statement_dump.h"
#include "map_ql_parser.h"
#include "output_handler_parser.h"
#include "tokenizer_utils.h"

#include <cctype>
#include <fstream>
#include <iostream>
#include <iterator>
#include <queue>
#include <set>
#include <sstream>
#include <vector>


//-----------------------------------------------------------------------------

template< class TStatement >
TStatement* parse_statement(typename TStatement::Factory& stmt_factory, Parsed_Query& parsed_query,
			    Tokenizer_Wrapper& token, Error_Output* error_output, int depth);

std::string probe_into(Tokenizer_Wrapper& token, Error_Output* error_output)
{
  std::string into = "_";
  if (token.good() && *token == "->")
  {
    ++token;
    clear_until_after(token, error_output, ".");
    if (token.good())
      into = get_identifier_token(token, error_output, "Variable");
  }
  return into;
}

std::string probe_from(Tokenizer_Wrapper& token, Error_Output* error_output)
{
  std::string from = "_";
  if (token.good() && *token == ".")
  {
    ++token;
    if (token.good())
      from = get_identifier_token(token, error_output, "Variable");
  }
  return from;
}

template< class TStatement >
std::vector< TStatement* > collect_substatements(typename TStatement::Factory& stmt_factory, Parsed_Query& parsed_query,
					    Tokenizer_Wrapper& token, Error_Output* error_output, int depth)
{
  std::vector< TStatement* > substatements;
  clear_until_after(token, error_output, "(");
  while (token.good() && *token != ")")
  {
    TStatement* substatement = parse_statement< TStatement >
	(stmt_factory, parsed_query, token, error_output, depth+1);
    if (substatement)
      substatements.push_back(substatement);
    clear_until_after(token, error_output, ";", ")", false);
    if (*token == ";")
      ++token;
  }
  if (token.good())
    ++token;

  return substatements;
}

template< class TStatement >
std::vector< TStatement* > collect_substatements_and_probe
    (typename TStatement::Factory& stmt_factory, Parsed_Query& parsed_query,
     Tokenizer_Wrapper& token, Error_Output* error_output, bool& is_difference, int depth)
{
  is_difference = false;

  std::vector< TStatement* > substatements;
  clear_until_after(token, error_output, "(");
  if (token.good() && *token != ")")
  {
    TStatement* substatement = parse_statement< TStatement >
        (stmt_factory, parsed_query, token, error_output, depth+1);
    if (substatement)
      substatements.push_back(substatement);
    clear_until_after(token, error_output, ";", ")", "-", false);
    if (*token == ";")
      ++token;
    if (*token == "-")
    {
      is_difference = true;
      ++token;
    }
  }
  if (token.good() && *token != ")")
  {
    TStatement* substatement = parse_statement< TStatement >
	(stmt_factory, parsed_query, token, error_output, depth+1);
    if (substatement)
      substatements.push_back(substatement);
    clear_until_after(token, error_output, ";", ")", false);
    if (*token == ";")
      ++token;
    if (is_difference && *token != ")")
    {
      if (error_output)
        error_output->add_parse_error("difference always requires two operands", token.line_col().first);
      clear_until_after(token, error_output, ")", false);
    }
  }
  while (token.good() && *token != ")")
  {
    TStatement* substatement = parse_statement< TStatement >
	(stmt_factory, parsed_query, token, error_output, depth+1);
    if (substatement)
      substatements.push_back(substatement);
    clear_until_after(token, error_output, ";", ")", false);
    if (*token == ";")
      ++token;
  }
  if (token.good())
    ++token;

  return substatements;
}


//-----------------------------------------------------------------------------


template< class TStatement >
TStatement* parse_value_tree(typename TStatement::Factory& stmt_factory, Tokenizer_Wrapper& token,
    Error_Output* error_output, Statement::QL_Context tree_context, bool parenthesis_expected)
{
  Token_Tree tree(token, error_output, parenthesis_expected);
  if (tree.tree.empty())
    return 0;

  return stmt_factory.create_statement(Token_Node_Ptr(tree, tree.tree[0].rhs), tree_context);
}


template< class TStatement >
TStatement* create_union_statement(typename TStatement::Factory& stmt_factory,
				   std::string into, uint line_nr)
{
  std::map< std::string, std::string > attr;
  attr["into"] = into;
  return stmt_factory.create_statement("union", line_nr, attr);
}

template< class TStatement >
TStatement* create_difference_statement(typename TStatement::Factory& stmt_factory,
                                   std::string into, uint line_nr)
{
  std::map< std::string, std::string > attr;
  attr["into"] = into;
  return stmt_factory.create_statement("difference", line_nr, attr);
}

template< class TStatement >
TStatement* create_for_statement(typename TStatement::Factory& stmt_factory,
				     std::string stmt_name, std::string from, std::string into, uint line_nr)
{
  std::map< std::string, std::string > attr;
  attr["from"] = from;
  attr["into"] = into;
  return stmt_factory.create_statement(stmt_name, line_nr, attr);
}


template< class TStatement >
TStatement* create_make_statement(typename TStatement::Factory& stmt_factory,
    std::string strategy, std::string from, std::string into, std::string type, uint line_nr)
{
  std::map< std::string, std::string > attr;
  if (from != "")
    attr["from"] = from;
  attr["into"] = into;
  attr["type"] = type;
  return stmt_factory.create_statement(strategy, line_nr, attr);
}


template< class TStatement >
TStatement* create_filter_statement(typename TStatement::Factory& stmt_factory, uint line_nr)
{
  std::map< std::string, std::string > attr;
  return stmt_factory.create_statement("filter", line_nr, attr);
}


template< class TStatement >
TStatement* create_filter_statement(typename TStatement::Factory& stmt_factory,
    const Token_Node_Ptr& tree_it, Error_Output* error_output, uint line_nr)
{
  TStatement* filter = 0;
  
  if (tree_it->token == ":" && tree_it->rhs)
  {
    TStatement* criterion = stmt_factory.create_statement(tree_it.rhs(), Statement::elem_eval_possible);
    if (criterion)
    {
      filter = create_filter_statement< TStatement >(stmt_factory, line_nr);
      if (filter)
        filter->add_statement(criterion, "");
    }
  }

  return filter;
}


template< class TStatement >
TStatement* create_complete_statement(typename TStatement::Factory& stmt_factory,
    std::string from, std::string into, uint line_nr)
{
  std::map< std::string, std::string > attr;
  attr["from"] = from;
  attr["into"] = into;
  return stmt_factory.create_statement("complete", line_nr, attr);
}


template< class TStatement >
TStatement* create_if_statement(typename TStatement::Factory& stmt_factory, uint line_nr)
{
  std::map< std::string, std::string > attr;
  return stmt_factory.create_statement("if", line_nr, attr);
}


template< class TStatement >
TStatement* create_retro_statement(typename TStatement::Factory& stmt_factory, uint line_nr)
{
  std::map< std::string, std::string > attr;
  return stmt_factory.create_statement("retro", line_nr, attr);
}


template< class TStatement >
TStatement* create_print_statement(typename TStatement::Factory& stmt_factory,
                                   std::string from, std::string mode, std::string order, std::string limit, std::string geometry,
                                   std::string south, std::string north, std::string west, std::string east,
                                  uint line_nr)
{
  std::map< std::string, std::string > attr;
  attr["from"] = from;
  attr["mode"] = mode;
  attr["order"] = order;
  attr["limit"] = limit;
  attr["geometry"] = geometry;
  attr["s"] = south;
  attr["n"] = north;
  attr["w"] = west;
  attr["e"] = east;
  return stmt_factory.create_statement("print", line_nr, attr);
}


template< class TStatement >
TStatement* create_timeline_statement(typename TStatement::Factory& stmt_factory, uint line_nr,
    const std::string& type, const std::string& ref, const std::string& version, const std::string& into)
{
  std::map< std::string, std::string > attr;
  attr["type"] = type;
  attr["ref"] = ref;
  attr["version"] = version;
  attr["into"] = into;
  return stmt_factory.create_statement("timeline", line_nr, attr);
}


template< class TStatement >
TStatement* create_query_statement(typename TStatement::Factory& stmt_factory,
				   std::string type, std::string into, uint line_nr)
{
  std::map< std::string, std::string > attr;
  attr["type"] = type;
  attr["into"] = into;
  return stmt_factory.create_statement("query", line_nr, attr);
}

typedef enum { haskv_plain, haskv_regex, haskv_icase } haskv_type;

template< class TStatement >
TStatement* create_has_kv_statement(typename TStatement::Factory& stmt_factory,
				    std::string key, std::string value, haskv_type regex, haskv_type key_regex, bool straight,
				    uint line_nr)
{
  std::map< std::string, std::string > attr;

  if (key_regex == haskv_plain)
    attr["k"] = key;
  else
    attr["regk"] = key;

  if (regex == haskv_plain)
    attr["v"] = value;
  else if (regex == haskv_regex)
    attr["regv"] = value;
  else
  {
    attr["regv"] = value;
    attr["case"] = "ignore";
  }

  attr["modv"] = (straight ? "" : "not");
  return stmt_factory.create_statement("has-kv", line_nr, attr);
}


template< class TStatement >
TStatement* create_id_query_statement(typename TStatement::Factory& stmt_factory,
    std::string type, const std::vector< std::string >& ref, std::string into, uint line_nr)
{
  std::map< std::string, std::string > attr;

  attr["type"] = type;
  attr["into"] = into;

  for (uint i = 0; i < ref.size(); ++i)
  {
    std::stringstream id;
    if (i == 0)
      id << "ref";
    else
      id << "ref_" << i;
    attr[id.str()] = ref[i];
  }

  return stmt_factory.create_statement("id-query", line_nr, attr);
}


template< class TStatement >
TStatement* create_id_query_statement(typename TStatement::Factory& stmt_factory,
    Token_Node_Ptr tree_it, Error_Output* error_output, uint line_nr,
    const std::string& type, const std::string& into)
{
  std::vector< std::string > ref;
  
  while (tree_it->token == "," && tree_it->rhs && tree_it->lhs)
  {
    ref.push_back(tree_it.rhs()->token);
    tree_it = tree_it.lhs();
  }
  
  if (tree_it->token == ":" && tree_it->rhs)
    ref.push_back(tree_it.rhs()->token);
  else
    ref.push_back(tree_it->token);
  
  std::reverse(ref.begin(), ref.end());

  return create_id_query_statement< TStatement >(stmt_factory, type, ref, into, line_nr);
}


template< class TStatement >
TStatement* create_item_statement(typename TStatement::Factory& stmt_factory,
				  std::string from, std::string into, uint line_nr)
{
  std::map< std::string, std::string > attr;
  attr["from"] = from;
  attr["into"] = into;
  return stmt_factory.create_statement("item", line_nr, attr);
}


template< class TStatement >
TStatement* create_bbox_statement(typename TStatement::Factory& stmt_factory,
  std::string south, std::string north, std::string west, std::string east, std::string into, uint line_nr)
{
  std::map< std::string, std::string > attr;
  attr["s"] = south;
  attr["n"] = north;
  attr["w"] = west;
  attr["e"] = east;
  attr["into"] = into;
  return stmt_factory.create_statement("bbox-query", line_nr, attr);
}


template< class TStatement >
TStatement* create_bbox_statement(typename TStatement::Factory& stmt_factory,
    Token_Node_Ptr tree_it, Error_Output* error_output, uint line_nr, const std::string& into)
{
  if (tree_it->token != "," || !tree_it->rhs || !tree_it->lhs)
  {
    if (error_output)
      error_output->add_parse_error("bbox requires four arguments", line_nr);
    return 0;
  }
  
  std::string east = tree_it.rhs()->token;
  tree_it = tree_it.lhs();
  
  if (tree_it->token != "," || !tree_it->rhs || !tree_it->lhs)
  {
    if (error_output)
      error_output->add_parse_error("bbox requires four arguments", line_nr);
    return 0;
  }
  
  std::string north = tree_it.rhs()->token;
  tree_it = tree_it.lhs();
  
  if (tree_it->token != "," || !tree_it->rhs || !tree_it->lhs)
  {
    if (error_output)
      error_output->add_parse_error("bbox requires four arguments", line_nr);
    return 0;
  }
  
  std::string west = tree_it.rhs()->token;
  std::string south = tree_it.lhs()->token;
  
  return create_bbox_statement< TStatement >(stmt_factory, south, north, west, east, into, line_nr);
}


template< class TStatement >
TStatement* create_around_statement(typename TStatement::Factory& stmt_factory,
				    std::string radius, std::string lat, std::string lon,
                                    std::string from, std::string into, uint line_nr)
{
  std::map< std::string, std::string > attr;
  attr["from"] = from;
  attr["into"] = into;
  attr["radius"] = radius;
  attr["lat"] = lat;
  attr["lon"] = lon;
  return stmt_factory.create_statement("around", line_nr, attr);
}


template< class TStatement >
TStatement* create_around_statement(typename TStatement::Factory& stmt_factory,
    Token_Node_Ptr tree_it, Error_Output* error_output, uint line_nr, const std::string& into)
{
  std::string lat;
  std::string lon;
  
  if (tree_it->token == "," && tree_it->rhs && tree_it->lhs)
  {
    lon = tree_it.rhs()->token;
    tree_it = tree_it.lhs();
    
    if (tree_it->token != "," || !tree_it->rhs || !tree_it->lhs)
    {
      if (error_output)
        error_output->add_parse_error("around requires one or three arguments", line_nr);
      return 0;
    }
    
    lat = tree_it.rhs()->token;
    tree_it = tree_it.lhs();
  }
  
  if (tree_it->token == ":" && tree_it->rhs)
  {
    std::string radius = decode_json(tree_it.rhs()->token, error_output);
    
    tree_it = tree_it.lhs();
    std::string from = "_";
    if (tree_it->token == "." && tree_it->rhs)
      from = tree_it.rhs()->token;
    
    return create_around_statement< TStatement >(stmt_factory, radius, lat, lon, from, into, line_nr);
  }
  else if (error_output)
    error_output->add_parse_error("around requires the radius as first argument", line_nr);

  return 0;
}


template< class TStatement >
TStatement* create_recurse_statement(typename TStatement::Factory& stmt_factory,
				     std::string type, std::string from, std::string into, uint line_nr)
{
  std::map< std::string, std::string > attr;
  attr["from"] = (from == "" ? "_" : from);
  attr["into"] = into;
  attr["type"] = type;
  return stmt_factory.create_statement("recurse", line_nr, attr);
}


template< class TStatement >
TStatement* create_recurse_statement(typename TStatement::Factory& stmt_factory,
                                     std::string type, std::string from, std::string role, std::string into, uint line_nr)
{
  std::map< std::string, std::string > attr;
  attr["from"] = (from == "" ? "_" : from);
  attr["into"] = into;
  attr["type"] = type;
  attr["role"] = role;
  attr["role-restricted"] = "yes";
  return stmt_factory.create_statement("recurse", line_nr, attr);
}


template< class TStatement >
TStatement* create_recurse_statement(typename TStatement::Factory& stmt_factory,
    Token_Node_Ptr tree_it, Error_Output* error_output, uint line_nr, const std::string& result_type,
    const std::string& into)
{
  std::string from = "_";
  std::string role;
  bool role_found = false;
  
  if (tree_it->token == ":" && tree_it->rhs)
  {
    role_found = true;
    role = decode_json(tree_it.rhs()->token, error_output);
    tree_it = tree_it.lhs();
  }

  if (tree_it->token == "." && tree_it->rhs)
  {
    from = tree_it.rhs()->token;
    tree_it = tree_it.lhs();
  }
  
  std::string type = tree_it->token;

  if (type == ">")
    return create_recurse_statement< TStatement >(stmt_factory, "down", from, "_", line_nr);
  else if (type == ">>")
    return create_recurse_statement< TStatement >(stmt_factory, "down-rel", from, "_", line_nr);
  else if (type == "<")
    return create_recurse_statement< TStatement >(stmt_factory, "up", from, "_", line_nr);
  else if (type == "<<")
    return create_recurse_statement< TStatement >(stmt_factory, "up-rel", from, "_", line_nr);
  else if (type == "r")
  {
    if (result_type == "node")
      type = "relation-node";
    else if (result_type == "way")
      type = "relation-way";
    else if (result_type == "relation")
      type = "relation-relation";
    else if (error_output)
      error_output->add_parse_error("A recursion from type 'r' produces nodes, ways, or relations.", line_nr);
  }
  else if (type == "w")
  {
    if (result_type == "node")
      type = "way-node";
    else if (error_output)
      error_output->add_parse_error("A recursion from type 'w' produces nodes.", line_nr);
  }
  else if (type == "br")
  {
    if (result_type == "relation")
      type = "relation-backwards";
    else if (error_output)
      error_output->add_parse_error("A recursion from type 'br' produces relations.", line_nr);
  }
  else if (type == "bw")
  {
    if (result_type == "relation")
      type = "way-relation";
    else if (error_output)
      error_output->add_parse_error("A recursion from type 'bw' produces relations.", line_nr);
  }
  else if (type == "bn")
  {
    if (result_type == "way")
      type = "node-way";
    else if (result_type == "relation")
      type = "node-relation";
    else if (error_output)
      error_output->add_parse_error("A recursion from type 'bn' produces ways or relations.", line_nr);
  }
  else
    return 0;

  if (role_found)
    return create_recurse_statement< TStatement >(stmt_factory, type, from, role, into, line_nr);
  return create_recurse_statement< TStatement >(stmt_factory, type, from, into, line_nr);
}


template< class TStatement >
TStatement* create_polygon_statement(typename TStatement::Factory& stmt_factory,
				   std::string bounds, std::string into, uint line_nr)
{
  std::map< std::string, std::string > attr;
  attr["bounds"] = bounds;
  attr["into"] = into;
  return stmt_factory.create_statement("polygon-query", line_nr, attr);
}


template< class TStatement >
TStatement* create_polygon_statement(typename TStatement::Factory& stmt_factory,
    const Token_Node_Ptr& tree_it, Error_Output* error_output, uint line_nr, const std::string& into)
{
  if (tree_it->token == ":" && tree_it->rhs)
  {
    std::string bounds = decode_json(tree_it.rhs()->token, error_output);
    return create_polygon_statement< TStatement >(stmt_factory, bounds, into, line_nr);
  }

  return 0;
}


template< class TStatement >
TStatement* create_user_statement
    (typename TStatement::Factory& stmt_factory,
     std::string type, const std::vector< std::string >& name, const std::vector< std::string >& uid, std::string into, uint line_nr)
{
  std::map< std::string, std::string > attr;
  std::vector< std::string >::const_iterator it;
  int i;

  attr["into"] = into;

  if (uid.empty())
    attr["uid"] = "";

  if (name.empty())
    attr["name"] = "";

  for(it = name.begin(), i = 0; it != name.end(); ++it, ++i)
  {
    std::stringstream id;
    if (i == 0)
      id << "name";
    else
      id << "name_" << i;
    attr[id.str()] = *it;
  }

  for(it = uid.begin(), i = 0; it != uid.end(); ++it, ++i)
  {
    std::stringstream id;
    if (i == 0)
      id << "uid";
    else
      id << "uid_" << i;
    attr[id.str()] = *it;
  }

  attr["type"] = type;
  return stmt_factory.create_statement("user", line_nr, attr);
}


template< class TStatement >
TStatement* create_user_statement(typename TStatement::Factory& stmt_factory,
    Token_Node_Ptr tree_it, Error_Output* error_output, uint line_nr,
    const std::string& type, const std::string& into)
{
  std::vector< std::string > users;
  
  while (tree_it->token == "," && tree_it->rhs && tree_it->lhs)
  {
    users.push_back(tree_it.rhs()->token);
    tree_it = tree_it.lhs();
  }
  
  if (tree_it->token == ":" && tree_it->rhs)
    users.push_back(tree_it.rhs()->token);
  
  std::reverse(users.begin(), users.end());

  if (tree_it->lhs && tree_it.lhs()->token == "user")
  {
    for (std::vector< std::string >::iterator it = users.begin(); it != users.end(); ++it)
      *it = decode_json(*it, error_output);
    return create_user_statement< TStatement >(
        stmt_factory, type, users, std::vector< std::string >(), into, line_nr);
  }
  return create_user_statement< TStatement >(
      stmt_factory, type, std::vector< std::string >(), users, into, line_nr);
}


template< class TStatement >
TStatement* create_newer_statement(typename TStatement::Factory& stmt_factory,
				   std::string than, uint line_nr)
{
  std::map< std::string, std::string > attr;
  attr["than"] = than;
  return stmt_factory.create_statement("newer", line_nr, attr);
}


template< class TStatement >
TStatement* create_newer_statement(typename TStatement::Factory& stmt_factory,
    const Token_Node_Ptr& tree_it, Error_Output* error_output, uint line_nr)
{
  if (tree_it->token == ":" && tree_it->rhs)
  {
    std::string date = decode_json(tree_it.rhs()->token, error_output);
    return create_newer_statement< TStatement >(stmt_factory, date, line_nr);
  }

  return 0;
}


template< class TStatement >
TStatement* create_area_statement(typename TStatement::Factory& stmt_factory,
				   std::string ref, std::string from, std::string into, uint line_nr)
{
  std::map< std::string, std::string > attr;
  attr["from"] = (from == "" ? "_" : from);
  attr["into"] = into;
  attr["ref"] = ref;
  return stmt_factory.create_statement("area-query", line_nr, attr);
}


template< class TStatement >
TStatement* create_area_statement(typename TStatement::Factory& stmt_factory,
    Token_Node_Ptr tree_it, Error_Output* error_output, uint line_nr, const std::string& into)
{
  std::string from = "_";
  std::string ref;
  
  if (tree_it->token == ":" && tree_it->rhs)
  {
    ref = tree_it.rhs()->token;
    tree_it = tree_it.lhs();
  }
  
  if (tree_it->token == "." && tree_it->rhs)
    from = tree_it.rhs()->token;

  return create_area_statement< TStatement >(stmt_factory, ref, from, into, line_nr);
}


template< class TStatement >
TStatement* create_coord_query_statement(typename TStatement::Factory& stmt_factory,
				     std::string lat, std::string lon, std::string from, std::string into, uint line_nr)
{
  std::map< std::string, std::string > attr;
  attr["from"] = (from == "" ? "_" : from);
  attr["into"] = into;
  attr["lat"] = lat;
  attr["lon"] = lon;
  return stmt_factory.create_statement("coord-query", line_nr, attr);
}

template< class TStatement >
TStatement* create_map_to_area_statement(typename TStatement::Factory& stmt_factory,
				     std::string from, std::string into, uint line_nr)
{
  std::map< std::string, std::string > attr;
  attr["from"] = (from == "" ? "_" : from);
  attr["into"] = into;
  return stmt_factory.create_statement("map-to-area", line_nr, attr);
}


template< class TStatement >
TStatement* create_pivot_statement(typename TStatement::Factory& stmt_factory,
                                   std::string from, std::string into, uint line_nr)
{
  std::map< std::string, std::string > attr;
  attr["from"] = (from == "" ? "_" : from);
  attr["into"] = into;
  return stmt_factory.create_statement("pivot", line_nr, attr);
}


template< class TStatement >
TStatement* create_pivot_statement(typename TStatement::Factory& stmt_factory,
    const Token_Node_Ptr& tree_it, Error_Output* error_output, uint line_nr, const std::string& into)
{
  std::string from = "_";
  if (tree_it->rhs && tree_it->token == ".")
      from = tree_it.rhs()->token;
  
  return create_pivot_statement< TStatement >(stmt_factory, from, into, line_nr);
}


template< class TStatement >
TStatement* create_changed_statement(typename TStatement::Factory& stmt_factory,
				   std::string since, std::string until, uint line_nr)
{
  std::map< std::string, std::string > attr;
  attr["since"] = since;
  attr["until"] = until;
  return stmt_factory.create_statement("changed", line_nr, attr);
}


template< class TStatement >
TStatement* create_changed_statement(typename TStatement::Factory& stmt_factory,
    const Token_Node_Ptr& tree_it, Error_Output* error_output, uint line_nr)
{
  std::string since;
  std::string until;
  
  if (tree_it->token == ":" && tree_it->rhs)
  {
    since = decode_json(tree_it.rhs()->token, error_output);
    until = since;
  }
  else if (tree_it->token == "," && tree_it->lhs && tree_it.lhs()->token == ":" && tree_it.lhs()->rhs
      && tree_it->rhs)
  {
    since = decode_json(tree_it.lhs().rhs()->token, error_output);
    until = decode_json(tree_it.rhs()->token, error_output);
  }
  else if (tree_it->token == "changed")
  {
    since = "auto";
    until = "auto";
  }
  else if (tree_it->token == ":")
  {
    if (error_output)
      error_output->add_parse_error("Date required after \"changed\" with colon",
          tree_it->line_col.first);
  }
  else
    if (error_output)
      error_output->add_parse_error("Unexpected token \"" + tree_it->token + "\" after \"changed\"",
          tree_it->line_col.first);

  return create_changed_statement< TStatement >(stmt_factory, since, until, line_nr);
}


//-----------------------------------------------------------------------------

std::vector< std::string > parse_setup(Tokenizer_Wrapper& token,
				       Error_Output* error_output, Parsed_Query& parsed_query)
{
  ++token;
  std::vector< std::string > result;
  result.push_back(get_identifier_token(token, error_output, "Keyword"));
  clear_until_after(token, error_output, ":", "]");
  result.push_back(get_text_token(token, error_output, "Value"));
  if (result.front() == "out")
  {
    Output_Handler_Parser* format_parser =
	Output_Handler_Parser::get_format_parser(result.back());
	
    if (!format_parser)
    {
      if (error_output)
	error_output->add_parse_error("Unknown output format: " + result.back(), token.line_col().first);
    }
    else
    {
      parsed_query.set_output_handler(format_parser, &token, error_output);
      if (parsed_query.get_output_handler())
      {
        result.push_back("output-config");
        result.push_back(parsed_query.get_output_handler()->dump_config());
      }
    }

    clear_until_after(token, error_output, "]", true);
  }
  else if (result.front() == "diff" || result.front() == "adiff")
  {
    clear_until_after(token, error_output, ",", "]", false);
    if (*token == ",")
    {
      ++token;
      result.push_back(get_text_token(token, error_output, "Value"));
      clear_until_after(token, error_output, "]", true);
    }
    else
      ++token;
  }
  else if (result.front() == "bbox")
  {
    clear_until_after(token, error_output, ",", "]", false);
    if (*token == ",")
    {
      ++token;
      result.back() += "," + get_text_token(token, error_output, "Value");
      clear_until_after(token, error_output, ",", "]", false);
    }
    if (*token == ",")
    {
      ++token;
      result.back() += "," + get_text_token(token, error_output, "Value");
      clear_until_after(token, error_output, ",", "]", false);
    }
    if (*token == ",")
    {
      ++token;
      result.back() += "," + get_text_token(token, error_output, "Value");
      clear_until_after(token, error_output, "]");
    }
  }
  else
    clear_until_after(token, error_output, "]");
  return result;
}


template< class TStatement >
TStatement* parse_union(typename TStatement::Factory& stmt_factory, Parsed_Query& parsed_query,
			Tokenizer_Wrapper& token, Error_Output* error_output, int depth)
{
  std::pair< uint, uint > line_col = token.line_col();

  bool is_difference = false;
  std::vector< TStatement* > substatements =
      collect_substatements_and_probe< TStatement >(stmt_factory, parsed_query, token, error_output,
						    is_difference, depth+1);
  std::string into = probe_into(token, error_output);

  if (is_difference)
  {
    TStatement* statement = create_difference_statement< TStatement >(stmt_factory, into, line_col.first);
    for (typename std::vector< TStatement* >::const_iterator it = substatements.begin();
        it != substatements.end(); ++it)
      statement->add_statement(*it, "");
    return statement;
  }
  else
  {
    TStatement* statement = create_union_statement< TStatement >(stmt_factory, into, line_col.first);
    for (typename std::vector< TStatement* >::const_iterator it = substatements.begin();
        it != substatements.end(); ++it)
      statement->add_statement(*it, "");
    return statement;
  }
}


template< class TStatement >
TStatement* parse_foreach(typename TStatement::Factory& stmt_factory, Parsed_Query& parsed_query,
			  Tokenizer_Wrapper& token, Error_Output* error_output, int depth)
{
  std::pair< uint, uint > line_col = token.line_col();
  ++token;

  std::string from = probe_from(token, error_output);
  std::string into = probe_into(token, error_output);
  std::vector< TStatement* > substatements =
      collect_substatements< TStatement >(stmt_factory, parsed_query, token, error_output, depth+1);

  TStatement* statement = create_for_statement< TStatement >
      (stmt_factory, "foreach", from, into, line_col.first);
  for (typename std::vector< TStatement* >::const_iterator it = substatements.begin();
      it != substatements.end(); ++it)
    statement->add_statement(*it, "");
  return statement;
}


template< class TStatement >
TStatement* parse_for(typename TStatement::Factory& stmt_factory, Parsed_Query& parsed_query,
			  Tokenizer_Wrapper& token, Error_Output* error_output, int depth)
{
  std::pair< uint, uint > line_col = token.line_col();
  ++token;

  std::string from = probe_from(token, error_output);
  std::string into = probe_into(token, error_output);

  clear_until_after(token, error_output, "(");
  TStatement* condition = parse_value_tree< TStatement >(stmt_factory, token, error_output,
      Statement::elem_eval_possible, true);
  clear_until_after(token, error_output, ")");
  std::vector< TStatement* > substatements =
      collect_substatements< TStatement >(stmt_factory, parsed_query, token, error_output, depth+1);

  TStatement* statement = create_for_statement< TStatement >
      (stmt_factory, "for", from, into, line_col.first);
  statement->add_statement(condition, "");
  for (typename std::vector< TStatement* >::const_iterator it = substatements.begin();
      it != substatements.end(); ++it)
    statement->add_statement(*it, "");
  return statement;
}


template< class TStatement >
TStatement* parse_complete(typename TStatement::Factory& stmt_factory, Parsed_Query& parsed_query,
              Tokenizer_Wrapper& token, Error_Output* error_output, int depth)
{
  std::pair< uint, uint > line_col = token.line_col();
  ++token;

  std::string from = probe_from(token, error_output);
  std::string into = probe_into(token, error_output);
  std::vector< TStatement* > substatements =
      collect_substatements< TStatement >(stmt_factory, parsed_query, token, error_output, depth);

  TStatement* statement = create_complete_statement< TStatement >
      (stmt_factory, from, into, line_col.first);
  for (typename std::vector< TStatement* >::const_iterator it = substatements.begin();
      it != substatements.end(); ++it)
    statement->add_statement(*it, "");
  return statement;
}


template< class TStatement >
TStatement* parse_if(typename TStatement::Factory& stmt_factory, Parsed_Query& parsed_query,
              Tokenizer_Wrapper& token, Error_Output* error_output, int depth)
{
  std::pair< uint, uint > line_col = token.line_col();
  ++token;

  clear_until_after(token, error_output, "(");
  TStatement* condition = parse_value_tree< TStatement >(stmt_factory, token, error_output,
      Statement::evaluator_expected, true);
  clear_until_after(token, error_output, ")");
  std::vector< TStatement* > substatements =
      collect_substatements< TStatement >(stmt_factory, parsed_query, token, error_output, depth);

  TStatement* statement = create_if_statement< TStatement >(stmt_factory, line_col.first);
  statement->add_statement(condition, "");
  for (typename std::vector< TStatement* >::const_iterator it = substatements.begin();
      it != substatements.end(); ++it)
    statement->add_statement(*it, "");
  return statement;
}


template< class TStatement >
TStatement* parse_retro(typename TStatement::Factory& stmt_factory, Parsed_Query& parsed_query,
              Tokenizer_Wrapper& token, Error_Output* error_output, int depth)
{
  std::pair< uint, uint > line_col = token.line_col();
  ++token;

  clear_until_after(token, error_output, "(");
  TStatement* condition = parse_value_tree< TStatement >(stmt_factory, token, error_output,
      Statement::evaluator_expected, true);
  clear_until_after(token, error_output, ")");
  std::vector< TStatement* > substatements =
      collect_substatements< TStatement >(stmt_factory, parsed_query, token, error_output, depth);

  TStatement* statement = create_retro_statement< TStatement >(stmt_factory, line_col.first);
  statement->add_statement(condition, "");
  for (typename std::vector< TStatement* >::const_iterator it = substatements.begin();
      it != substatements.end(); ++it)
    statement->add_statement(*it, "");
  return statement;
}


template< class TStatement >
TStatement* parse_timeline(typename TStatement::Factory& stmt_factory, Parsed_Query& parsed_query,
              Tokenizer_Wrapper& token, Error_Output* error_output, int depth)
{
  std::pair< uint, uint > line_col = token.line_col();
  ++token;

  clear_until_after(token, error_output, "(");
  std::string type = get_text_token(token, error_output, "OSM base type");
  if (type == "rel")
    type = "relation";
    
  clear_until_after(token, error_output, ",");
  std::string ref = get_text_token(token, error_output, "OSM element id");
  clear_until_after(token, error_output, ",", ")", false);
  std::string version;
  if (*token == ",")
  {
    ++token;
    version = get_text_token(token, error_output, "Number");
  }
  clear_until_after(token, error_output, ")");

  std::string into = probe_into(token, error_output);

  return create_timeline_statement< TStatement >(stmt_factory, line_col.first, type, ref, version, into);
}


template< class TStatement >
TStatement* parse_output(typename TStatement::Factory& stmt_factory,
			 const std::string& from, Tokenizer_Wrapper& token, Error_Output* error_output)
{
  TStatement* statement = 0;
  if (*token == "out")
  {
    ++token;
    std::string mode = "body";
    std::string order = "id";
    std::string limit = "";
    std::string geometry = "skeleton";
    std::string south = "";
    std::string north = "";
    std::string west = "";
    std::string east = "";
    while (token.good() && *token != ";")
    {
      if (*token == "ids")
	mode = "ids_only";
      else if (*token == "tags")
        mode = "tags";
      else if (*token == "skel")
	mode = "skeleton";
      else if (*token == "body")
	mode = "body";
      else if (*token == "meta")
	mode = "meta";
      else if (*token == "quirks")
	mode = "quirks";
      else if (*token == "count")
        mode = "count";
      else if (*token == "qt")
	order = "quadtile";
      else if (*token == "asc")
	order = "id";
      else if (*token == "geom")
        geometry = "full";
      else if (*token == "bb")
        geometry = "bounds";
      else if (*token == "center")
        geometry = "center";
      else if (isdigit((*token)[0]))
	limit = *token;
      else if (*token == "(")
      {
        ++token;
        south = get_text_token(token, error_output, "Number");
        clear_until_after(token, error_output, ",");
        west = get_text_token(token, error_output, "Number");
        clear_until_after(token, error_output, ",");
        north = get_text_token(token, error_output, "Number");
        clear_until_after(token, error_output, ",");
        east = get_text_token(token, error_output, "Number");
        clear_until_after(token, error_output, ")", false);
      }
      else
      {
	if (error_output)
	  error_output->add_parse_error
	      (std::string("Invalid parameter for print: \"") + *token + "\"", token.line_col().first);
      }
      ++token;
    }

    if (statement == 0)
    {
      statement = create_print_statement< TStatement >
          (stmt_factory, from == "" ? "_" : from, mode, order, limit, geometry,
           south, north, west, east,
           token.line_col().first);
    }
    else
    {
      if (error_output)
	error_output->add_parse_error("Garbage after output statement found.",
				      token.line_col().first);
    }
  }

  return statement;
}


template< class TStatement >
TStatement* parse_make(typename TStatement::Factory& stmt_factory, const std::string& from,
                       Tokenizer_Wrapper& token, Error_Output* error_output, const std::string& strategy)
{
  TStatement* statement = 0;
  std::vector< TStatement* > evaluators;
  std::string type = "";
  if (*token == strategy)
  {
    ++token;
    if (*token != ";")
      type = get_identifier_token(token, error_output, "Element class name");

    Token_Tree tree(token, error_output, false);
    if (tree.tree.size() > 1)
    {
      Statement::QL_Context tree_context = (strategy == "convert" ? Statement::in_convert : Statement::generic);
      Token_Node_Ptr tree_it(tree, tree.tree[0].rhs);
      
      while (tree_it->token == ",")
      {
        if (tree_it->rhs)
        {
          TStatement* stmt = stmt_factory.create_statement(tree_it.rhs(), tree_context);
          if (stmt)
            evaluators.push_back(stmt);
        }
      
        tree_it = tree_it.lhs();
      }
      
      TStatement* stmt = stmt_factory.create_statement(tree_it, tree_context);
      if (stmt)
        evaluators.push_back(stmt);
      
      std::reverse(evaluators.begin(),evaluators.end());
    }

    std::string into = probe_into(token, error_output);

    statement = create_make_statement< TStatement >(stmt_factory, strategy, from, into, type, token.line_col().first);
    {
      for (typename std::vector< TStatement* >::const_iterator it = evaluators.begin();
          it != evaluators.end(); ++it)
        statement->add_statement(*it, "");
    }
  }

  return statement;
}


struct Statement_Text
{
  Statement_Text(std::string statement_ = "",
		 std::pair< uint, uint > line_col_ = std::make_pair(0, 0))
    : statement(statement_), line_col(line_col_) {}

  std::string statement;
  std::pair< uint, uint > line_col;
  std::vector< std::string > attributes;
};

template< class TStatement >
TStatement* create_query_substatement
    (typename TStatement::Factory& stmt_factory,
     Tokenizer_Wrapper& token, Error_Output* error_output,
     const Statement_Text& clause, std::string type, std::string from, std::string into)
{
  if (clause.statement == "has-kv")
    return create_has_kv_statement< TStatement >
        (stmt_factory, clause.attributes[0], clause.attributes[1], haskv_plain, haskv_plain,
	 (clause.attributes[2] == ""), clause.line_col.first);
  else if (clause.statement == "has-kv_regex")
    return create_has_kv_statement< TStatement >
        (stmt_factory, clause.attributes[0], clause.attributes[1], haskv_regex, haskv_plain,
	 (clause.attributes[2] == ""), clause.line_col.first);
  else if (clause.statement == "has-kv_icase")
    return create_has_kv_statement< TStatement >
        (stmt_factory, clause.attributes[0], clause.attributes[1], haskv_icase, haskv_plain,
	 (clause.attributes[2] == ""), clause.line_col.first);
  else if (clause.statement == "has-kv_keyregex_icase")
    return create_has_kv_statement< TStatement >
        (stmt_factory, clause.attributes[0], clause.attributes[1], haskv_icase, haskv_regex,
     (clause.attributes[2] == ""), clause.line_col.first);
  else if (clause.statement == "has-kv_keyregex")
    return create_has_kv_statement< TStatement >
        (stmt_factory, clause.attributes[0], clause.attributes[1], haskv_regex, haskv_regex,
	 (clause.attributes[2] == ""), clause.line_col.first);
  else if (clause.statement == "id-query")
    return create_id_query_statement< TStatement >
        (stmt_factory, type, clause.attributes, into, clause.line_col.first);
  else if (clause.statement == "bbox-query")
    return create_bbox_statement< TStatement >
        (stmt_factory,
	 clause.attributes[0], clause.attributes[2], clause.attributes[1], clause.attributes[3],
	 into, clause.line_col.first);
  else if (clause.statement == "item")
    return create_item_statement< TStatement >
        (stmt_factory, clause.attributes[0], "_", clause.line_col.first);
  return 0;
}

template< class TStatement >
TStatement* parse_full_recurse(typename TStatement::Factory& stmt_factory,
    Tokenizer_Wrapper& token, const std::string& from, Error_Output* error_output)
{
  std::string type = *token;
  uint line_col = token.line_col().first;
  ++token;
  std::string into = probe_into(token, error_output);

  if (type == ">")
    return create_recurse_statement< TStatement >(stmt_factory, "down", from, into, line_col);
  else if (type == ">>")
    return create_recurse_statement< TStatement >(stmt_factory, "down-rel", from, into, line_col);
  else if (type == "<")
    return create_recurse_statement< TStatement >(stmt_factory, "up", from, into, line_col);
  else if (type == "<<")
    return create_recurse_statement< TStatement >(stmt_factory, "up-rel", from, into, line_col);
  else
    return 0;
}

template< class TStatement >
TStatement* parse_coord_query(typename TStatement::Factory& stmt_factory,
    Tokenizer_Wrapper& token, const std::string& from, Error_Output* error_output)
{
  std::string type = *token;
  uint line_col = token.line_col().first;
  ++token;

  std::string lat, lon;
  if (*token == "(")
  {
    ++token;
    lat = get_text_token(token, error_output, "Number");
    clear_until_after(token, error_output, ",", ")", false);
    if (*token == ",")
    {
      ++token;
      lon = get_text_token(token, error_output, "Number");
      clear_until_after(token, error_output, ")", false);
    }
    ++token;
  }
  std::string into = probe_into(token, error_output);

  return create_coord_query_statement< TStatement >(stmt_factory, lat, lon, from, into, line_col);
}

template< class TStatement >
TStatement* parse_map_to_area(typename TStatement::Factory& stmt_factory,
    Tokenizer_Wrapper& token, const std::string& from, Error_Output* error_output)
{
  std::string type = *token;
  uint line_col = token.line_col().first;
  ++token;

  std::string into = probe_into(token, error_output);

  return create_map_to_area_statement< TStatement >(stmt_factory, from, into, line_col);
}


Token_Node_Ptr find_leftmost_token(Token_Node_Ptr tree_it)
{
  while (tree_it->lhs)
    tree_it = tree_it.lhs();
  
  return tree_it;
}


template< class TStatement >
TStatement* create_query_criterion(typename TStatement::Factory& stmt_factory,
    const Token_Node_Ptr& tree_it, Error_Output* error_output,
    const std::string& type, bool& can_standalone, const std::string& into)
{
  Token_Node_Ptr criterion = find_leftmost_token(tree_it);
  uint line_nr = criterion->line_col.first;
  
  bool criterion_is_number = (!criterion->token.empty()
      && (isdigit(criterion->token[0])
          || (criterion->token[0] == '-' && criterion->token.size() > 1 && isdigit(criterion->token[1]))));
  
  can_standalone = true;
  if (criterion->token == "id" || (criterion_is_number && tree_it->token != ","))
    return create_id_query_statement< TStatement >(stmt_factory, tree_it, error_output, line_nr, type, into);
  else if (criterion->token == "uid" || criterion->token == "user")
    return create_user_statement< TStatement >(stmt_factory, tree_it, error_output, line_nr, type, into);
  else if (criterion->token == "r" || criterion->token == "w"
      || criterion->token == "bn" || criterion->token == "bw" || criterion->token == "br")
    return create_recurse_statement< TStatement >(stmt_factory, tree_it, error_output, line_nr, type, into);
  
  can_standalone = (type == "node");
  if (criterion->token == "area")
    return create_area_statement< TStatement >(stmt_factory, tree_it, error_output, line_nr, into);
  else if (criterion->token == "around")
    return create_around_statement< TStatement >(stmt_factory, tree_it, error_output, line_nr, into);
  else if (criterion->token == "pivot")
    return create_pivot_statement< TStatement >(stmt_factory, tree_it, error_output, line_nr, into);
  else if (criterion->token == "poly")
    return create_polygon_statement< TStatement >(stmt_factory, tree_it, error_output, line_nr, into);
  else if (criterion_is_number && tree_it->token == ",")
    return create_bbox_statement< TStatement >(stmt_factory, tree_it, error_output, line_nr, into);
  
  can_standalone = false;
  if (criterion->token == "changed")
    return create_changed_statement< TStatement >(stmt_factory, tree_it, error_output, line_nr);
  else if (criterion->token == "if")
    return create_filter_statement< TStatement >(stmt_factory, tree_it, error_output, line_nr);
  else if (criterion->token == "newer")
    return create_newer_statement< TStatement >(stmt_factory, tree_it, error_output, line_nr);
  else if (criterion->token == ">" || criterion->token == ">>"
      || criterion->token == "<" || criterion->token == "<<")
    return create_recurse_statement< TStatement >(stmt_factory, tree_it, error_output, line_nr, type, "_");

  if (error_output)
    error_output->add_parse_error("Unknown query clause", line_nr);
  return 0;
}


template< class TStatement >
TStatement* parse_query(typename TStatement::Factory& stmt_factory, Parsed_Query& parsed_query,
			const std::string& type, const std::string& from, Tokenizer_Wrapper& token,
		 Error_Output* error_output)
{
  std::pair< uint, uint > query_line_col = token.line_col();

  std::vector< Statement_Text > clauses;
  std::vector< Token_Tree > subtrees;
  while (token.good() && (*token == "[" || *token == "(" || *token == "."))
  {
    if (*token == "[")
    {
      ++token;

      bool key_regex = (*token == "~");
      if (key_regex)
	++token;

      if (*token == "!")    // [!key] as shortcut for [key !~ ".*"]
      {
        ++token;
        std::string key = get_text_token(token, error_output, "Key");
        clear_until_after(token, error_output, "]");
        Statement_Text clause("has-kv_regex", token.line_col());
        clause.attributes.push_back(key);
        clause.attributes.push_back(".*");
        clause.attributes.push_back("!");
        clauses.push_back(clause);
        continue;
      }

      std::string key = get_text_token(token, error_output, "Key");
      clear_until_after(token, error_output, "!", "~", "=", "!=", "]", false);

      bool straight = true;
      if (*token == "!")
      {
	straight = false;
	++token;
	clear_until_after(token, error_output, "~", "=", "]", false);
      }

      if (*token == "]")
      {
	if (key_regex && error_output)
	  error_output->add_parse_error(
	      "A regular expression for a key can only be combined with a regular expression as value criterion",
	      token.line_col().first);
	
	Statement_Text clause("has-kv", token.line_col());
	clause.attributes.push_back(key);
	clause.attributes.push_back("");
	clause.attributes.push_back(straight ? "" : "!");
	++token;
	clauses.push_back(clause);
      }
      else if (*token == "=" || *token == "!=")
      {
	if (key_regex && error_output)
	  error_output->add_parse_error(
	      "A regular expression for a key can only be combined with a regular expression as value criterion",
	      token.line_col().first);
	
	straight = (*token == "=");
	++token;
	if (token.good() && *token == "]")
	  ++token;
	else
	{
	  Statement_Text clause("has-kv", token.line_col());
	  clause.attributes.push_back(key);
	  clause.attributes.push_back(get_text_token(token, error_output, "Value"));
	  if (clause.attributes.back() != "")
	  {
	    clause.attributes.push_back(straight ? "" : "!");
	    clauses.push_back(clause);
	  }
	  clear_until_after(token, error_output, "]");
	}
      }
      else //if (*token == "~")
      {
	if (key_regex)
	{
	  ++token;
	  Statement_Text clause("has-kv_keyregex", token.line_col());
	  clause.attributes.push_back(key);
	  clause.attributes.push_back(get_text_token(token, error_output, "Value"));
	  clause.attributes.push_back(straight ? "" : "!");
	  clear_until_after(token, error_output, ",", "]", false);
	  if (*token == ",")
	  {
	    clause.statement = "has-kv_keyregex_icase";
	    ++token;
	    clear_until_after(token, error_output, "i");
	    clear_until_after(token, error_output, "]", false);
	  }
	  ++token;
	  clauses.push_back(clause);
	}
	else
	{
	  ++token;
	  Statement_Text clause("has-kv_regex", token.line_col());
	  clause.attributes.push_back(key);
	  clause.attributes.push_back(get_text_token(token, error_output, "Value"));
	  clause.attributes.push_back(straight ? "" : "!");
	  clear_until_after(token, error_output, ",", "]", false);
	  if (*token == ",")
	  {
	    clause.statement = "has-kv_icase";
	    ++token;
	    clear_until_after(token, error_output, "i");
	    clear_until_after(token, error_output, "]", false);
	  }
	  ++token;
	  clauses.push_back(clause);
	}
      }
    }
    else if (*token == "(")
    {
      ++token;
      if (!token.good())
      {
	if (error_output)
	  error_output->add_parse_error("':' or '.' expected.", token.line_col().first);
	break;
      }

      subtrees.push_back(Token_Tree(token, error_output, true));
      clear_until_after(token, error_output, ")");
    }
    else
    {
      Statement_Text clause("item", token.line_col());
      clause.attributes.push_back(probe_from(token, error_output));
      clauses.push_back(clause);
    }
  }

  std::string into = probe_into(token, error_output);

  TStatement* statement = 0;
  if (clauses.empty() && subtrees.empty())
  {
    if (from == "")
    {
      if (parsed_query.get_global_bbox_limitation().valid())
      {
        statement = create_query_statement< TStatement >
            (stmt_factory, type, into, query_line_col.first);
      }
      else if (error_output)
	error_output->add_parse_error("An empty query is not allowed", token.line_col().first);
    }
    else
    {
      if (type == "")
        statement = create_item_statement< TStatement >(stmt_factory, from, into, query_line_col.first);
      else
      {
        statement = create_query_statement< TStatement >
           (stmt_factory, type, into, query_line_col.first);
        TStatement* substatement = create_item_statement< TStatement >(stmt_factory, from, "_", query_line_col.first);
        statement->add_statement(substatement, "");
      }
    }
  }
  else if (clauses.size() == 1 && from == "" && subtrees.empty())
  {
    if (clauses.front().statement == "has-kv"
       || clauses.front().statement == "has-kv_regex"
       || clauses.front().statement == "has-kv_keyregex"
       || clauses.front().statement == "has-kv_icase"
       || clauses.front().statement == "has-kv_keyregex_icase"
       || (clauses.front().statement == "bbox-query" && type != "node"))
    {
      statement = create_query_statement< TStatement >
          (stmt_factory, type, into, query_line_col.first);
      TStatement* substatement = create_query_substatement< TStatement >
          (stmt_factory, token, error_output, clauses.front(), type, from, "_");
      if (substatement)
	statement->add_statement(substatement, "");
    }
    else
    {
      statement = create_query_substatement< TStatement >
          (stmt_factory, token, error_output, clauses.front(), type, from, into);
    }
  }
  else if (clauses.empty() && from == "" && subtrees.size() == 1)
  {
    bool can_standalone = false;
    TStatement* filter = create_query_criterion< TStatement >(stmt_factory,
        Token_Node_Ptr(subtrees[0], subtrees[0].tree[0].rhs), error_output, type, can_standalone, into);
    if (filter)
    {
      if (can_standalone)
        statement = filter;
      else
      {
        statement = create_query_statement< TStatement >(stmt_factory, type, into, query_line_col.first);
        statement->add_statement(filter, "");
      }
    }
  }
  else
  {
    statement = create_query_statement< TStatement >(stmt_factory, type, into, query_line_col.first);
    if (!statement)
      return 0;

    if (from != "")
    {
      TStatement* substatement = create_item_statement< TStatement >
          (stmt_factory, from, "_", query_line_col.first);
      if (substatement)
	statement->add_statement(substatement, "");
    }

    for (std::vector< Statement_Text >::const_iterator it = clauses.begin();
        it != clauses.end(); ++it)
    {
      TStatement* substatement = create_query_substatement< TStatement >
          (stmt_factory, token, error_output, *it, type, from, "_");
      if (substatement)
	statement->add_statement(substatement, "");
    }
    for (typename std::vector< Token_Tree >::const_iterator it = subtrees.begin(); it != subtrees.end(); ++it)
    {
      bool can_standalone = false;
      TStatement* filter = create_query_criterion< TStatement >(stmt_factory,
          Token_Node_Ptr(*it, it->tree[0].rhs), error_output, type, can_standalone, "_");
      if (filter)
        statement->add_statement(filter, "");
    }
  }

  return statement;
}


template< class TStatement >
TStatement* parse_statement(typename TStatement::Factory& stmt_factory, Parsed_Query& parsed_query,
			    Tokenizer_Wrapper& token, Error_Output* error_output, int depth)
{
  if (!token.good())
    return 0;

  if (depth >= 1024)
  {
    if (error_output)
      error_output->add_parse_error("Nesting of statements is limited to 1023 levels", token.line_col().first);
    return 0;
  }

  if (*token == "(")
    return parse_union< TStatement >(stmt_factory, parsed_query, token, error_output, depth);
  else if (*token == "for")
    return parse_for< TStatement >(stmt_factory, parsed_query, token, error_output, depth);
  else if (*token == "foreach")
    return parse_foreach< TStatement >(stmt_factory, parsed_query, token, error_output, depth);
  else if (*token == "complete")
    return parse_complete< TStatement >(stmt_factory, parsed_query, token, error_output, depth);
  else if (*token == "if")
    return parse_if< TStatement >(stmt_factory, parsed_query, token, error_output, depth);
  else if (*token == "retro")
    return parse_retro< TStatement >(stmt_factory, parsed_query, token, error_output, depth);
  else if (*token == "timeline")
    return parse_timeline< TStatement >(stmt_factory, parsed_query, token, error_output, depth);

  std::string from = "";
  if (token.good() && *token == ".")
  {
    ++token;
    if (token.good())
    {
      from = *token;
      ++token;
    }
  }

  if (token.good() && *token == "out")
    return parse_output< TStatement >(stmt_factory, from, token, error_output);
  if (token.good() && *token == "convert")
    return parse_make< TStatement >(stmt_factory, from, token, error_output, "convert");
  if (token.good() && *token == "make")
    return parse_make< TStatement >(stmt_factory, from, token, error_output, "make");
  if (token.good() && (*token == "<" || *token == "<<" || *token == ">" || *token == ">>"))
    return parse_full_recurse< TStatement >(stmt_factory, token, from, error_output);
  if (token.good() && *token == "is_in")
    return parse_coord_query< TStatement >(stmt_factory, token, from, error_output);
  if (token.good() && *token == "map_to_area")
    return parse_map_to_area< TStatement >(stmt_factory, token, from, error_output);

  std::string type = "";
  if (*token != "out" && from == "")
  {
    type = *token;
    if (type == "rel")
      type = "relation";
    else if (type != "node" && type != "way" && type != "relation" && type != "area")
    {
      if (error_output)
	error_output->add_parse_error("Unknown type \"" + type + "\"", token.line_col().first);
    }
    ++token;
  }
  if (token.good() && *token == ".")
  {
    ++token;
    if (token.good())
    {
      from = *token;
      ++token;
    }
  }

  return parse_query< TStatement >(stmt_factory, parsed_query, type, from, token, error_output);
}


template< class TStatement >
void generic_parse_and_validate_map_ql
    (typename TStatement::Factory& stmt_factory,
     const std::string& xml_raw, Error_Output* error_output, std::vector< TStatement* >& stmt_seq, Parsed_Query& parsed_query)
{
  std::istringstream in(xml_raw);
  Tokenizer_Wrapper token(in);

  std::map< std::string, std::string > attr;
  while (token.good() && *token == "[")
  {
    std::vector< std::string > kv = parse_setup(token, error_output, parsed_query);
    if (kv.size() < 2)
      continue;
    if (kv[0] == "maxsize")
      kv[0] = "element-limit";
    else if (kv[0] == "out")
      kv[0] = "output";
    else if (kv[0] == "diff" || kv[0] == "adiff")
    {
      if (kv[0] == "adiff")
	attr["augmented"] = "deletions";
      if (kv.size() >= 3)
        attr["date"] = kv[2];
      kv[0] = "from";
    }
    attr[kv[0]] = kv[1];
    if (kv.size() == 4)
      attr[kv[2]] = kv[3];
  }

  TStatement* base_statement = stmt_factory.create_statement
      ("osm-script", token.line_col().first, attr);

  if (!attr.empty())
    clear_until_after(token, error_output, ";");

  while (token.good())
  {
    TStatement* statement = parse_statement< TStatement >(stmt_factory, parsed_query, token, error_output, 0);
    if (statement)
      base_statement->add_statement(statement, "");
    
    clear_until_after(token, error_output, ";");
  }

  stmt_seq.push_back(base_statement);
}


void parse_and_validate_map_ql
    (Statement::Factory& stmt_factory, const std::string& xml_raw, Error_Output* error_output, Parsed_Query& parsed_query)
{
  generic_parse_and_validate_map_ql< Statement >
      (stmt_factory, xml_raw, error_output, *get_statement_stack(), parsed_query);
}

void parse_and_dump_xml_from_map_ql
    (Statement::Factory& stmt_factory_, const std::string& xml_raw, Error_Output* error_output, Parsed_Query& parsed_query)
{
  Statement_Dump::Factory stmt_factory(stmt_factory_);
  std::vector< Statement_Dump* > stmt_seq;
  generic_parse_and_validate_map_ql< Statement_Dump >(stmt_factory, xml_raw, error_output, stmt_seq, parsed_query);
  for (std::vector< Statement_Dump* >::const_iterator it = stmt_seq.begin();
      it != stmt_seq.end(); ++it)
    std::cout<<(*it)->dump_xml();
  for (std::vector< Statement_Dump* >::iterator it = stmt_seq.begin();
      it != stmt_seq.end(); ++it)
    delete *it;
}

void parse_and_dump_compact_from_map_ql
    (Statement::Factory& stmt_factory_, const std::string& xml_raw, Error_Output* error_output, Parsed_Query& parsed_query)
{
  Statement_Dump::Factory stmt_factory(stmt_factory_);
  std::vector< Statement_Dump* > stmt_seq;
  generic_parse_and_validate_map_ql< Statement_Dump >(stmt_factory, xml_raw, error_output, stmt_seq, parsed_query);
  for (std::vector< Statement_Dump* >::const_iterator it = stmt_seq.begin();
      it != stmt_seq.end(); ++it)
    std::cout<<(*it)->dump_compact_map_ql(stmt_factory_)<<'\n';
  for (std::vector< Statement_Dump* >::iterator it = stmt_seq.begin();
      it != stmt_seq.end(); ++it)
    delete *it;
}

void parse_and_dump_bbox_from_map_ql
    (Statement::Factory& stmt_factory_, const std::string& xml_raw, Error_Output* error_output, Parsed_Query& parsed_query)
{
  Statement_Dump::Factory stmt_factory(stmt_factory_);
  std::vector< Statement_Dump* > stmt_seq;
  generic_parse_and_validate_map_ql< Statement_Dump >(stmt_factory, xml_raw, error_output, stmt_seq, parsed_query);
  for (std::vector< Statement_Dump* >::const_iterator it = stmt_seq.begin();
      it != stmt_seq.end(); ++it)
    std::cout<<(*it)->dump_bbox_map_ql(stmt_factory_)<<'\n';
  for (std::vector< Statement_Dump* >::iterator it = stmt_seq.begin();
      it != stmt_seq.end(); ++it)
    delete *it;
}

void parse_and_dump_pretty_from_map_ql
    (Statement::Factory& stmt_factory_, const std::string& xml_raw, Error_Output* error_output, Parsed_Query& parsed_query)
{
  Statement_Dump::Factory stmt_factory(stmt_factory_);
  std::vector< Statement_Dump* > stmt_seq;
  generic_parse_and_validate_map_ql< Statement_Dump >(stmt_factory, xml_raw, error_output, stmt_seq, parsed_query);
  for (std::vector< Statement_Dump* >::const_iterator it = stmt_seq.begin();
      it != stmt_seq.end(); ++it)
    std::cout<<(*it)->dump_pretty_map_ql(stmt_factory_);
  for (std::vector< Statement_Dump* >::iterator it = stmt_seq.begin();
      it != stmt_seq.end(); ++it)
    delete *it;
}
