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

#ifndef DE__OSM3S___OVERPASS_API__STATEMENTS__TAG_VALUE_H
#define DE__OSM3S___OVERPASS_API__STATEMENTS__TAG_VALUE_H


#include "../../expat/escape_json.h"
#include "../../expat/escape_xml.h"
#include "../data/tag_store.h"
#include "../data/utils.h"
#include "evaluator.h"
#include "statement.h"

#include <map>
#include <string>
#include <vector>


/* === Fixed Value evaluator ===

This operator always returns a fixed value.
It does not take any argument.

Its syntax is

  <Value>
*/

class Evaluator_Fixed final : public Evaluator
{
public:
  struct Statement_Maker : public Generic_Statement_Maker< Evaluator_Fixed >
  {
    Statement_Maker() : Generic_Statement_Maker< Evaluator_Fixed >("eval-fixed") {}
  };
  static Statement_Maker statement_maker;

  struct Evaluator_Maker : public Statement::Evaluator_Maker
  {
    Statement* create_evaluator(const Token_Node_Ptr& tree_it, QL_Context tree_context,
        Statement::Factory& stmt_factory, Parsed_Query& global_settings, Error_Output* error_output) override;
    Evaluator_Maker() { Statement::maker_by_token()[""].push_back(this); }
  };
  static Evaluator_Maker evaluator_maker;

  std::string dump_xml(const std::string& indent) const override
  { return indent + "<eval-fixed v=\"" + escape_xml(value) + "\"/>\n"; }
  std::string dump_compact_ql(const std::string&) const override;

  Evaluator_Fixed(int line_number_, const std::map< std::string, std::string >& input_attributes,
                   Parsed_Query& global_settings);
  std::string get_name() const override { return "eval-fixed"; }
  std::string get_result_name() const override { return ""; }
  void execute(Resource_Manager& rman) override {}
  ~Evaluator_Fixed() override {}

  Requested_Context request_context() const override { return Requested_Context(); }

  Statement::Eval_Return_Type return_type() const override { return Statement::string; };
  Eval_Task* get_string_task(Prepare_Task_Context& context, const std::string* key) override
  { return new Const_Eval_Task(value); }

private:
  std::string value;
};


/* === Element Dependend Operators ===

Element dependend operators depend on one or no parameter.
Their syntax varies,
but most have the apppearance of a function name plus parentheses.

They can only be called in a context where elements are processed.
This applies to convert, to filter, or to arguments of aggregate functions.
They cannot be called directly from make.


==== Id and Type of the Element ====

The operator <em>id</em> returns the id of the element.
The operator <em>type</em> returns the type of the element.
Both operators take no parameters.

The syntax is

  id()

resp.

  type()
*/

struct Id_Eval_Task final : public Eval_Task
{
  std::string eval(const std::string* key) const override { return ""; }

  std::string eval(const Element_With_Context< Node_Skeleton >& data, const std::string* key) const override
      { return data.object ? to_string(data.object->id.val()) : ""; }
  std::string eval(const Element_With_Context< Attic< Node_Skeleton > >& data, const std::string* key) const override
      { return data.object ? to_string(data.object->id.val()) : ""; }
  std::string eval(const Element_With_Context< Way_Skeleton >& data, const std::string* key) const override
      { return data.object ? to_string(data.object->id.val()) : ""; }
  std::string eval(const Element_With_Context< Attic< Way_Skeleton > >& data, const std::string* key) const override
      { return data.object ? to_string(data.object->id.val()) : ""; }
  std::string eval(const Element_With_Context< Relation_Skeleton >& data, const std::string* key) const override
      { return data.object ? to_string(data.object->id.val()) : ""; }
  std::string eval(const Element_With_Context< Attic< Relation_Skeleton > >& data, const std::string* key) const override
      { return data.object ? to_string(data.object->id.val()) : ""; }
  std::string eval(const Element_With_Context< Area_Skeleton >& data, const std::string* key) const override
      { return data.object ? to_string(data.object->id.val()) : ""; }
  std::string eval(const Element_With_Context< Derived_Skeleton >& data, const std::string* key) const override
      { return data.object ? to_string(data.object->id.val()) : ""; }
};


class Evaluator_Id final : public Evaluator
{
public:
  struct Statement_Maker : public Generic_Statement_Maker< Evaluator_Id >
  {
    Statement_Maker() : Generic_Statement_Maker< Evaluator_Id >("eval-id") {}
  };
  static Statement_Maker statement_maker;
  static Element_Function_Maker< Evaluator_Id > evaluator_maker;

  static std::string stmt_func_name() { return "id"; }
  std::string dump_xml(const std::string& indent) const override
  { return indent + "<eval-id/>\n"; }
  std::string dump_compact_ql(const std::string&) const override { return "id()"; }

  Evaluator_Id(int line_number_, const std::map< std::string, std::string >& input_attributes,
                   Parsed_Query& global_settings);
  std::string get_name() const override { return "eval-id"; }
  std::string get_result_name() const override { return ""; }
  void execute(Resource_Manager& rman) override {}
  ~Evaluator_Id() override {}

  Requested_Context request_context() const override { return Requested_Context().add_usage(Set_Usage::SKELETON); }

  Statement::Eval_Return_Type return_type() const override { return Statement::string; };
  Eval_Task* get_string_task(Prepare_Task_Context& context, const std::string* key) override { return new Id_Eval_Task(); }
};


struct Type_Eval_Task final : public Eval_Task
{
  std::string eval(const std::string* key) const override { return ""; }

  std::string eval(const Element_With_Context< Node_Skeleton >& data, const std::string* key) const override
      { return "node"; }
  std::string eval(const Element_With_Context< Attic< Node_Skeleton > >& data, const std::string* key) const override
      { return "node"; }
  std::string eval(const Element_With_Context< Way_Skeleton >& data, const std::string* key) const override
      { return "way"; }
  std::string eval(const Element_With_Context< Attic< Way_Skeleton > >& data, const std::string* key) const override
      { return "way"; }
  std::string eval(const Element_With_Context< Relation_Skeleton >& data, const std::string* key) const override
      { return "relation"; }
  std::string eval(const Element_With_Context< Attic< Relation_Skeleton > >& data, const std::string* key) const override
      { return "relation"; }
  std::string eval(const Element_With_Context< Area_Skeleton >& data, const std::string* key) const override
      { return "area"; }
  std::string eval(const Element_With_Context< Derived_Skeleton >& data, const std::string* key) const override
      { return data.object ? data.object->type_name : ""; }
};


class Evaluator_Type final : public Evaluator
{
public:
  struct Statement_Maker : public Generic_Statement_Maker< Evaluator_Type >
  {
    Statement_Maker() : Generic_Statement_Maker< Evaluator_Type >("eval-type") {}
  };
  static Statement_Maker statement_maker;
  static Element_Function_Maker< Evaluator_Type > evaluator_maker;

  static std::string stmt_func_name() { return "type"; }
  std::string dump_xml(const std::string& indent) const override { return indent + "<eval-type/>\n"; }
  std::string dump_compact_ql(const std::string&) const override { return "type()"; }

  Evaluator_Type(int line_number_, const std::map< std::string, std::string >& input_attributes,
                   Parsed_Query& global_settings);
  std::string get_name() const override { return "eval-type"; }
  std::string get_result_name() const override { return ""; }
  void execute(Resource_Manager& rman) override {}
  ~Evaluator_Type() override {}

  Requested_Context request_context() const override { return Requested_Context().add_usage(Set_Usage::SKELETON); }

  Statement::Eval_Return_Type return_type() const override { return Statement::string; };
  Eval_Task* get_string_task(Prepare_Task_Context& context, const std::string* key) override
  { return new Type_Eval_Task(); }
};


/* === Tag Value and Generic Value Operators ===

The tag operator returns the value of the tag of the given key.
The <em>is_tag</em> operator returns "1" if the given element has a tag with this key and "0" otherwise.
They only can be called in the context of an element.

Their syntax is

  t[<Key name>]

resp.

  is_tag(<Key name >)

The <Key name> must be in quotation marks if it contains special characters.

The tag operator can also be called with a substatement. Its syntax is then

  t[<Substatement>]

The generic tag operator returns the value of the tag of the key it is called for.
It only can be called in the context of an element.
In addition, it must be part of the value of a generic property to have its key specified.

Its syntax is

  ::
*/

std::string find_value(const std::vector< std::pair< std::string, std::string > >* tags, const std::string& key);


struct Value_Eval_Task final : public Eval_Task
{
  Value_Eval_Task(Eval_Task* rhs_) : rhs(rhs_) {}
  ~Value_Eval_Task() override { delete rhs; }

  std::string eval(const std::string* key) const override;

  std::string eval(const Element_With_Context< Node_Skeleton >& data, const std::string* key) const override;
  std::string eval(const Element_With_Context< Attic< Node_Skeleton > >& data, const std::string* key) const override;
  std::string eval(const Element_With_Context< Way_Skeleton >& data, const std::string* key) const override;
  std::string eval(const Element_With_Context< Attic< Way_Skeleton > >& data, const std::string* key) const override;
  std::string eval(const Element_With_Context< Relation_Skeleton >& data, const std::string* key) const override;
  std::string eval(const Element_With_Context< Attic< Relation_Skeleton > >& data, const std::string* key) const override;
  std::string eval(const Element_With_Context< Area_Skeleton >& data, const std::string* key) const override;
  std::string eval(const Element_With_Context< Derived_Skeleton >& data, const std::string* key) const override;

private:
  Eval_Task* rhs;
};


class Evaluator_Value final : public Evaluator
{
public:
  struct Statement_Maker : public Generic_Statement_Maker< Evaluator_Value >
  {
    Statement_Maker() : Generic_Statement_Maker< Evaluator_Value >("eval-value") {}
  };
  static Statement_Maker statement_maker;

  struct Evaluator_Maker : public Statement::Evaluator_Maker
  {
    Statement* create_evaluator(const Token_Node_Ptr& tree_it, QL_Context tree_context,
        Statement::Factory& stmt_factory, Parsed_Query& global_settings, Error_Output* error_output) override;
    Evaluator_Maker() { Statement::maker_by_token()["["].push_back(this); }
  };
  static Evaluator_Maker evaluator_maker;

  std::string dump_xml(const std::string& indent) const override
  {
    return indent + "<eval-value>\n"
        + (rhs ? rhs->dump_xml(indent + "  ") : "")
        + indent + "</eval-value>\n";
  }
  std::string dump_compact_ql(const std::string&) const override
  { return std::string("t[\"") + (rhs ? rhs->dump_compact_ql("") : "") + "\"]"; }

  Evaluator_Value(int line_number_, const std::map< std::string, std::string >& input_attributes,
                   Parsed_Query& global_settings);
  void add_statement(Statement* statement, std::string text) override;
  std::string get_name() const override { return "eval-value"; }
  std::string get_result_name() const override { return ""; }
  void execute(Resource_Manager& rman) override {}
  ~Evaluator_Value() override {}

  Requested_Context request_context() const override;

  Statement::Eval_Return_Type return_type() const override { return Statement::string; };
  Eval_Task* get_string_task(Prepare_Task_Context& context, const std::string* target_key) override;

private:
  Evaluator* rhs;
};


std::string exists_value(const std::vector< std::pair< std::string, std::string > >* tags, const std::string& key);


struct Is_Tag_Eval_Task final : public Eval_Task
{
  Is_Tag_Eval_Task(const std::string& key_) : key(key_) {}

  std::string eval(const std::string* key) const override { return ""; }

  std::string eval(const Element_With_Context< Node_Skeleton >& data, const std::string* key) const override
      { return exists_value(data.tags, this->key); }
  std::string eval(const Element_With_Context< Attic< Node_Skeleton > >& data, const std::string* key) const override
      { return exists_value(data.tags, this->key); }
  std::string eval(const Element_With_Context< Way_Skeleton >& data, const std::string* key) const override
      { return exists_value(data.tags, this->key); }
  std::string eval(const Element_With_Context< Attic< Way_Skeleton > >& data, const std::string* key) const override
      { return exists_value(data.tags, this->key); }
  std::string eval(const Element_With_Context< Relation_Skeleton >& data, const std::string* key) const override
      { return exists_value(data.tags, this->key); }
  std::string eval(const Element_With_Context< Attic< Relation_Skeleton > >& data, const std::string* key) const override
      { return exists_value(data.tags, this->key); }
  std::string eval(const Element_With_Context< Area_Skeleton >& data, const std::string* key) const override
      { return exists_value(data.tags, this->key); }
  std::string eval(const Element_With_Context< Derived_Skeleton >& data, const std::string* key) const override
      { return exists_value(data.tags, this->key); }

private:
  std::string key;
};


class Evaluator_Is_Tag final : public Evaluator
{
public:
  struct Statement_Maker : public Generic_Statement_Maker< Evaluator_Is_Tag >
  {
    Statement_Maker() : Generic_Statement_Maker< Evaluator_Is_Tag >("eval-is-tag") {}
  };
  static Statement_Maker statement_maker;

  struct Evaluator_Maker : public Statement::Evaluator_Maker
  {
    Statement* create_evaluator(const Token_Node_Ptr& tree_it, QL_Context tree_context,
        Statement::Factory& stmt_factory, Parsed_Query& global_settings, Error_Output* error_output) override;
    Evaluator_Maker() { Statement::maker_by_func_name()["is_tag"].push_back(this); }
  };
  static Evaluator_Maker evaluator_maker;

  std::string dump_xml(const std::string& indent) const override
  { return indent + "<eval-is-tag k=\"" + escape_xml(key) + "\"/>\n"; }
  std::string dump_compact_ql(const std::string&) const override
  { return std::string("is_tag(\"") + escape_cstr(key) + "\")"; }

  Evaluator_Is_Tag(int line_number_, const std::map< std::string, std::string >& input_attributes,
                   Parsed_Query& global_settings);
  std::string get_name() const override { return "eval-is-tag"; }
  std::string get_result_name() const override { return ""; }
  void execute(Resource_Manager& rman) override {}
  ~Evaluator_Is_Tag() override {}

  Requested_Context request_context() const override { return Requested_Context().add_usage(Set_Usage::TAGS); }

  Statement::Eval_Return_Type return_type() const override { return Statement::string; };
  Eval_Task* get_string_task(Prepare_Task_Context& context, const std::string* target_key) override
  { return new Is_Tag_Eval_Task(key); }

private:
  std::string key;
};


struct Generic_Eval_Task final : public Eval_Task
{
  std::string eval(const std::string* key) const override { return ""; }

  std::string eval(const Element_With_Context< Node_Skeleton >& data, const std::string* key) const override
      { return key ? find_value(data.tags, *key) : ""; }
  std::string eval(const Element_With_Context< Attic< Node_Skeleton > >& data, const std::string* key) const override
      { return key ? find_value(data.tags, *key) : ""; }
  std::string eval(const Element_With_Context< Way_Skeleton >& data, const std::string* key) const override
      { return key ? find_value(data.tags, *key) : ""; }
  std::string eval(const Element_With_Context< Attic< Way_Skeleton > >& data, const std::string* key) const override
      { return key ? find_value(data.tags, *key) : ""; }
  std::string eval(const Element_With_Context< Relation_Skeleton >& data, const std::string* key) const override
      { return key ? find_value(data.tags, *key) : ""; }
  std::string eval(const Element_With_Context< Attic< Relation_Skeleton > >& data, const std::string* key) const override
      { return key ? find_value(data.tags, *key) : ""; }
  std::string eval(const Element_With_Context< Area_Skeleton >& data, const std::string* key) const override
      { return key ? find_value(data.tags, *key) : ""; }
  std::string eval(const Element_With_Context< Derived_Skeleton >& data, const std::string* key) const override
      { return key ? find_value(data.tags, *key) : ""; }
};


class Evaluator_Generic final : public Evaluator
{
public:
  struct Statement_Maker : public Generic_Statement_Maker< Evaluator_Generic >
  {
    Statement_Maker() : Generic_Statement_Maker< Evaluator_Generic >("eval-generic") {}
  };
  static Statement_Maker statement_maker;

  struct Evaluator_Maker : public Statement::Evaluator_Maker
  {
    Statement* create_evaluator(const Token_Node_Ptr& tree_it, QL_Context tree_context,
        Statement::Factory& stmt_factory, Parsed_Query& global_settings, Error_Output* error_output) override;
    Evaluator_Maker() { Statement::maker_by_token()["::"].push_back(this); }
  };
  static Evaluator_Maker evaluator_maker;

  std::string dump_xml(const std::string& indent) const override
  { return indent + "<eval-generic/>\n"; }
  std::string dump_compact_ql(const std::string&) const override { return "::"; }

  Evaluator_Generic(int line_number_, const std::map< std::string, std::string >& input_attributes,
                   Parsed_Query& global_settings);
  std::string get_name() const override { return "eval-generic"; }
  std::string get_result_name() const override { return ""; }
  void execute(Resource_Manager& rman) override {}
  ~Evaluator_Generic() override {}

  Requested_Context request_context() const override { return Requested_Context().add_usage(Set_Usage::TAGS); }

  Statement::Eval_Return_Type return_type() const override { return Statement::string; };
  Eval_Task* get_string_task(Prepare_Task_Context& context, const std::string* key) override
  { return new Generic_Eval_Task(); }
};


/* === All Keys Evaluator ===

The all keys evaluator returns a container of the keys of the given element.

Its syntax is

  keys()
*/


std::vector< std::string > all_keys(const std::vector< std::pair< std::string, std::string > >* tags);


struct All_Keys_Eval_Task final : public Eval_Container_Task
{
  std::vector< std::string > eval(const std::string* key) const override { return std::vector< std::string >(); }

  std::vector< std::string > eval(
      const Element_With_Context< Node_Skeleton >& data, const std::string* key) const override
      { return all_keys(data.tags); }
  std::vector< std::string > eval(
      const Element_With_Context< Attic< Node_Skeleton > >& data, const std::string* key) const override
      { return all_keys(data.tags); }
  std::vector< std::string > eval(
      const Element_With_Context< Way_Skeleton >& data, const std::string* key) const override
      { return all_keys(data.tags); }
  std::vector< std::string > eval(
      const Element_With_Context< Attic< Way_Skeleton > >& data, const std::string* key) const override
      { return all_keys(data.tags); }
  std::vector< std::string > eval(
      const Element_With_Context< Relation_Skeleton >& data, const std::string* key) const override
      { return all_keys(data.tags); }
  std::vector< std::string > eval(
      const Element_With_Context< Attic< Relation_Skeleton > >& data, const std::string* key) const override
      { return all_keys(data.tags); }
  std::vector< std::string > eval(
      const Element_With_Context< Area_Skeleton >& data, const std::string* key) const override
      { return all_keys(data.tags); }
  std::vector< std::string > eval(
      const Element_With_Context< Derived_Skeleton >& data, const std::string* key) const override
      { return all_keys(data.tags); }
};


class Evaluator_All_Keys final : public Evaluator
{
public:
  struct Statement_Maker : public Generic_Statement_Maker< Evaluator_All_Keys >
  {
    Statement_Maker() : Generic_Statement_Maker< Evaluator_All_Keys >("eval-all-keys") {}
  };
  static Statement_Maker statement_maker;
  static Element_Function_Maker< Evaluator_All_Keys > evaluator_maker;

  static std::string stmt_func_name() { return "keys"; }
  std::string dump_xml(const std::string& indent) const override
  { return indent + "<eval-all-keys/>\n"; }
  std::string dump_compact_ql(const std::string&) const override { return "keys()"; }

  Evaluator_All_Keys(int line_number_, const std::map< std::string, std::string >& input_attributes,
                   Parsed_Query& global_settings);
  std::string get_name() const override { return "eval-all-keys"; }
  std::string get_result_name() const override { return ""; }
  void execute(Resource_Manager& rman) override {}
  ~Evaluator_All_Keys() override {}

  Requested_Context request_context() const override { return Requested_Context().add_usage(Set_Usage::TAGS); }

  Statement::Eval_Return_Type return_type() const override { return Statement::container; };
  Eval_Task* get_string_task(Prepare_Task_Context& context, const std::string* target_key) override { return 0; }
  Eval_Container_Task* get_container_task(Prepare_Task_Context& context, const std::string* key) override
  { return new All_Keys_Eval_Task(); }

private:
  std::string key;
};


/* === Meta Data Operators ===

The <em>version</em> operator returns the version number of the given element.
Its syntax is:

  version()

The <em>timestamp</em> operator returns the timestamp of the given element.
Its syntax is:

  timestamp()

The <em>changeset</em> operator returns the changeset id of the changeset
in which the given element has been last edited.
Its syntax is:

  changeset()

The <em>uid</em> operator returns the id of the user
that has last touched the given element.
Its syntax is:

  uid()

The <em>uid</em> operator returns the name of the user
that has last touched the given element.
Its syntax is:

  user()
*/

struct Version_Eval_Task final : public Eval_Task
{
  Version_Eval_Task() {}

  std::string eval(const std::string* key) const override { return ""; }

  std::string eval(const Element_With_Context< Node_Skeleton >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->version) : ""; }
  std::string eval(const Element_With_Context< Attic< Node_Skeleton > >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->version) : ""; }
  std::string eval(const Element_With_Context< Way_Skeleton >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->version) : ""; }
  std::string eval(const Element_With_Context< Attic< Way_Skeleton > >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->version) : ""; }
  std::string eval(const Element_With_Context< Relation_Skeleton >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->version) : ""; }
  std::string eval(const Element_With_Context< Attic< Relation_Skeleton > >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->version) : ""; }
  std::string eval(const Element_With_Context< Area_Skeleton >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->version) : ""; }
  std::string eval(const Element_With_Context< Derived_Skeleton >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->version) : ""; }
};


class Evaluator_Version final : public Evaluator
{
public:
  struct Statement_Maker : public Generic_Statement_Maker< Evaluator_Version >
  {
    Statement_Maker() : Generic_Statement_Maker< Evaluator_Version >("eval-version") {}
  };
  static Statement_Maker statement_maker;
  static Element_Function_Maker< Evaluator_Version > evaluator_maker;

  static std::string stmt_func_name() { return "version"; }
  std::string dump_xml(const std::string& indent) const override
  { return indent + "<eval-version/>\n"; }
  std::string dump_compact_ql(const std::string&) const override
  { return "version(\"\")"; }

  Evaluator_Version(int line_number_, const std::map< std::string, std::string >& input_attributes,
                   Parsed_Query& global_settings);
  std::string get_name() const override { return "eval-version"; }
  std::string get_result_name() const override { return ""; }
  void execute(Resource_Manager& rman) override {}
  ~Evaluator_Version() override {}

  Requested_Context request_context() const override { return Requested_Context().add_usage(Set_Usage::META); }

  Statement::Eval_Return_Type return_type() const override { return Statement::string; };
  Eval_Task* get_string_task(Prepare_Task_Context& context, const std::string* key) override
  { return new Version_Eval_Task(); }
};


struct Timestamp_Eval_Task final : public Eval_Task
{
  Timestamp_Eval_Task() {}

  std::string eval(const std::string* key) const override { return ""; }

  std::string eval(const Element_With_Context< Node_Skeleton >& data, const std::string* key) const override
      { return data.meta ? Timestamp(data.meta->timestamp).str() : ""; }
  std::string eval(const Element_With_Context< Attic< Node_Skeleton > >& data, const std::string* key) const override
      { return data.meta ? Timestamp(data.meta->timestamp).str() : ""; }
  std::string eval(const Element_With_Context< Way_Skeleton >& data, const std::string* key) const override
      { return data.meta ? Timestamp(data.meta->timestamp).str() : ""; }
  std::string eval(const Element_With_Context< Attic< Way_Skeleton > >& data, const std::string* key) const override
      { return data.meta ? Timestamp(data.meta->timestamp).str() : ""; }
  std::string eval(const Element_With_Context< Relation_Skeleton >& data, const std::string* key) const override
      { return data.meta ? Timestamp(data.meta->timestamp).str() : ""; }
  std::string eval(const Element_With_Context< Attic< Relation_Skeleton > >& data, const std::string* key) const override
      { return data.meta ? Timestamp(data.meta->timestamp).str() : ""; }
  std::string eval(const Element_With_Context< Area_Skeleton >& data, const std::string* key) const override
      { return data.meta ? Timestamp(data.meta->timestamp).str() : ""; }
  std::string eval(const Element_With_Context< Derived_Skeleton >& data, const std::string* key) const override
      { return data.meta ? Timestamp(data.meta->timestamp).str() : ""; }
};


class Evaluator_Timestamp final : public Evaluator
{
public:
  struct Statement_Maker : public Generic_Statement_Maker< Evaluator_Timestamp >
  {
    Statement_Maker() : Generic_Statement_Maker< Evaluator_Timestamp >("eval-timestamp") {}
  };
  static Statement_Maker statement_maker;
  static Element_Function_Maker< Evaluator_Timestamp > evaluator_maker;

  static std::string stmt_func_name() { return "timestamp"; }
  std::string dump_xml(const std::string& indent) const override
  { return indent + "<eval-timestamp/>\n"; }
  std::string dump_compact_ql(const std::string&) const override
  { return "timestamp(\"\")"; }

  Evaluator_Timestamp(int line_number_, const std::map< std::string, std::string >& input_attributes,
                   Parsed_Query& global_settings);
  std::string get_name() const override { return "eval-timestamp"; }
  std::string get_result_name() const override { return ""; }
  void execute(Resource_Manager& rman) override {}
  ~Evaluator_Timestamp() override {}

  Requested_Context request_context() const override { return Requested_Context().add_usage(Set_Usage::META); }

  Statement::Eval_Return_Type return_type() const override { return Statement::string; };
  Eval_Task* get_string_task(Prepare_Task_Context& context, const std::string* key) override
  { return new Timestamp_Eval_Task(); }
};


struct Changeset_Eval_Task final : public Eval_Task
{
  Changeset_Eval_Task() {}

  std::string eval(const std::string* key) const override { return ""; }

  std::string eval(const Element_With_Context< Node_Skeleton >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->changeset) : ""; }
  std::string eval(const Element_With_Context< Attic< Node_Skeleton > >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->changeset) : ""; }
  std::string eval(const Element_With_Context< Way_Skeleton >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->changeset) : ""; }
  std::string eval(const Element_With_Context< Attic< Way_Skeleton > >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->changeset) : ""; }
  std::string eval(const Element_With_Context< Relation_Skeleton >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->changeset) : ""; }
  std::string eval(const Element_With_Context< Attic< Relation_Skeleton > >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->changeset) : ""; }
  std::string eval(const Element_With_Context< Area_Skeleton >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->changeset) : ""; }
  std::string eval(const Element_With_Context< Derived_Skeleton >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->changeset) : ""; }
};


class Evaluator_Changeset final : public Evaluator
{
public:
  struct Statement_Maker : public Generic_Statement_Maker< Evaluator_Changeset >
  {
    Statement_Maker() : Generic_Statement_Maker< Evaluator_Changeset >("eval-changeset") {}
  };
  static Statement_Maker statement_maker;
  static Element_Function_Maker< Evaluator_Changeset > evaluator_maker;

  static std::string stmt_func_name() { return "changeset"; }
  std::string dump_xml(const std::string& indent) const override
  { return indent + "<eval-changeset/>\n"; }
  std::string dump_compact_ql(const std::string&) const override
  { return "changeset(\"\")"; }

  Evaluator_Changeset(int line_number_, const std::map< std::string, std::string >& input_attributes,
                   Parsed_Query& global_settings);
  std::string get_name() const override { return "eval-changeset"; }
  std::string get_result_name() const override { return ""; }
  void execute(Resource_Manager& rman) override {}
  ~Evaluator_Changeset() override {}

  Requested_Context request_context() const override { return Requested_Context().add_usage(Set_Usage::META); }

  Statement::Eval_Return_Type return_type() const override { return Statement::string; };
  Eval_Task* get_string_task(Prepare_Task_Context& context, const std::string* key) override
  { return new Changeset_Eval_Task(); }
};


struct Uid_Eval_Task final : public Eval_Task
{
  Uid_Eval_Task() {}

  std::string eval(const std::string* key) const override { return ""; }

  std::string eval(const Element_With_Context< Node_Skeleton >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->user_id) : ""; }
  std::string eval(const Element_With_Context< Attic< Node_Skeleton > >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->user_id) : ""; }
  std::string eval(const Element_With_Context< Way_Skeleton >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->user_id) : ""; }
  std::string eval(const Element_With_Context< Attic< Way_Skeleton > >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->user_id) : ""; }
  std::string eval(const Element_With_Context< Relation_Skeleton >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->user_id) : ""; }
  std::string eval(const Element_With_Context< Attic< Relation_Skeleton > >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->user_id) : ""; }
  std::string eval(const Element_With_Context< Area_Skeleton >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->user_id) : ""; }
  std::string eval(const Element_With_Context< Derived_Skeleton >& data, const std::string* key) const override
      { return data.meta ? to_string(data.meta->user_id) : ""; }
};


class Evaluator_Uid final : public Evaluator
{
public:
  struct Statement_Maker : public Generic_Statement_Maker< Evaluator_Uid >
  {
    Statement_Maker() : Generic_Statement_Maker< Evaluator_Uid >("eval-uid") {}
  };
  static Statement_Maker statement_maker;
  static Element_Function_Maker< Evaluator_Uid > evaluator_maker;

  static std::string stmt_func_name() { return "uid"; }
  std::string dump_xml(const std::string& indent) const override
  { return indent + "<eval-uid/>\n"; }
  std::string dump_compact_ql(const std::string&) const override
  { return "uid(\"\")"; }

  Evaluator_Uid(int line_number_, const std::map< std::string, std::string >& input_attributes,
                   Parsed_Query& global_settings);
  std::string get_name() const override { return "eval-uid"; }
  std::string get_result_name() const override { return ""; }
  void execute(Resource_Manager& rman) override {}
  ~Evaluator_Uid() override {}

  Requested_Context request_context() const override { return Requested_Context().add_usage(Set_Usage::META); }

  Statement::Eval_Return_Type return_type() const override { return Statement::string; };
  Eval_Task* get_string_task(Prepare_Task_Context& context, const std::string* key) override
  { return new Uid_Eval_Task(); }
};


struct User_Eval_Task final : public Eval_Task
{
  User_Eval_Task() {}

  std::string eval(const std::string* key) const override { return ""; }

  std::string eval(const Element_With_Context< Node_Skeleton >& data, const std::string* key) const override
      { return data.user_name ? *data.user_name : ""; }
  std::string eval(const Element_With_Context< Attic< Node_Skeleton > >& data, const std::string* key) const override
      { return data.user_name ? *data.user_name : ""; }
  std::string eval(const Element_With_Context< Way_Skeleton >& data, const std::string* key) const override
      { return data.user_name ? *data.user_name : ""; }
  std::string eval(const Element_With_Context< Attic< Way_Skeleton > >& data, const std::string* key) const override
      { return data.user_name ? *data.user_name : ""; }
  std::string eval(const Element_With_Context< Relation_Skeleton >& data, const std::string* key) const override
      { return data.user_name ? *data.user_name : ""; }
  std::string eval(const Element_With_Context< Attic< Relation_Skeleton > >& data, const std::string* key) const override
      { return data.user_name ? *data.user_name : ""; }
  std::string eval(const Element_With_Context< Area_Skeleton >& data, const std::string* key) const override
      { return data.user_name ? *data.user_name : ""; }
  std::string eval(const Element_With_Context< Derived_Skeleton >& data, const std::string* key) const override
      { return data.user_name ? *data.user_name : ""; }
};


class Evaluator_User final : public Evaluator
{
public:
  struct Statement_Maker : public Generic_Statement_Maker< Evaluator_User >
  {
    Statement_Maker() : Generic_Statement_Maker< Evaluator_User >("eval-user") {}
  };
  static Statement_Maker statement_maker;
  static Element_Function_Maker< Evaluator_User > evaluator_maker;

  static std::string stmt_func_name() { return "user"; }
  std::string dump_xml(const std::string& indent) const override
  { return indent + "<eval-user/>\n"; }
  std::string dump_compact_ql(const std::string&) const override
  { return "user(\"\")"; }

  Evaluator_User(int line_number_, const std::map< std::string, std::string >& input_attributes,
                   Parsed_Query& global_settings);
  std::string get_name() const override { return "eval-user"; }
  std::string get_result_name() const override { return ""; }
  void execute(Resource_Manager& rman) override {}
  ~Evaluator_User() override {}

  Requested_Context request_context() const override
  { return Requested_Context().add_usage(Set_Usage::META).add_user_names(); }

  Statement::Eval_Return_Type return_type() const override { return Statement::string; };
  Eval_Task* get_string_task(Prepare_Task_Context& context, const std::string* key) override
  { return new User_Eval_Task(); }
};


/* === Element Properties Count ===

This variant of the <em>count</em> operator counts tags or members
of the given element.
Opposed to the statistical variant of the count operator,
they cannot take an input set as extra argument.

The syntax for tags is

  count_tags()

The syntax to count the number of member entries is

  count_members()

The syntax to count the number of distinct members is

  count_distinct_members()

The syntax to count the number of member entries with a specific role is

  count_by_role()

The syntax to count the number of distinct members with a specific role is

  count_distinct_by_role()
*/

class Evaluator_Properties_Count final : public Evaluator
{
public:
  enum Objects { nothing, tags, members, distinct_members, by_role, distinct_by_role };
  static std::string to_string(Objects objects);
  enum Members_Type { all, nodes, ways, relations };
  static std::string to_string(Members_Type types);

  struct Statement_Maker : public Generic_Statement_Maker< Evaluator_Properties_Count >
  {
    Statement_Maker() : Generic_Statement_Maker< Evaluator_Properties_Count >("eval-prop-count") {}
  };
  static Statement_Maker statement_maker;

  struct Evaluator_Maker : public Statement::Evaluator_Maker
  {
    Statement* create_evaluator(const Token_Node_Ptr& tree_it, QL_Context tree_context,
        Statement::Factory& stmt_factory, Parsed_Query& global_settings, Error_Output* error_output) override;
    Evaluator_Maker()
    {
      Statement::maker_by_func_name()["count_tags"].push_back(this);
      Statement::maker_by_func_name()["count_members"].push_back(this);
      Statement::maker_by_func_name()["count_distinct_members"].push_back(this);
      Statement::maker_by_func_name()["count_by_role"].push_back(this);
      Statement::maker_by_func_name()["count_distinct_by_role"].push_back(this);
    }
  };
  static Evaluator_Maker evaluator_maker;

  std::string dump_xml(const std::string& indent) const override
  {
    return indent + "<eval-prop-count type=\"" + to_string(to_count) + "\""
        + (to_count == by_role || to_count == distinct_by_role ?
            std::string(" role=\"") + escape_xml(role) + "\"" : std::string(""))
        + (type_to_count != all ?
            std::string(" members_type=\"") + to_string(type_to_count) + "\"" : std::string("")) + "/>\n";
  }
  std::string dump_compact_ql(const std::string&) const override
  {
    return std::string("count_") + to_string(to_count) + "("
        + (to_count == by_role || to_count == distinct_by_role ?
            std::string("\"") + escape_cstr(role) + "\""
            + (type_to_count != all ? "," : "") : std::string(""))
        + (type_to_count != all ? to_string(type_to_count) : std::string("")) + ")";
  }

  Evaluator_Properties_Count(int line_number_, const std::map< std::string, std::string >& input_attributes,
                   Parsed_Query& global_settings);
  std::string get_name() const override { return "eval-prop-count"; }
  std::string get_result_name() const override { return ""; }
  void execute(Resource_Manager& rman) override {}
  ~Evaluator_Properties_Count() override {}

  Requested_Context request_context() const override;

  Statement::Eval_Return_Type return_type() const override { return Statement::string; };
  Eval_Task* get_string_task(Prepare_Task_Context& context, const std::string* key) override;

private:
  Objects to_count;
  Members_Type type_to_count;
  std::string role;
};


struct Prop_Count_Eval_Task final : public Eval_Task
{
  Prop_Count_Eval_Task(
      Evaluator_Properties_Count::Objects to_count_, Evaluator_Properties_Count::Members_Type type_to_count_,
      uint32 role_id_ = std::numeric_limits< uint32 >::max())
      : to_count(to_count_), type_to_count(type_to_count_), role_id(role_id_) {}

  std::string eval(const std::string* key) const override { return "0"; }

  std::string eval(const Element_With_Context< Node_Skeleton >& data, const std::string* key) const override;
  std::string eval(const Element_With_Context< Attic< Node_Skeleton > >& data, const std::string* key) const override;
  std::string eval(const Element_With_Context< Way_Skeleton >& data, const std::string* key) const override;
  std::string eval(const Element_With_Context< Attic< Way_Skeleton > >& data, const std::string* key) const override;
  std::string eval(const Element_With_Context< Relation_Skeleton >& data, const std::string* key) const override;
  std::string eval(const Element_With_Context< Attic< Relation_Skeleton > >& data, const std::string* key) const override;
  std::string eval(const Element_With_Context< Area_Skeleton >& data, const std::string* key) const override;
  std::string eval(const Element_With_Context< Derived_Skeleton >& data, const std::string* key) const override;

private:
  Evaluator_Properties_Count::Objects to_count;
  Evaluator_Properties_Count::Members_Type type_to_count;
  uint32 role_id;
};


#endif
