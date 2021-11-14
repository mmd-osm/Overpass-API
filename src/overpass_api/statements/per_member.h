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

#ifndef DE__OSM3S___OVERPASS_API__STATEMENTS__PER_MEMBER_H
#define DE__OSM3S___OVERPASS_API__STATEMENTS__PER_MEMBER_H


#include "../../expat/escape_json.h"
#include "../../expat/escape_xml.h"
#include "../data/tag_store.h"
#include "../data/utils.h"
#include "evaluator.h"
#include "statement.h"

#include <map>
#include <string>
#include <vector>


struct Per_Member_Aggregator : public Evaluator
{
  Per_Member_Aggregator(int line_number_) : Evaluator(line_number_), rhs(0) {}
  void add_statement(Statement* statement, std::string text) override;

  Evaluator* rhs;
};


template< typename Evaluator_ >
struct Per_Member_Aggregator_Syntax : public Per_Member_Aggregator
{
  Per_Member_Aggregator_Syntax(int line_number_) : Per_Member_Aggregator(line_number_) {}

  std::string dump_xml(const std::string& indent) const override
  {
    return indent + "<" + Evaluator_::stmt_name() + ">\n"
        + (rhs ? rhs->dump_xml(indent + "  ") : "")
        + indent + "</" + Evaluator_::stmt_name() + ">\n";
  }

  std::string dump_compact_ql(const std::string&) const override
  {
    return Evaluator_::stmt_func_name() + "("
        + (rhs ? rhs->dump_compact_ql("") : "")
        + ")";
  }

  std::string get_name() const override { return Evaluator_::stmt_name(); }
  std::string get_result_name() const override { return ""; }
};


template< typename Evaluator_ >
struct Per_Member_Aggregator_Maker final : Statement::Evaluator_Maker
{
  Statement* create_evaluator(
      const Token_Node_Ptr& tree_it, Statement::QL_Context tree_context,
      Statement::Factory& stmt_factory, Parsed_Query& global_settings, Error_Output* error_output) override
  {
    if (!tree_it.assert_is_function(error_output) || !tree_it.assert_has_input_set(error_output, false)
        || !tree_it.assert_has_arguments(error_output, true)
        || !assert_element_in_context(error_output, tree_it, tree_context))
      return 0;

    Statement* result = new Evaluator_(
        tree_it->line_col.first, std::map< std::string, std::string >(), global_settings);
    if (result)
    {
      Statement* rhs = stmt_factory.create_evaluator(
          tree_it.rhs(), Statement::member_eval_possible,
          Statement::Single_Return_Type_Checker(Statement::string));
      if (rhs)
        result->add_statement(rhs, "");
      else if (error_output)
        error_output->add_parse_error(Evaluator_::stmt_func_name() + "(...) needs an argument",
            tree_it->line_col.first);
    }
    return result;
  }

  Per_Member_Aggregator_Maker()
  {
    Statement::maker_by_func_name()[Evaluator_::stmt_func_name()].push_back(this);
  }
};


/* === Per Member Aggregators ===

Aggregators per member help to process information that does apply to only a single member of a way or relation.
Examples of this are the position within the way or relation, geometric properties like the angles, or roles.
A per member aggregator needs an element to operate on and executes its argument multiple times,
passing the possible positions one by one in addition to the element to its argument.

==== Per Member ====

For each element, the aggregator executes its argument once per member of the element.
The return value is a semicolon separated list of the return values.
No deduplication or sotring takes place.
Note that the aggregator returns an empty value for nodes and deriveds
and does not execute its argument in that case.

The syntax is

  per_member(<Evaluator>)
*/

struct Per_Member_Eval_Task final : public Eval_Task
{
  Per_Member_Eval_Task(Eval_Task* rhs) : rhs_task(rhs) {}

  std::string eval(const std::string* key) const override { return ""; }

  std::string eval(const Element_With_Context< Way_Skeleton >& data, const std::string* key) const override;
  std::string eval(const Element_With_Context< Attic< Way_Skeleton > >& data, const std::string* key) const override;
  std::string eval(const Element_With_Context< Relation_Skeleton >& data, const std::string* key) const override;
  std::string eval(const Element_With_Context< Attic< Relation_Skeleton > >& data, const std::string* key) const override;

private:
  std::unique_ptr< Eval_Task > rhs_task;
};


class Evaluator_Per_Member final : public Per_Member_Aggregator_Syntax< Evaluator_Per_Member >
{
public:
  struct Statement_Maker : public Generic_Statement_Maker< Evaluator_Per_Member >
  {
    Statement_Maker() : Generic_Statement_Maker< Evaluator_Per_Member >("eval-per-member") {}
  };
  static Statement_Maker statement_maker;
  static Per_Member_Aggregator_Maker< Evaluator_Per_Member > evaluator_maker;

  static std::string stmt_func_name() { return "per_member"; }
  static std::string stmt_name() { return "eval-per-member"; }

  Evaluator_Per_Member(int line_number_, const std::map< std::string, std::string >& input_attributes,
                   Parsed_Query& global_settings);
  std::string get_result_name() const override { return ""; }
  void execute(Resource_Manager& rman) override {}
  ~Evaluator_Per_Member() override = default;

  Requested_Context request_context() const override
  { return (rhs ? rhs->request_context() : Requested_Context()).add_usage(Set_Usage::SKELETON); }

  Statement::Eval_Return_Type return_type() const override { return Statement::string; };
  Eval_Task* get_string_task(Prepare_Task_Context& context, const std::string* key) override;
};


/* ==== Per Vertex ====

For each element, the aggregator executes its argument once per (inner) vertex of the element.
Otherwise it behaves similar to the per_member operator.
For closed ways, this means that it is executed once for all vertices but the first member.
For open ways, it is executed once for all vertices but the first and last member.
For relations its behaviour is currently undefined.

The syntax is

  per_vertex(<Evaluator>)
*/

struct Per_Vertex_Eval_Task final : public Eval_Task
{
  Per_Vertex_Eval_Task(Eval_Task* rhs) : rhs_task(rhs) {}

  std::string eval(const std::string* key) const override { return ""; }

  std::string eval(const Element_With_Context< Way_Skeleton >& data, const std::string* key) const override;
  std::string eval(const Element_With_Context< Attic< Way_Skeleton > >& data, const std::string* key) const override;

private:
  std::unique_ptr< Eval_Task > rhs_task;
};


class Evaluator_Per_Vertex final : public Per_Member_Aggregator_Syntax< Evaluator_Per_Vertex >
{
public:
  struct Statement_Maker : public Generic_Statement_Maker< Evaluator_Per_Vertex >
  {
    Statement_Maker() : Generic_Statement_Maker< Evaluator_Per_Vertex >("eval-per-vertex") {}
  };
  static Statement_Maker statement_maker;
  static Per_Member_Aggregator_Maker< Evaluator_Per_Vertex > evaluator_maker;

  static std::string stmt_func_name() { return "per_vertex"; }
  static std::string stmt_name() { return "eval-per-vertex"; }

  Evaluator_Per_Vertex(int line_number_, const std::map< std::string, std::string >& input_attributes,
                   Parsed_Query& global_settings);
  std::string get_result_name() const override { return ""; }
  void execute(Resource_Manager& rman) override {}
  ~Evaluator_Per_Vertex() override = default;

  Requested_Context request_context() const override
  { return (rhs ? rhs->request_context() : Requested_Context()).add_usage(Set_Usage::SKELETON); }

  Statement::Eval_Return_Type return_type() const override { return Statement::string; };
  Eval_Task* get_string_task(Prepare_Task_Context& context, const std::string* key) override;
};


/* === Member Dependend Functions ===

Member dependend functions can only be used when an element plus a position within the element is in context.
They then deliver specific information for that member.

==== Position of the Member ====

The position function returns for a member its one-based position within the element.

The syntax is

  pos()
*/

struct Pos_Eval_Task final : public Eval_Task
{
  std::string eval(const std::string* key) const override { return ""; }

  std::string eval(uint pos, const Element_With_Context< Way_Skeleton >& data, const std::string* key) const override
      { return to_string(pos+1); }
  std::string eval(uint pos, const Element_With_Context< Attic< Way_Skeleton > >& data, const std::string* key) const override
      { return to_string(pos+1); }
  std::string eval(uint pos, const Element_With_Context< Relation_Skeleton >& data, const std::string* key) const override
      { return to_string(pos+1); }
  std::string eval(uint pos, const Element_With_Context< Attic< Relation_Skeleton > >& data, const std::string* key) const override
      { return to_string(pos+1); }
};


class Evaluator_Pos final : public Evaluator
{
public:
  struct Statement_Maker : public Generic_Statement_Maker< Evaluator_Pos >
  {
    Statement_Maker() : Generic_Statement_Maker< Evaluator_Pos >("eval-pos") {}
  };
  static Statement_Maker statement_maker;
  static Member_Function_Maker< Evaluator_Pos > evaluator_maker;

  static std::string stmt_func_name() { return "pos"; }
  std::string dump_xml(const std::string& indent) const override
  { return indent + "<eval-pos/>\n"; }
  std::string dump_compact_ql(const std::string&) const override { return "pos()"; }

  Evaluator_Pos(int line_number_, const std::map< std::string, std::string >& input_attributes,
                   Parsed_Query& global_settings);
  std::string get_name() const override { return "eval-pos"; }
  std::string get_result_name() const override { return ""; }
  void execute(Resource_Manager& rman) override {}
  ~Evaluator_Pos() override = default;

  Requested_Context request_context() const override { return Requested_Context().add_usage(Set_Usage::SKELETON); }

  Statement::Eval_Return_Type return_type() const override { return Statement::string; };
  Eval_Task* get_string_task(Prepare_Task_Context& context, const std::string* key) override { return new Pos_Eval_Task(); }
};


/* ==== Reference to the Member ====

The functions mtype and reference return for a member the type resp. id of the referenced object.

The syntax is

  mtype()

resp.

  ref()
*/

struct Membertype_Eval_Task final : public Eval_Task
{
  std::string eval(const std::string* key) const override { return ""; }

  std::string eval(uint pos, const Element_With_Context< Way_Skeleton >& data, const std::string* key) const override
      { return "node"; }
  std::string eval(uint pos, const Element_With_Context< Attic< Way_Skeleton > >& data, const std::string* key) const override
      { return "node"; }
  std::string eval(uint pos, const Element_With_Context< Relation_Skeleton >& data, const std::string* key) const override
      { return member_type_name(data.object->members()[pos].type); }
  std::string eval(uint pos, const Element_With_Context< Attic< Relation_Skeleton > >& data, const std::string* key) const override
      { return member_type_name(data.object->members()[pos].type); }
};


class Evaluator_Membertype final : public Evaluator
{
public:
  struct Statement_Maker : public Generic_Statement_Maker< Evaluator_Membertype >
  {
    Statement_Maker() : Generic_Statement_Maker< Evaluator_Membertype >("eval-membertype") {}
  };
  static Statement_Maker statement_maker;
  static Member_Function_Maker< Evaluator_Membertype > evaluator_maker;

  static std::string stmt_func_name() { return "mtype"; }
  std::string dump_xml(const std::string& indent) const override
  { return indent + "<eval-membertype/>\n"; }
  std::string dump_compact_ql(const std::string&) const override { return "mtype()"; }

  Evaluator_Membertype(int line_number_, const std::map< std::string, std::string >& input_attributes,
                   Parsed_Query& global_settings);
  std::string get_name() const override { return "eval-membertype"; }
  std::string get_result_name() const override { return ""; }
  void execute(Resource_Manager& rman) override {}
  ~Evaluator_Membertype() override = default;

  Requested_Context request_context() const override { return Requested_Context().add_usage(Set_Usage::SKELETON); }

  Statement::Eval_Return_Type return_type() const override { return Statement::string; };
  Eval_Task* get_string_task(Prepare_Task_Context& context, const std::string* key) override { return new Membertype_Eval_Task(); }
};


struct Ref_Eval_Task final : public Eval_Task
{
  std::string eval(const std::string* key) const override { return ""; }

  std::string eval(uint pos, const Element_With_Context< Way_Skeleton >& data, const std::string* key) const override
      { return to_string(data.object->nds()[pos].val()); }
  std::string eval(uint pos, const Element_With_Context< Attic< Way_Skeleton > >& data, const std::string* key) const override
      { return to_string(data.object->nds()[pos].val()); }
  std::string eval(uint pos, const Element_With_Context< Relation_Skeleton >& data, const std::string* key) const override
      { return to_string(data.object->members()[pos].ref.val()); }
  std::string eval(uint pos, const Element_With_Context< Attic< Relation_Skeleton > >& data, const std::string* key) const override
      { return to_string(data.object->members()[pos].ref.val()); }
};


class Evaluator_Ref final : public Evaluator
{
public:
  struct Statement_Maker : public Generic_Statement_Maker< Evaluator_Ref >
  {
    Statement_Maker() : Generic_Statement_Maker< Evaluator_Ref >("eval-ref") {}
  };
  static Statement_Maker statement_maker;
  static Member_Function_Maker< Evaluator_Ref > evaluator_maker;

  static std::string stmt_func_name() { return "ref"; }
  std::string dump_xml(const std::string& indent) const override
  { return indent + "<eval-ref/>\n"; }
  std::string dump_compact_ql(const std::string&) const override { return "ref()"; }

  Evaluator_Ref(int line_number_, const std::map< std::string, std::string >& input_attributes,
                   Parsed_Query& global_settings);
  std::string get_name() const override { return "eval-ref"; }
  std::string get_result_name() const override { return ""; }
  void execute(Resource_Manager& rman) override {}
  ~Evaluator_Ref() override = default;

  Requested_Context request_context() const override { return Requested_Context().add_usage(Set_Usage::SKELETON); }

  Statement::Eval_Return_Type return_type() const override { return Statement::string; };
  Eval_Task* get_string_task(Prepare_Task_Context& context, const std::string* key) override { return new Ref_Eval_Task(); }
};


/* ==== Role of the Member ====

The role function returns the role of the member.

The syntax is

  role()
*/

struct Role_Eval_Task final : public Eval_Task
{
  Role_Eval_Task(const std::map< uint32, std::string >* roles_) : roles(roles_) {}

  std::string eval(const std::string* key) const override { return ""; }

  std::string eval(uint pos, const Element_With_Context< Way_Skeleton >& data, const std::string* key) const override
      { return ""; }
  std::string eval(uint pos, const Element_With_Context< Attic< Way_Skeleton > >& data, const std::string* key) const override
      { return ""; }
  std::string eval(uint pos, const Element_With_Context< Relation_Skeleton >& data, const std::string* key) const override
      { return roles ? roles->find(data.object->members()[pos].role)->second : std::string(); }
  std::string eval(uint pos, const Element_With_Context< Attic< Relation_Skeleton > >& data, const std::string* key) const override
      { return roles ? roles->find(data.object->members()[pos].role)->second : std::string(); }

private:
  const std::map< uint32, std::string >* roles;
};


class Evaluator_Role final : public Evaluator
{
public:
  struct Statement_Maker : public Generic_Statement_Maker< Evaluator_Role >
  {
    Statement_Maker() : Generic_Statement_Maker< Evaluator_Role >("eval-role") {}
  };
  static Statement_Maker statement_maker;
  static Member_Function_Maker< Evaluator_Role > evaluator_maker;

  static std::string stmt_func_name() { return "role"; }
  std::string dump_xml(const std::string& indent) const override
  { return indent + "<eval-role/>\n"; }
  std::string dump_compact_ql(const std::string&) const override { return "role()"; }

  Evaluator_Role(int line_number_, const std::map< std::string, std::string >& input_attributes,
                   Parsed_Query& global_settings);
  std::string get_name() const override { return "eval-role"; }
  std::string get_result_name() const override { return ""; }
  void execute(Resource_Manager& rman) override {}
  ~Evaluator_Role() override = default;

  Requested_Context request_context() const override
  { return Requested_Context().add_usage(Set_Usage::SKELETON).add_role_names(); }

  Statement::Eval_Return_Type return_type() const override { return Statement::string; }
  Eval_Task* get_string_task(Prepare_Task_Context& context, const std::string* key) override
  { return new Role_Eval_Task(context.get_roles()); }
};


/* ==== Angle of a Way at the Position of a Member ====

This function returns the angle between the segment ending at this member and the segment starting there.
It is so far only defined for ways.
If the way is a closed way then for the last member the first segment is used as the starting segment.
This function is intended to be used within the per_vertex() enumerator.

The syntax is

  role()
*/

struct Angle_Eval_Task final : public Eval_Task
{
  Angle_Eval_Task() : cache_way_ref(0u), cache_geom_ref(0) {}

  std::string eval(const std::string* key) const override { return ""; }

  std::string eval(uint pos, const Element_With_Context< Way_Skeleton >& data, const std::string* key) const override;
  std::string eval(uint pos, const Element_With_Context< Attic< Way_Skeleton > >& data, const std::string* key) const override;

private:
  mutable Way_Skeleton::Id_Type cache_way_ref;
  mutable const Opaque_Geometry* cache_geom_ref;
  mutable std::vector< Cartesian > cached;

  std::string prettyprinted_angle(uint pos) const;
  void keep_cartesians_up_to_date(Way_Skeleton::Id_Type, const Opaque_Geometry*) const;
};


class Evaluator_Angle final : public Evaluator
{
public:
  struct Statement_Maker : public Generic_Statement_Maker< Evaluator_Angle >
  {
    Statement_Maker() : Generic_Statement_Maker< Evaluator_Angle >("eval-angle") {}
  };
  static Statement_Maker statement_maker;
  static Member_Function_Maker< Evaluator_Angle > evaluator_maker;

  static std::string stmt_func_name() { return "angle"; }
  std::string dump_xml(const std::string& indent) const override
  { return indent + "<eval-angle/>\n"; }
  std::string dump_compact_ql(const std::string&) const override { return "angle()"; }

  Evaluator_Angle(int line_number_, const std::map< std::string, std::string >& input_attributes,
                   Parsed_Query& global_settings);
  std::string get_name() const override { return "eval-angle"; }
  std::string get_result_name() const override { return ""; }
  void execute(Resource_Manager& rman) override {}
  ~Evaluator_Angle() override = default;

  Requested_Context request_context() const override
  { return Requested_Context().add_usage(Set_Usage::GEOMETRY); }

  Statement::Eval_Return_Type return_type() const override { return Statement::string; }
  Eval_Task* get_string_task(Prepare_Task_Context& context, const std::string* key) override
  { return new Angle_Eval_Task(); }
};


#endif
