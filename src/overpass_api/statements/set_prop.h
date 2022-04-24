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

#ifndef DE__OSM3S___OVERPASS_API__STATEMENTS__SET_PROP_H
#define DE__OSM3S___OVERPASS_API__STATEMENTS__SET_PROP_H

#ifdef HAVE_CONFIG_H
#include <config.h>
#undef VERSION
#endif

#include "../../expat/escape_json.h"
#include "../../expat/escape_xml.h"
#include "../data/tag_store.h"
#include "../data/utils.h"
#include "evaluator.h"
#include "statement.h"

#include <map>
#include <string>
#include <vector>


struct Set_Prop_Task
{
  enum Mode { single_key, set_id, set_geometry, generic };

  virtual ~Set_Prop_Task() = default;

  virtual void process(Derived_Structure& result, bool& id_set) const = 0;

  virtual void process(const Element_With_Context< Node_Skeleton >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const = 0;
  virtual void process(const Element_With_Context< Attic< Node_Skeleton > >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const = 0;
  virtual void process(const Element_With_Context< Way_Skeleton >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const = 0;
  virtual void process(const Element_With_Context< Attic< Way_Skeleton > >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const = 0;
  virtual void process(const Element_With_Context< Relation_Skeleton>& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const = 0;
  virtual void process(const Element_With_Context< Attic< Relation_Skeleton > >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const = 0;
  virtual void process(const Element_With_Context< Area_Skeleton >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const = 0;
  virtual void process(const Element_With_Context< Derived_Skeleton >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const = 0;
};


struct Set_Prop_Plain_Task : public Set_Prop_Task
{
  Set_Prop_Plain_Task(Eval_Task* rhs_, const std::string& key_, Mode mode_) : rhs(rhs_), key(key_), mode(mode_) {}
  ~Set_Prop_Plain_Task() override { delete rhs; }

  void process(Derived_Structure& result, bool& id_set) const override;

  void process(const Element_With_Context< Node_Skeleton >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;
  void process(const Element_With_Context< Attic< Node_Skeleton > >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;
  void process(const Element_With_Context< Way_Skeleton >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;
  void process(const Element_With_Context< Attic< Way_Skeleton > >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;
  void process(const Element_With_Context< Relation_Skeleton>& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;
  void process(const Element_With_Context< Attic< Relation_Skeleton > >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;
  void process(const Element_With_Context< Area_Skeleton >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;
  void process(const Element_With_Context< Derived_Skeleton >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;

private:
  Set_Prop_Plain_Task(const Set_Prop_Plain_Task&);
  const Set_Prop_Plain_Task& operator=(const Set_Prop_Plain_Task&);

  Eval_Task* rhs;
  std::string key;
  Mode mode;
};


struct Set_Prop_Generic_Task : public Set_Prop_Task
{
  void process(Derived_Structure& result, bool& id_set) const override;

  void process(const Element_With_Context< Node_Skeleton >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;
  void process(const Element_With_Context< Attic< Node_Skeleton > >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;
  void process(const Element_With_Context< Way_Skeleton >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;
  void process(const Element_With_Context< Attic< Way_Skeleton > >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;
  void process(const Element_With_Context< Relation_Skeleton>& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;
  void process(const Element_With_Context< Attic< Relation_Skeleton > >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;
  void process(const Element_With_Context< Area_Skeleton >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;
  void process(const Element_With_Context< Derived_Skeleton >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;

  void add_key(const std::string& key, Eval_Task* task);

private:
  Owning_Array< Eval_Task > rhs;
  std::vector< std::string > keys;
};


struct Set_Prop_Geometry_Task : public Set_Prop_Task
{
  Set_Prop_Geometry_Task(Eval_Geometry_Task* rhs_) : rhs(rhs_) {}
  ~Set_Prop_Geometry_Task() override { delete rhs; }

  void process(Derived_Structure& result, bool& id_set) const override;

  void process(const Element_With_Context< Node_Skeleton >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;
  void process(const Element_With_Context< Attic< Node_Skeleton > >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;
  void process(const Element_With_Context< Way_Skeleton >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;
  void process(const Element_With_Context< Attic< Way_Skeleton > >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;
  void process(const Element_With_Context< Relation_Skeleton>& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;
  void process(const Element_With_Context< Attic< Relation_Skeleton > >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;
  void process(const Element_With_Context< Area_Skeleton >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;
  void process(const Element_With_Context< Derived_Skeleton >& data,
    const std::vector< std::string >& declared_keys, Derived_Structure& result, bool& id_set) const override;

private:
  Set_Prop_Geometry_Task(const Set_Prop_Plain_Task&);
  const Set_Prop_Geometry_Task& operator=(const Set_Prop_Plain_Task&);

  Eval_Geometry_Task* rhs;
};


class Set_Prop_Statement : public Statement
{
public:
  struct Statement_Maker : public Generic_Statement_Maker< Set_Prop_Statement >
  {
    Statement_Maker() : Generic_Statement_Maker< Set_Prop_Statement >("set-prop") {}
  };
  static Statement_Maker statement_maker;

  struct Evaluator_Maker : public Statement::Evaluator_Maker
  {
    Statement* create_evaluator(const Token_Node_Ptr& tree_it, QL_Context tree_context,
        Statement::Factory& stmt_factory, Parsed_Query& global_settings, Error_Output* error_output) override;
    Evaluator_Maker()
    {
      Statement::maker_by_token()["="].push_back(this);
      Statement::maker_by_token()["!"].push_back(this);
    }
  };
  static Evaluator_Maker evaluator_maker;

#ifdef HAVE_OVERPASS_XML
  std::string dump_xml(const std::string& indent) const override;
#endif

  std::string dump_compact_ql(const std::string&) const override;
  std::string dump_pretty_ql(const std::string&) const override { return dump_compact_ql(""); }

  Set_Prop_Statement(int line_number_, const std::map< std::string, std::string >& input_attributes,
                   Parsed_Query& global_settings);
  std::string get_name() const override { return "set-prop"; }
  std::string get_result_name() const override { return ""; }
  void add_statement(Statement* statement, std::string text) override;
  void execute(Resource_Manager& rman) override {}
  ~Set_Prop_Statement() override { delete key; }

  virtual Requested_Context request_context() const;

  Set_Prop_Task* get_task(Prepare_Task_Context& context, const std::vector< std::string >& otherwise_set_keys);

  const std::string* get_key() const { return key; }
  bool has_value() const { return tag_value; }
  Set_Prop_Task::Mode get_mode() const { return mode; }

private:
  std::string input;
  std::string* key = nullptr;
  Set_Prop_Task::Mode mode;
  Evaluator* tag_value = nullptr;
};


#endif
