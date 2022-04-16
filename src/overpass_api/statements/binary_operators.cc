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
#include "binary_operators.h"

using namespace std::string_literals;

Evaluator_Pair_Operator::Evaluator_Pair_Operator(int line_number_) : Evaluator(line_number_), lhs(0), rhs(0) {}


void Evaluator_Pair_Operator::add_statement(Statement* statement, std::string text)
{
  auto* tag_value_ = dynamic_cast< Evaluator* >(statement);
  if (!tag_value_)
    substatement_error(get_name(), statement);
  else if (!lhs)
    lhs = tag_value_;
  else if (!rhs)
    rhs = tag_value_;
  else
    add_static_error(get_name() + " must have exactly two evaluator substatements.");
}


void Evaluator_Pair_Operator::add_substatements(Statement* result, const std::string& operator_name,
    const Token_Node_Ptr& tree_it, Statement::QL_Context tree_context,
    Statement::Factory& stmt_factory, Error_Output* error_output)
{
  if (result)
  {
    Statement* lhs = stmt_factory.create_evaluator(
        tree_it.lhs(), tree_context, Statement::Single_Return_Type_Checker(Statement::string));
    if (lhs)
      result->add_statement(lhs, "");
    else if (error_output)
      error_output->add_parse_error(std::string("Operator \"") + operator_name
          + "\" needs a left-hand-side argument", tree_it->line_col.first);

    Statement* rhs = stmt_factory.create_evaluator(
        tree_it.rhs(), tree_context, Statement::Single_Return_Type_Checker(Statement::string));
    if (rhs)
      result->add_statement(rhs, "");
    else if (error_output)
      error_output->add_parse_error(std::string("Operator \"") + operator_name
          + "\" needs a right-hand-side argument", tree_it->line_col.first);
  }
}


Eval_Task* Evaluator_Pair_Operator::get_string_task(Prepare_Task_Context& context, const std::string* key)
{
  Eval_Task* lhs_task = lhs ? lhs->get_string_task(context, key) : 0;
  Eval_Task* rhs_task = rhs ? rhs->get_string_task(context, key) : 0;
  return new Binary_Eval_Task(lhs_task, rhs_task, this);
}


Eval_Variant Binary_Eval_Task::eval(const std::string* key) const
{
  return evaluator->process([&] { return (lhs ? lhs->eval(key) : ""); },
                            [&] { return (rhs ? rhs->eval(key) : ""); });
}


Eval_Variant Binary_Eval_Task::eval(const Element_With_Context< Node_Skeleton >& data, const std::string* key) const
{
  return evaluator->process([&] { return (lhs ? lhs->eval(data, key) : ""); },
                            [&] { return (rhs ? rhs->eval(data, key) : ""); });
}


Eval_Variant Binary_Eval_Task::eval(const Element_With_Context< Attic< Node_Skeleton > >& data, const std::string* key) const
{
  return evaluator->process([&] { return (lhs ? lhs->eval(data, key) : ""); },
                            [&] { return (rhs ? rhs->eval(data, key) : ""); });
}


Eval_Variant Binary_Eval_Task::eval(const Element_With_Context< Way_Skeleton >& data, const std::string* key) const
{
  return evaluator->process([&] { return (lhs ? lhs->eval(data, key) : ""); },
                            [&] { return (rhs ? rhs->eval(data, key) : ""); });
}


Eval_Variant Binary_Eval_Task::eval(const Element_With_Context< Attic< Way_Skeleton > >& data, const std::string* key) const
{
  return evaluator->process([&] { return (lhs ? lhs->eval(data, key) : ""); },
                            [&] { return (rhs ? rhs->eval(data, key) : ""); });
}


Eval_Variant Binary_Eval_Task::eval(const Element_With_Context< Relation_Skeleton >& data, const std::string* key) const
{
  return evaluator->process([&] { return (lhs ? lhs->eval(data, key) : ""); },
                            [&] { return (rhs ? rhs->eval(data, key) : ""); });
}


Eval_Variant Binary_Eval_Task::eval(const Element_With_Context< Attic< Relation_Skeleton > >& data, const std::string* key) const
{
  return evaluator->process([&] { return (lhs ? lhs->eval(data, key) : ""); },
                            [&] { return (rhs ? rhs->eval(data, key) : ""); });
}


Eval_Variant Binary_Eval_Task::eval(const Element_With_Context< Area_Skeleton >& data, const std::string* key) const
{
  return evaluator->process([&] { return (lhs ? lhs->eval(data, key) : ""); },
                            [&] { return (rhs ? rhs->eval(data, key) : ""); });
}


Eval_Variant Binary_Eval_Task::eval(const Element_With_Context< Derived_Skeleton >& data, const std::string* key) const
{
  return evaluator->process([&] { return (lhs ? lhs->eval(data, key) : ""); },
                            [&] { return (rhs ? rhs->eval(data, key) : ""); });
}


Eval_Variant Binary_Eval_Task::eval(uint pos, const Element_With_Context< Way_Skeleton >& data, const std::string* key) const
{
  return evaluator->process([&] { return (lhs ? lhs->eval(pos, data, key) : ""); },
                            [&] { return (rhs ? rhs->eval(pos, data, key) : ""); });
}


Eval_Variant Binary_Eval_Task::eval(uint pos, const Element_With_Context< Attic< Way_Skeleton > >& data, const std::string* key) const
{
  return evaluator->process([&] { return (lhs ? lhs->eval(pos, data, key) : ""); },
                            [&] { return (rhs ? rhs->eval(pos, data, key) : ""); });
}


Eval_Variant Binary_Eval_Task::eval(uint pos, const Element_With_Context< Relation_Skeleton >& data, const std::string* key) const
{
  return evaluator->process([&] { return (lhs ? lhs->eval(pos, data, key) : ""); },
                            [&] { return (rhs ? rhs->eval(pos, data, key) : ""); });
}


Eval_Variant Binary_Eval_Task::eval(uint pos, const Element_With_Context< Attic< Relation_Skeleton > >& data, const std::string* key) const
{
  return evaluator->process([&] { return (lhs ? lhs->eval(pos, data, key) : ""); },
                            [&] { return (rhs ? rhs->eval(pos, data, key) : ""); });
}


Requested_Context Evaluator_Pair_Operator::request_context() const
{
  if (lhs && rhs)
  {
    Requested_Context result = lhs->request_context();
    result.add(rhs->request_context());
    return result;
  }
  else if (lhs)
    return lhs->request_context();
  else if (rhs)
    return rhs->request_context();

  return Requested_Context();
}


//-----------------------------------------------------------------------------


Operator_Stmt_Maker< Evaluator_And > Evaluator_And::statement_maker;
Operator_Eval_Maker< Evaluator_And > Evaluator_And::evaluator_maker;


Eval_Variant Evaluator_And::process(const Eval_Variant& lhs_s, const Eval_Variant& rhs_s) const
{
  return eval_variant_represents_boolean_true(lhs_s) && eval_variant_represents_boolean_true(rhs_s);
}

inline Eval_Variant Evaluator_And::process(TransientFunction<Eval_Variant()> lhs, TransientFunction<Eval_Variant()> rhs) const
{
  return eval_variant_represents_boolean_true(lhs()) && eval_variant_represents_boolean_true(rhs());
}


//-----------------------------------------------------------------------------


Operator_Stmt_Maker< Evaluator_Or > Evaluator_Or::statement_maker;
Operator_Eval_Maker< Evaluator_Or > Evaluator_Or::evaluator_maker;


Eval_Variant Evaluator_Or::process(const Eval_Variant& lhs_s, const Eval_Variant& rhs_s) const
{
  return eval_variant_represents_boolean_true(lhs_s) || eval_variant_represents_boolean_true(rhs_s);
}

inline Eval_Variant Evaluator_Or::process(TransientFunction<Eval_Variant()> lhs, TransientFunction<Eval_Variant()> rhs) const
{
  return eval_variant_represents_boolean_true(lhs()) || eval_variant_represents_boolean_true(rhs());
}


//-----------------------------------------------------------------------------


Operator_Stmt_Maker< Evaluator_Equal > Evaluator_Equal::statement_maker;
Operator_Eval_Maker< Evaluator_Equal > Evaluator_Equal::evaluator_maker;


Eval_Variant Evaluator_Equal::process(const Eval_Variant& lhs_s, const Eval_Variant& rhs_s) const
{
  int64 lhs_l = 0;
  int64 rhs_l = 0;

  if (try_int64(lhs_s, lhs_l) && try_int64(rhs_s, rhs_l))
    return lhs_l == rhs_l;

  double lhs_d = 0;
  double rhs_d = 0;
  if (try_double(lhs_s, lhs_d) && try_double(rhs_s, rhs_d))
    return lhs_d == rhs_d;

  return eval_variant_to_string(lhs_s) == eval_variant_to_string(rhs_s);
}


//-----------------------------------------------------------------------------


Operator_Stmt_Maker< Evaluator_Not_Equal > Evaluator_Not_Equal::statement_maker;
Operator_Eval_Maker< Evaluator_Not_Equal > Evaluator_Not_Equal::evaluator_maker;


Eval_Variant Evaluator_Not_Equal::process(const Eval_Variant& lhs_s, const Eval_Variant& rhs_s) const
{
  int64 lhs_l = 0;
  int64 rhs_l = 0;
  if (try_int64(lhs_s, lhs_l) && try_int64(rhs_s, rhs_l))
    return !(lhs_l == rhs_l);

  double lhs_d = 0;
  double rhs_d = 0;
  if (try_double(lhs_s, lhs_d) && try_double(rhs_s, rhs_d))
    return !(lhs_d == rhs_d);

  return !(eval_variant_to_string(lhs_s) == eval_variant_to_string(rhs_s));
}


//-----------------------------------------------------------------------------


Operator_Stmt_Maker< Evaluator_Less > Evaluator_Less::statement_maker;
Operator_Eval_Maker< Evaluator_Less > Evaluator_Less::evaluator_maker;


Eval_Variant Evaluator_Less::process(const Eval_Variant& lhs_s, const Eval_Variant& rhs_s) const
{
  int64 lhs_l = 0;
  int64 rhs_l = 0;
  if (try_int64(lhs_s, lhs_l) && try_int64(rhs_s, rhs_l))
    return lhs_l < rhs_l;

  double lhs_d = 0;
  double rhs_d = 0;
  if (try_double(lhs_s, lhs_d) && try_double(rhs_s, rhs_d))
    return lhs_d < rhs_d;

  return eval_variant_to_string(lhs_s) < eval_variant_to_string(rhs_s);
}


//-----------------------------------------------------------------------------


Operator_Stmt_Maker< Evaluator_Less_Equal > Evaluator_Less_Equal::statement_maker;
Operator_Eval_Maker< Evaluator_Less_Equal > Evaluator_Less_Equal::evaluator_maker;


Eval_Variant Evaluator_Less_Equal::process(const Eval_Variant& lhs_s, const Eval_Variant& rhs_s) const
{
  int64 lhs_l = 0;
  int64 rhs_l = 0;
  if (try_int64(lhs_s, lhs_l) && try_int64(rhs_s, rhs_l))
    return lhs_l <= rhs_l;

  double lhs_d = 0;
  double rhs_d = 0;
  if (try_double(lhs_s, lhs_d) && try_double(rhs_s, rhs_d))
    return lhs_d <= rhs_d;

  return eval_variant_to_string(lhs_s) <= eval_variant_to_string(rhs_s);
}


//-----------------------------------------------------------------------------


Operator_Stmt_Maker< Evaluator_Greater > Evaluator_Greater::statement_maker;
Operator_Eval_Maker< Evaluator_Greater > Evaluator_Greater::evaluator_maker;


Eval_Variant Evaluator_Greater::process(const Eval_Variant& lhs_s, const Eval_Variant& rhs_s) const
{
  int64 lhs_l = 0;
  int64 rhs_l = 0;
  if (try_int64(lhs_s, lhs_l) && try_int64(rhs_s, rhs_l))
    return lhs_l > rhs_l;

  double lhs_d = 0;
  double rhs_d = 0;
  if (try_double(lhs_s, lhs_d) && try_double(rhs_s, rhs_d))
    return lhs_d > rhs_d;

  return eval_variant_to_string(lhs_s) > eval_variant_to_string(rhs_s);
}


//-----------------------------------------------------------------------------


Operator_Stmt_Maker< Evaluator_Greater_Equal > Evaluator_Greater_Equal::statement_maker;
Operator_Eval_Maker< Evaluator_Greater_Equal > Evaluator_Greater_Equal::evaluator_maker;


Eval_Variant Evaluator_Greater_Equal::process(const Eval_Variant& lhs_s, const Eval_Variant& rhs_s) const
{
  int64 lhs_l = 0;
  int64 rhs_l = 0;
  if (try_int64(lhs_s, lhs_l) && try_int64(rhs_s, rhs_l))
    return lhs_l >= rhs_l;

  double lhs_d = 0;
  double rhs_d = 0;
  if (try_double(lhs_s, lhs_d) && try_double(rhs_s, rhs_d))
    return lhs_d >= rhs_d;

  return eval_variant_to_string(lhs_s) >= eval_variant_to_string(rhs_s);
}


//-----------------------------------------------------------------------------


Operator_Stmt_Maker< Evaluator_Plus > Evaluator_Plus::statement_maker;
Operator_Eval_Maker< Evaluator_Plus > Evaluator_Plus::evaluator_maker;


Eval_Variant Evaluator_Plus::process(const Eval_Variant& lhs_s, const Eval_Variant& rhs_s) const
{
  int64 lhs_l = 0;
  int64 rhs_l = 0;
  if (try_int64(lhs_s, lhs_l) && try_int64(rhs_s, rhs_l))
    return (lhs_l + rhs_l);

  double lhs_d = 0;
  double rhs_d = 0;
  if (try_double(lhs_s, lhs_d) && try_double(rhs_s, rhs_d))
    return (lhs_d + rhs_d);

  return eval_variant_to_string(lhs_s) + eval_variant_to_string(rhs_s);
}


//-----------------------------------------------------------------------------


Operator_Stmt_Maker< Evaluator_Minus > Evaluator_Minus::statement_maker;
Operator_Eval_Maker< Evaluator_Minus > Evaluator_Minus::evaluator_maker;


Eval_Variant Evaluator_Minus::process(const Eval_Variant& lhs_s, const Eval_Variant& rhs_s) const
{
  int64 lhs_l = 0;
  int64 rhs_l = 0;
  if (try_int64(lhs_s, lhs_l) && try_int64(rhs_s, rhs_l))
    return (lhs_l - rhs_l);

  double lhs_d = 0;
  double rhs_d = 0;
  if (try_double(lhs_s, lhs_d) && try_double(rhs_s, rhs_d))
    return (lhs_d - rhs_d);

  return "NaN"s;
}


//-----------------------------------------------------------------------------


Operator_Stmt_Maker< Evaluator_Times > Evaluator_Times::statement_maker;
Operator_Eval_Maker< Evaluator_Times > Evaluator_Times::evaluator_maker;


Eval_Variant Evaluator_Times::process(const Eval_Variant& lhs_s, const Eval_Variant& rhs_s) const
{
  int64 lhs_l = 0;
  int64 rhs_l = 0;
  if (try_int64(lhs_s, lhs_l) && try_int64(rhs_s, rhs_l))
    return (lhs_l * rhs_l);

  double lhs_d = 0;
  double rhs_d = 0;
  if (try_double(lhs_s, lhs_d) && try_double(rhs_s, rhs_d))
    return (lhs_d * rhs_d);

  return "NaN"s;
}


//-----------------------------------------------------------------------------


Operator_Stmt_Maker< Evaluator_Divided > Evaluator_Divided::statement_maker;
Operator_Eval_Maker< Evaluator_Divided > Evaluator_Divided::evaluator_maker;


Eval_Variant Evaluator_Divided::process(const Eval_Variant& lhs_s, const Eval_Variant& rhs_s) const
{
  // On purpose no int64 detection

  double lhs_d = 0;
  double rhs_d = 0;
  if (try_double(lhs_s, lhs_d) && try_double(rhs_s, rhs_d))
    return (lhs_d / rhs_d);

  return "NaN"s;
}

//-----------------------------------------------------------------------------


Operator_Stmt_Maker< Evaluator_Modulo > Evaluator_Modulo::statement_maker;
Operator_Eval_Maker< Evaluator_Modulo > Evaluator_Modulo::evaluator_maker;


Eval_Variant Evaluator_Modulo::process(const Eval_Variant& lhs_s, const Eval_Variant& rhs_s) const
{
  int64 lhs_l = 0;
  int64 rhs_l = 0;
  if (try_int64(lhs_s, lhs_l) && try_int64(rhs_s, rhs_l)) {
    if (rhs_l != 0)  // in case rhs is 0, return "NaN"
      return (lhs_l % rhs_l);
  }

  return "NaN"s;
}
