/** Copyright 2021 mmd
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

#ifndef DE__OSM3S___OVERPASS_API__STATEMENTS__STATEMENT_VISITOR_H
#define DE__OSM3S___OVERPASS_API__STATEMENTS__STATEMENT_VISITOR_H

class Statement;
class Osm_Script_Statement;
class Area_Query_Statement;
class Around_Statement;
class Bbox_Query_Statement;
class Changed_Statement;
class Compare_Statement;
class Complete_Statement;
class Convert_Statement;
class Coord_Query_Statement;
class Difference_Statement;
class Else_Statement;
class Evaluator;
class Evaluator_Aggregator;
class Evaluator_All_Keys;
class Evaluator_And;
class Evaluator_Angle;
class Evaluator_Binary_Function;
class Evaluator_Center;
class Evaluator_Changeset;
class Evaluator_Divided;
class Evaluator_Equal;
class Evaluator_Fixed;
class Evaluator_Generic;
class Evaluator_Geom_Concat_Value;
class Evaluator_Geometry;
class Evaluator_Geometry_Unary_Function;
class Evaluator_Greater;
class Evaluator_Greater_Equal;
class Evaluator_Hull;
class Evaluator_Id;
class Evaluator_Is_Closed;
class Evaluator_Is_Tag;
class Evaluator_Latitude;
class Evaluator_Length;
class Evaluator_Less;
class Evaluator_Less_Equal;
class Evaluator_Linestring;
class Evaluator_Longitude;
class Evaluator_Lrs_In;
class Evaluator_Lrs_Isect;
class Evaluator_Lrs_Max;
class Evaluator_Lrs_Min;
class Evaluator_Lrs_Union;
class Evaluator_Max_Value;
class Evaluator_Membertype;
class Evaluator_Minus;
class Evaluator_Min_Value;
class Evaluator_Modulo;
class Evaluator_Not_Equal;
class Evaluator_Or;
class Evaluator_Pair_Operator;
class Evaluator_Per_Member;
class Evaluator_Per_Vertex;
class Evaluator_Plus;
class Evaluator_Point;
class Evaluator_Polygon;
class Evaluator_Pos;
class Evaluator_Properties_Count;
class Evaluator_Ref;
class Evaluator_Role;
class Evaluator_Set_Count;
class Evaluator_Set_Value;
class Evaluator_Sum_Value;
class Evaluator_Times;
class Evaluator_Timestamp;
class Evaluator_Trace;
class Evaluator_Type;
class Evaluator_Uid;
class Evaluator_Unary_Function;
class Evaluator_Union_Value;
class Evaluator_User;
class Evaluator_Value;
class Evaluator_Version;
class Filter_Statement;
class Foreach_Statement;
class For_Statement;
class Has_Kv_Statement;
class Id_Query_Statement;
class If_Statement;
class Item_Statement;
class Make_Area_Statement;
class Make_Statement;
class Map_To_Area_Statement;
class Newer_Statement;
class Output_Statement;
class Pivot_Statement;
class Polygon_Query_Statement;
class Print_Statement;
class Query_Statement;
class Recurse_Statement;
class Retro_Statement;
class Set_Prop_Statement;
class Ternary_Evaluator;
class Timeline_Statement;
class Union_Statement;
class User_Statement;

class Query_Constraint;



class Statement_Visitor {
public:
  virtual ~Statement_Visitor() = default;

  virtual bool visit(Area_Query_Statement& data) { return true; };
  virtual bool visit(Around_Statement& data) { return true; };
  virtual bool visit(Bbox_Query_Statement& data) { return true; };
  virtual bool visit(Changed_Statement& data) { return true; };
  virtual bool visit(Compare_Statement& data) { return true; };
  virtual bool visit(Complete_Statement& data) { return true; };
  virtual bool visit(Convert_Statement& data) { return true; };
  virtual bool visit(Coord_Query_Statement& data) { return true; };
  virtual bool visit(Difference_Statement& data) { return true; };
  virtual bool visit(Else_Statement& data) { return true; };
  virtual bool visit(Evaluator& data) { return true; };
  virtual bool visit(Evaluator_Aggregator& data) { return true; };
  virtual bool visit(Evaluator_All_Keys& data) { return true; };
  virtual bool visit(Evaluator_And& data) { return true; };
  virtual bool visit(Evaluator_Angle& data) { return true; };
  virtual bool visit(Evaluator_Binary_Function& data) { return true; };
  virtual bool visit(Evaluator_Center& data) { return true; };
  virtual bool visit(Evaluator_Changeset& data) { return true; };
  virtual bool visit(Evaluator_Divided& data) { return true; };
  virtual bool visit(Evaluator_Equal& data) { return true; };
  virtual bool visit(Evaluator_Fixed& data) { return true; };
  virtual bool visit(Evaluator_Generic& data) { return true; };
  virtual bool visit(Evaluator_Geom_Concat_Value& data) { return true; };
  virtual bool visit(Evaluator_Geometry& data) { return true; };
  virtual bool visit(Evaluator_Geometry_Unary_Function& data) { return true; };
  virtual bool visit(Evaluator_Greater& data) { return true; };
  virtual bool visit(Evaluator_Greater_Equal& data) { return true; };
  virtual bool visit(Evaluator_Hull& data) { return true; };
  virtual bool visit(Evaluator_Id& data) { return true; };
  virtual bool visit(Evaluator_Is_Closed& data) { return true; };
  virtual bool visit(Evaluator_Is_Tag& data) { return true; };
  virtual bool visit(Evaluator_Latitude& data) { return true; };
  virtual bool visit(Evaluator_Length& data) { return true; };
  virtual bool visit(Evaluator_Less& data) { return true; };
  virtual bool visit(Evaluator_Less_Equal& data) { return true; };
  virtual bool visit(Evaluator_Linestring& data) { return true; };
  virtual bool visit(Evaluator_Longitude& data) { return true; };
  virtual bool visit(Evaluator_Lrs_In& data) { return true; };
  virtual bool visit(Evaluator_Lrs_Isect& data) { return true; };
  virtual bool visit(Evaluator_Lrs_Max& data) { return true; };
  virtual bool visit(Evaluator_Lrs_Min& data) { return true; };
  virtual bool visit(Evaluator_Lrs_Union& data) { return true; };
  virtual bool visit(Evaluator_Max_Value& data) { return true; };
  virtual bool visit(Evaluator_Membertype& data) { return true; };
  virtual bool visit(Evaluator_Minus& data) { return true; };
  virtual bool visit(Evaluator_Min_Value& data) { return true; };
  virtual bool visit(Evaluator_Modulo& data) { return true; };
  virtual bool visit(Evaluator_Not_Equal& data) { return true; };
  virtual bool visit(Evaluator_Or& data) { return true; };
  virtual bool visit(Evaluator_Pair_Operator& data) { return true; };
  virtual bool visit(Evaluator_Per_Member& data) { return true; };
  virtual bool visit(Evaluator_Per_Vertex& data) { return true; };
  virtual bool visit(Evaluator_Plus& data) { return true; };
  virtual bool visit(Evaluator_Point& data) { return true; };
  virtual bool visit(Evaluator_Polygon& data) { return true; };
  virtual bool visit(Evaluator_Pos& data) { return true; };
  virtual bool visit(Evaluator_Properties_Count& data) { return true; };
  virtual bool visit(Evaluator_Ref& data) { return true; };
  virtual bool visit(Evaluator_Role& data) { return true; };
  virtual bool visit(Evaluator_Set_Count& data) { return true; };
  virtual bool visit(Evaluator_Set_Value& data) { return true; };
  virtual bool visit(Evaluator_Sum_Value& data) { return true; };
  virtual bool visit(Evaluator_Times& data) { return true; };
  virtual bool visit(Evaluator_Timestamp& data) { return true; };
  virtual bool visit(Evaluator_Trace& data) { return true; };
  virtual bool visit(Evaluator_Type& data) { return true; };
  virtual bool visit(Evaluator_Uid& data) { return true; };
  virtual bool visit(Evaluator_Unary_Function& data) { return true; };
  virtual bool visit(Evaluator_Union_Value& data) { return true; };
  virtual bool visit(Evaluator_User& data) { return true; };
  virtual bool visit(Evaluator_Value& data) { return true; };
  virtual bool visit(Evaluator_Version& data) { return true; };
  virtual bool visit(Filter_Statement& data) { return true; };
  virtual bool visit(Foreach_Statement& data) { return true; };
  virtual bool visit(For_Statement& data) { return true; };
  virtual bool visit(Has_Kv_Statement& data) { return true; };
  virtual bool visit(Id_Query_Statement& data) { return true; };
  virtual bool visit(If_Statement& data) { return true; };
  virtual bool visit(Item_Statement& data) { return true; };
  virtual bool visit(Make_Area_Statement& data) { return true; };
  virtual bool visit(Make_Statement& data) { return true; };
  virtual bool visit(Map_To_Area_Statement& data) { return true; };
  virtual bool visit(Newer_Statement& data) { return true; };
  virtual bool visit(Osm_Script_Statement& data) { return true; };
  virtual bool visit(Output_Statement& data) { return true; };
  virtual bool visit(Pivot_Statement& data) { return true; };
  virtual bool visit(Polygon_Query_Statement& data) { return true; };
  virtual bool visit(Print_Statement& data) { return true; };
  virtual bool visit(Query_Statement& data) { return true; };
  virtual bool visit(Recurse_Statement& data) { return true; };
  virtual bool visit(Retro_Statement& data) { return true; };
  virtual bool visit(Set_Prop_Statement& data) { return true; };
  virtual bool visit(Ternary_Evaluator& data) { return true; };
  virtual bool visit(Timeline_Statement& data) { return true; };
  virtual bool visit(Union_Statement& data) { return true; };
  virtual bool visit(User_Statement& data) { return true; };

  virtual bool visit(const Area_Query_Statement& data) const { return true; };
  virtual bool visit(const Around_Statement& data) const { return true; };
  virtual bool visit(const Bbox_Query_Statement& data) const { return true; };
  virtual bool visit(const Changed_Statement& data) const { return true; };
  virtual bool visit(const Compare_Statement& data) const { return true; };
  virtual bool visit(const Complete_Statement& data) const { return true; };
  virtual bool visit(const Convert_Statement& data) const { return true; };
  virtual bool visit(const Coord_Query_Statement& data) const { return true; };
  virtual bool visit(const Difference_Statement& data) const { return true; };
  virtual bool visit(const Else_Statement& data) const { return true; };
  virtual bool visit(const Evaluator& data) const { return true; };
  virtual bool visit(const Evaluator_Aggregator& data) const { return true; };
  virtual bool visit(const Evaluator_All_Keys& data) const { return true; };
  virtual bool visit(const Evaluator_And& data) const { return true; };
  virtual bool visit(const Evaluator_Angle& data) const { return true; };
  virtual bool visit(const Evaluator_Binary_Function& data) const { return true; };
  virtual bool visit(const Evaluator_Center& data) const { return true; };
  virtual bool visit(const Evaluator_Changeset& data) const { return true; };
  virtual bool visit(const Evaluator_Divided& data) const { return true; };
  virtual bool visit(const Evaluator_Equal& data) const { return true; };
  virtual bool visit(const Evaluator_Fixed& data) const { return true; };
  virtual bool visit(const Evaluator_Generic& data) const { return true; };
  virtual bool visit(const Evaluator_Geom_Concat_Value& data) const { return true; };
  virtual bool visit(const Evaluator_Geometry& data) const { return true; };
  virtual bool visit(const Evaluator_Geometry_Unary_Function& data) const { return true; };
  virtual bool visit(const Evaluator_Greater& data) const { return true; };
  virtual bool visit(const Evaluator_Greater_Equal& data) const { return true; };
  virtual bool visit(const Evaluator_Hull& data) const { return true; };
  virtual bool visit(const Evaluator_Id& data) const { return true; };
  virtual bool visit(const Evaluator_Is_Closed& data) const { return true; };
  virtual bool visit(const Evaluator_Is_Tag& data) const { return true; };
  virtual bool visit(const Evaluator_Latitude& data) const { return true; };
  virtual bool visit(const Evaluator_Length& data) const { return true; };
  virtual bool visit(const Evaluator_Less& data) const { return true; };
  virtual bool visit(const Evaluator_Less_Equal& data) const { return true; };
  virtual bool visit(const Evaluator_Linestring& data) const { return true; };
  virtual bool visit(const Evaluator_Longitude& data) const { return true; };
  virtual bool visit(const Evaluator_Lrs_In& data) const { return true; };
  virtual bool visit(const Evaluator_Lrs_Isect& data) const { return true; };
  virtual bool visit(const Evaluator_Lrs_Max& data) const { return true; };
  virtual bool visit(const Evaluator_Lrs_Min& data) const { return true; };
  virtual bool visit(const Evaluator_Lrs_Union& data) const { return true; };
  virtual bool visit(const Evaluator_Max_Value& data) const { return true; };
  virtual bool visit(const Evaluator_Membertype& data) const { return true; };
  virtual bool visit(const Evaluator_Minus& data) const { return true; };
  virtual bool visit(const Evaluator_Min_Value& data) const { return true; };
  virtual bool visit(const Evaluator_Modulo& data) const { return true; };
  virtual bool visit(const Evaluator_Not_Equal& data) const { return true; };
  virtual bool visit(const Evaluator_Or& data) const { return true; };
  virtual bool visit(const Evaluator_Pair_Operator& data) const { return true; };
  virtual bool visit(const Evaluator_Per_Member& data) const { return true; };
  virtual bool visit(const Evaluator_Per_Vertex& data) const { return true; };
  virtual bool visit(const Evaluator_Plus& data) const { return true; };
  virtual bool visit(const Evaluator_Point& data) const { return true; };
  virtual bool visit(const Evaluator_Polygon& data) const { return true; };
  virtual bool visit(const Evaluator_Pos& data) const { return true; };
  virtual bool visit(const Evaluator_Properties_Count& data) const { return true; };
  virtual bool visit(const Evaluator_Ref& data) const { return true; };
  virtual bool visit(const Evaluator_Role& data) const { return true; };
  virtual bool visit(const Evaluator_Set_Count& data) const { return true; };
  virtual bool visit(const Evaluator_Set_Value& data) const { return true; };
  virtual bool visit(const Evaluator_Sum_Value& data) const { return true; };
  virtual bool visit(const Evaluator_Times& data) const { return true; };
  virtual bool visit(const Evaluator_Timestamp& data) const { return true; };
  virtual bool visit(const Evaluator_Trace& data) const { return true; };
  virtual bool visit(const Evaluator_Type& data) const { return true; };
  virtual bool visit(const Evaluator_Uid& data) const { return true; };
  virtual bool visit(const Evaluator_Unary_Function& data) const { return true; };
  virtual bool visit(const Evaluator_Union_Value& data) const { return true; };
  virtual bool visit(const Evaluator_User& data) const { return true; };
  virtual bool visit(const Evaluator_Value& data) const { return true; };
  virtual bool visit(const Evaluator_Version& data) const { return true; };
  virtual bool visit(const Filter_Statement& data) const { return true; };
  virtual bool visit(const Foreach_Statement& data) const { return true; };
  virtual bool visit(const For_Statement& data) const { return true; };
  virtual bool visit(const Has_Kv_Statement& data) const { return true; };
  virtual bool visit(const Id_Query_Statement& data) const { return true; };
  virtual bool visit(const If_Statement& data) const { return true; };
  virtual bool visit(const Item_Statement& data) const { return true; };
  virtual bool visit(const Make_Area_Statement& data) const { return true; };
  virtual bool visit(const Make_Statement& data) const { return true; };
  virtual bool visit(const Map_To_Area_Statement& data) const { return true; };
  virtual bool visit(const Newer_Statement& data) const { return true; };
  virtual bool visit(const Osm_Script_Statement& data) const { return true; };
  virtual bool visit(const Output_Statement& data) const { return true; };
  virtual bool visit(const Pivot_Statement& data) const { return true; };
  virtual bool visit(const Polygon_Query_Statement& data) const { return true; };
  virtual bool visit(const Print_Statement& data) const { return true; };
  virtual bool visit(const Query_Statement& data) const { return true; };
  virtual bool visit(const Recurse_Statement& data) const { return true; };
  virtual bool visit(const Retro_Statement& data) const { return true; };
  virtual bool visit(const Set_Prop_Statement& data) const { return true; };
  virtual bool visit(const Ternary_Evaluator& data) const { return true; };
  virtual bool visit(const Timeline_Statement& data) const { return true; };
  virtual bool visit(const Union_Statement& data) const { return true; };
  virtual bool visit(const User_Statement& data) const { return true; };
};

class DataRenderer: public Statement_Visitor {
public:
  ~DataRenderer() override = default;

  bool visit(Osm_Script_Statement& data) override;
  bool visit(Union_Statement& data) override;
  bool visit(Query_Statement& data) override;
  bool visit(Has_Kv_Statement& data) override;
  bool visit(Around_Statement& data) override;
  bool visit(Foreach_Statement& data) override;
  bool visit(Area_Query_Statement& data) override;
  bool visit(Id_Query_Statement& data) override;
  bool visit(Complete_Statement& data) override;
  bool visit(Retro_Statement& data) override;

  void render(Statement& object);

private:
  std::vector<bool> optimize_union;

  bool inside_optimize_union();
};

/*
template <class C>
class AllOfType: public Statement_Visitor
{
public:

    AllOfType(const std::vector<Statement*> & objects_) : objects(objects_), counter(0) {}

    operator bool() const;

private:
    bool visit(const C& data) const override  { ++counter; return true; }

    const std::vector<Statement*> & objects;
    mutable int counter;
};
*/


template <class Self, class... Types>
class OnlyOfTypeOverride;


template <class Self, class ThisType, class... RemainingTypes>
class OnlyOfTypeOverride<Self, ThisType, RemainingTypes...> : public OnlyOfTypeOverride<Self, RemainingTypes...>
{
public:
  bool visit(const ThisType & data) const override
  {
    static_cast<const Self*>(this)->inc();
    return true;
  }
  using OnlyOfTypeOverride<Self, RemainingTypes...>::visit;
};


template <class Self, class ThisType>
class OnlyOfTypeOverride<Self, ThisType> : public Statement_Visitor
{
public:
  bool visit(const ThisType & data) const override
  {
    static_cast<const Self*>(this)->inc();
    return true;
  }
};


template <class... Types>
class Statement_Has_Only_Type : public OnlyOfTypeOverride<Statement_Has_Only_Type<Types...>, Types...>
{
public:
  Statement_Has_Only_Type(const std::vector<Statement*> & objects_) : objects(objects_), counter(0) {}

  operator bool() const;

  void inc() const { ++counter; };

private:
  const std::vector<Statement*> & objects;
  mutable int counter;
};

template <class... Types>
class Constraint_Has_Only_Type : public OnlyOfTypeOverride<Constraint_Has_Only_Type<Types...>, Types...>
{
public:
  Constraint_Has_Only_Type(const std::vector<Query_Constraint*> & constraints_) : constraints(constraints_), counter(0) {}

  operator bool() const;

  void inc() const { ++counter; };

private:
  const std::vector<Query_Constraint*> & constraints;
  mutable int counter;
};




#endif
