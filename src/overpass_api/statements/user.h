#ifndef DE__OSM3S___OVERPASS_API__STATEMENTS__USER_H
#define DE__OSM3S___OVERPASS_API__STATEMENTS__USER_H

#include <map>
#include <set>
#include <string>
#include <vector>
#include "statement.h"

using namespace std;

class User_Statement : public Statement
{
  public:
    User_Statement(int line_number_) : Statement(line_number_) {}
    virtual void set_attributes(const char **attr);
    virtual string get_name() const { return "user"; }
    virtual string get_result_name() const { return output; }
    virtual void forecast();
    virtual void execute(Resource_Manager& rman);
    virtual ~User_Statement() {}

    void calc_ranges
        (set< pair< Uint32_Index, Uint32_Index > >& node_req,
         set< pair< Uint31_Index, Uint31_Index > >& other_req,
         Transaction& transaction);
    uint32 get_id() const { return user_id; }
 
  private:
    string input, output;
    uint32 user_id;
    string user_name;
};

#endif