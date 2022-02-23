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

#include <fstream>
#include <iomanip>
#include <iostream>
#include <list>

#include <locale.h>
#include <stdio.h>
#include <stdlib.h>

#include "../../expat/expat_justparse_interface.h"
#include "../../template_db/random_file.h"
#include "../../template_db/transaction.h"
#include "../core/settings.h"
#include "../frontend/output.h"
#include "node_updater.h"

const int PACKAGE_SIZE = 10000;
const int MAX_TIMESTAMPS_PER_DB_UPDATE = 1000000;


int main(int argc, char* args[])
{
  if (argc < 2)
  {
    std::cout<<"Usage: "<<args[0]<<" db_dir\n";
    return 0;
  }
  
  std::string db_dir(args[1]);
  
  try
  {    
    std::vector< Change_Entry< Node_Skeleton::Id_Type > > ce;

    std::map< Timestamp, std::set< Change_Package > > del;
    std::map< Timestamp, std::set< Change_Package > > ins;

    Timestamp current_idx{};

    Nonsynced_Transaction transaction(false, false, db_dir, "");

    Block_Backend< Timestamp, Change_Entry< Node_Skeleton::Id_Type > > db(transaction.data_index(attic_settings().NODE_CHANGELOG));

    Nonsynced_Transaction target_transaction(true, false, db_dir, "");
    Block_Backend_Updater< Timestamp, Change_Package > db_cp(target_transaction.data_index(attic_settings().NODE_CHANGEPACK));


    for (Block_Backend< Timestamp, Change_Entry< Node_Skeleton::Id_Type > >::Flat_Iterator
        it(db.flat_begin()); !(it == db.flat_end()); ++it)
    {
      if (it.start_of_new_index()) {    // start of new timestamp
        if (!ce.empty()) {
          ins[current_idx] = Change_Package::build_packages(ce, PACKAGE_SIZE);
          ce.clear();
        }

        if (ins.size() == MAX_TIMESTAMPS_PER_DB_UPDATE) {
          db_cp.update(del, ins);
          ins.clear();
        }

        current_idx = it.index();
      }
      ce.emplace_back(it.object());
    }

    if (!ce.empty()) {
      ins[current_idx] = Change_Package::build_packages(ce, PACKAGE_SIZE);
      ce.clear();
    }

    db_cp.update(del, ins);
  }
  catch (const File_Error & e)
  {
    std::cout<<e.origin<<' '<<e.filename<<' '<<e.error_number<<'\n';
  }
  
  return 0;
}
