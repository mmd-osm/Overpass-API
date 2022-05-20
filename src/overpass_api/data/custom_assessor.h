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

#ifndef DE__OSM3S___OVERPASS_API__DATA__CUSTOM_ASSESSOR_H
#define DE__OSM3S___OVERPASS_API__DATA__CUSTOM_ASSESSOR_H


#include <map>
#include <string>
#include <vector>

#include "../../template_db/block_backend.h"
#include "../../template_db/transaction.h"
#include "../core/datatypes.h"
#include "../core/settings.h"

// Header file for performance optimized custom Range_Idx_Assessor implementations
// These explicit template specializations replace their generic counterpart in block_backend.h

// Hint: adding a declaration for these explicit specializations in block_backend.h
// helps to ensure that this code is being used everywhere.


template< >
struct Range_Idx_Assessor<Tag_Index_Global, Ranges< Tag_Index_Global >::Iterator >
{
  Range_Idx_Assessor(const Ranges< Tag_Index_Global >::Iterator& index_it_, const Ranges< Tag_Index_Global >::Iterator& index_end_)
      : index_it(index_it_), index_end(index_end_) {}

  bool is_relevant(Handle < Tag_Index_Global > & handle)
  {
    while (index_it != index_end && !(handle < index_it.upper_bound()))
      ++index_it;
    return index_it != index_end && !(handle < index_it.lower_bound()) && handle < index_it.upper_bound();
  }

private:
  Ranges< Tag_Index_Global >::Iterator index_it;
  Ranges< Tag_Index_Global >::Iterator index_end;
};


template< >
struct Range_Idx_Assessor<Tag_Index_Local, Ranges< Tag_Index_Local >::Iterator >
{
  Range_Idx_Assessor(const Ranges< Tag_Index_Local >::Iterator& index_it_, const Ranges< Tag_Index_Local >::Iterator& index_end_)
      : index_it(index_it_), index_end(index_end_) {}

  bool is_relevant(Handle < Tag_Index_Local > & handle)
  {
    while (index_it != index_end && !(handle < index_it.upper_bound()))
      ++index_it;
    return index_it != index_end && !(handle < index_it.lower_bound()) && handle < index_it.upper_bound();
  }

private:
  Ranges< Tag_Index_Local >::Iterator index_it;
  Ranges< Tag_Index_Local >::Iterator index_end;
};



#endif
