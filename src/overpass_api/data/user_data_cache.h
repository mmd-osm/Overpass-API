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

#ifndef DE__OSM3S___OVERPASS_API__DATA__USER_DATA_CACHE_H
#define DE__OSM3S___OVERPASS_API__DATA__USER_DATA_CACHE_H

#include <array>
#include <map>
#include <string>
#include <vector>

#include "../../template_db/block_backend.h"
#include "../../template_db/transaction.h"
#include "../core/datatypes.h"
#include "../core/settings.h"


struct User_Data_Cache
{
  User_Data_Cache()  = default;
  const user_id_name_t& users(Transaction& transaction);
};


inline const user_id_name_t& User_Data_Cache::users(
    Transaction& transaction)
{
  static user_id_name_t users_;
  static bool loaded;
  static uint32 replicate_id;

  if (replicate_id != transaction.get_replicate_id())
  {
    replicate_id = transaction.get_replicate_id();
    if (loaded)
    {
      user_id_name_t().swap(users_);
      loaded = false;
    }
  }

  if (!loaded)
  {
    // According to meta_updater.cc, USER_DATA stores user ids + names in buckets of up to 256 entries,
    // lowest 16 bits of the user id are ignored. Unfortunately, the list of user names / ids per Index
    // is not sorted, and we don't want to sort a huge user list either. So let's use a
    // fixed array instead to sort the entries.
    std::array<std::string, 256> usernames;

    Block_Backend< Uint32_Index, User_Data > user_db
        (transaction.data_index(meta_settings().USER_DATA));
    users_.reserve(1000000);

    uint32 current_idx{};

    for (auto it = user_db.flat_begin(); it != user_db.flat_end(); ++it) {

      if (it.start_of_new_index()) {
        // copy entries for previous index from array to users_ vector
        for (int i = 0; i < 256; i++) {
          if (!usernames[i].empty()) {
            users_.emplace_back(current_idx + i, std::move(usernames[i]));
            usernames[i].clear();
          }
        }
        current_idx = it.index_handle().id();
      }
      usernames[it.handle().id() - current_idx] = it.handle().get_name();
    }

    // and copy remaining entries
    for (int i = 0; i < 256; i++) {
      if (!usernames[i].empty()) {
        users_.emplace_back(current_idx + i, std::move(usernames[i]));
        usernames[i].clear();
      }
    }

    loaded = true;
  }

  return users_;
}


#endif
