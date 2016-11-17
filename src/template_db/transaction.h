/** Copyright 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016 Roland Olbricht et al.
 *
 * This file is part of Template_DB.
 *
 * Template_DB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * Template_DB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Overpass_API.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef DE__OSM3S___TEMPLATE_DB__TRANSACTION_H
#define DE__OSM3S___TEMPLATE_DB__TRANSACTION_H

#include "random_file.h"

#include <map>
#include <mutex>
#include <vector>


class Index_Cache
{


private:
  std::map< const File_Properties*, File_Blocks_Index_Base* >
    data_files;
  std::map< const File_Properties*, Random_File_Index* >
    random_files;

  friend class Nonsynced_Transaction;

};


class Transaction
{
  public:
    virtual ~Transaction() {}
    virtual File_Blocks_Index_Base* data_index(const File_Properties*) = 0;
    virtual Random_File_Index* random_index(const File_Properties*) = 0;
    virtual std::string get_db_dir() const = 0;
};


class Nonsynced_Transaction : public Transaction
{
  public:
  Nonsynced_Transaction
      (bool writeable, bool use_shadow,
       const std::string& db_dir, const std::string& file_name_extension);

  Nonsynced_Transaction
        (bool writeable, bool use_shadow,
	 const std::string& db_dir, const std::string& file_name_extension,
	 Index_Cache* ic);
    virtual ~Nonsynced_Transaction();
    
    File_Blocks_Index_Base* data_index(const File_Properties*);
    Random_File_Index* random_index(const File_Properties*);
    
    void flush();
    std::string get_db_dir() const { return db_dir; }
    
  private:
    std::map< const File_Properties*, File_Blocks_Index_Base* >
      data_files;
    std::map< const File_Properties*, Random_File_Index* >
      random_files;
    bool writeable, use_shadow;
    std::string file_name_extension, db_dir;
    std::mutex transaction_mutex;
    Index_Cache* ic;
};


inline Nonsynced_Transaction::Nonsynced_Transaction
    (bool writeable_, bool use_shadow_,
     const std::string& db_dir_, const std::string& file_name_extension_)
  : writeable(writeable_), use_shadow(use_shadow_),
    file_name_extension(file_name_extension_), db_dir(db_dir_), ic(nullptr) {}


inline Nonsynced_Transaction::Nonsynced_Transaction
    (bool writeable_, bool use_shadow_,
     const std::string& db_dir_, const std::string& file_name_extension_,
     Index_Cache* ic_)
  : writeable(writeable_), use_shadow(use_shadow_),
    file_name_extension(file_name_extension_), db_dir(db_dir_), ic(ic_) {}

    
inline Nonsynced_Transaction::~Nonsynced_Transaction()
{
  flush();
}


inline void Nonsynced_Transaction::flush()
{

  std::lock_guard<std::mutex> guard(transaction_mutex);

  if (ic != nullptr)
  {
    for (std::map< const File_Properties*, File_Blocks_Index_Base* >::iterator
        it = data_files.begin(); it != data_files.end(); ++it)
      delete it->second;
    data_files.clear();
    for (std::map< const File_Properties*, Random_File_Index* >::iterator
        it = random_files.begin(); it != random_files.end(); ++it)
      delete it->second;
    random_files.clear();
  }
}


inline File_Blocks_Index_Base* Nonsynced_Transaction::data_index
    (const File_Properties* fp)
{ 
  std::lock_guard<std::mutex> guard(transaction_mutex);

  std::map< const File_Properties*, File_Blocks_Index_Base* > * df;

  if (ic != nullptr)
    df = &ic->data_files;
  else
    df = &data_files;

  std::map< const File_Properties*, File_Blocks_Index_Base* >::iterator
      it = df->find(fp);
  if (it != df->end())
    return it->second;

  File_Blocks_Index_Base* data_index = fp->new_data_index
      (writeable, use_shadow, db_dir, file_name_extension);
  if (data_index != 0)
    df->operator [](fp) = data_index;
  return data_index;
}


inline Random_File_Index* Nonsynced_Transaction::random_index(const File_Properties* fp)
{ 
  std::lock_guard<std::mutex> guard(transaction_mutex);

  std::map< const File_Properties*, Random_File_Index* > * rf;

  if (ic != nullptr)
    rf = &ic->random_files;
  else
    rf = &random_files;


  std::map< const File_Properties*, Random_File_Index* >::iterator
      it = rf->find(fp);
  if (it != rf->end())
    return it->second;
  
  rf->operator [](fp) = new Random_File_Index(*fp, writeable, use_shadow, db_dir, file_name_extension);
  return rf->operator [](fp);
}


#endif
