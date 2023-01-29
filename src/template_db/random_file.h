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

#ifndef DE__OSM3S___TEMPLATE_DB__RANDOM_FILE_H
#define DE__OSM3S___TEMPLATE_DB__RANDOM_FILE_H

#include "random_file_index.h"
#include "types.h"
#include "lz4_wrapper.h"
#include "zlib_wrapper.h"

#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <map>
#include <memory>
#include <vector>


template< typename Key, typename Value >
struct Random_File
{
public:
  Random_File(const Random_File& f) = delete;
  Random_File(Random_File_Index*);
  ~Random_File();

  Value get(const Key& pos);
  void put(const Key& pos, const Value& index);

private:
  bool changed;
  const uint32 index_size;
  const uint32 compression_factor;

  Raw_File val_file;
  Random_File_Index* index;
  std::unique_ptr<uint8[]> cache;
  uint32 cache_pos;
  const uint32 block_size;
  const uint32 cache_window_size;

  std::unique_ptr<uint8[]> buffer;

  bool is_outside_cache_window(uint32 pos) const noexcept;
  void move_cache_window(uint32 pos);
  uint32 allocate_block(uint32 data_size);
};


/** Implementation Random_File: ---------------------------------------------*/


template< typename Key, typename Value >
Random_File< Key, Value >::Random_File(Random_File_Index* index_)
  : changed(false), index_size(Value::max_size_of()),
  compression_factor(index_->get_compression_factor()),
  val_file(index_->get_map_file_name(),
	   index_->writeable() ? O_RDWR|O_CREAT : O_RDONLY,
	   S_666, "Random_File:3"),
  index(index_),
  cache(new uint8[index_->get_block_size() * index_->get_compression_factor()]),
  cache_pos(index->npos),
  block_size(index_->get_block_size()),
  cache_window_size((block_size*compression_factor /index_size)),
  buffer(new uint8[index_->get_block_size() * index_->get_compression_factor() * 2])  // increased buffer size for lz4
{
}


template< typename Key, typename Value >
Random_File< Key, Value >::~Random_File()
{
  move_cache_window(index->npos);
  //delete index;
}


template< typename Key, typename Value >
Value Random_File< Key, Value >::get(const Key& pos)
{
  if (is_outside_cache_window(pos.val() / cache_window_size)) {
    move_cache_window(pos.val() / cache_window_size);
  }
  return Value(cache.get() + (pos.val() % cache_window_size)*index_size);
}


template< typename Key, typename Value >
void Random_File< Key, Value >::put(const Key& pos, const Value& val)
{
  if (!index->writeable())
    throw File_Error(0, index->get_map_file_name(), "Random_File:2");

  if (is_outside_cache_window(pos.val() / cache_window_size)) {
    move_cache_window(pos.val() / cache_window_size);
  }
  val.to_data(cache.get() + (pos.val() % cache_window_size)*index_size);
  changed = true;
}

template< typename Key, typename Value >
bool Random_File< Key, Value >::is_outside_cache_window(uint32 pos) const noexcept
{
  // The cache already contains the needed position.
  if ((pos == cache_pos) && (cache_pos != index->npos))
    return false;
  return true;
}


template< typename Key, typename Value >
void Random_File< Key, Value >::move_cache_window(uint32 pos)
{
  if (!(is_outside_cache_window(pos)))
    return;

  if (pos != index->npos && pos >= 256*1024*1024/Value::max_size_of())
    throw File_Error(0, index->get_map_file_name(), "Random_File: id too large for map file");

  if (changed)
  {
    uint32 data_size = compression_factor;
    void* target = cache.get();

    if (index->get_compression_method() == Block_Compression::ZLIB_COMPRESSION)
    {
      target = buffer.get();
      uint32 compressed_size = Zlib_Deflate(1)
          .compress(cache.get(), block_size * compression_factor, target, block_size * index->get_compression_factor());
      data_size = (compressed_size - 1) / block_size + 1;
      zero_padding((uint8*)target + compressed_size, block_size * data_size - compressed_size);
    }
    else if (index->get_compression_method() == Block_Compression::LZ4_COMPRESSION)
    {
      target = buffer.get();
      uint32 compressed_size = LZ4_Deflate()
          .compress(cache.get(), block_size * compression_factor, target, block_size * index->get_compression_factor() * 2);
      data_size = (compressed_size - 1) / block_size + 1;
      zero_padding((uint8*)target + compressed_size, block_size * data_size - compressed_size);
    }

    uint32 disk_pos = allocate_block(data_size);

    // Save the found position to the index.
    if (index->get_blocks().size() <= cache_pos)
      index->get_blocks().resize(cache_pos+1, Random_File_Index_Entry(index->npos, 1));
    Random_File_Index_Entry entry(disk_pos, data_size);
    index->get_blocks()[cache_pos] = entry;

    // Write the data at the found position.
    val_file.seek((int64)disk_pos*block_size, "Random_File:21");
    val_file.write((uint8*)target, (uint64) block_size * data_size, "Random_File:22");
  }
  changed = false;

  if (pos == index->npos)
    return;

  if ((index->get_blocks().size() <= pos) || (index->get_blocks()[pos].pos == index->npos))
  {
    // Reset the whole cache to zero.
    zero_padding(cache.get(), block_size * compression_factor);
  }
  else
  {
    val_file.seek((int64)(index->get_blocks()[pos].pos)*block_size, "Random_File:23");
    if (index->get_compression_method() == Block_Compression::NO_COMPRESSION)
      val_file.read(cache.get(), (uint64) block_size * index->get_blocks()[pos].size, "Random_File:24");
    else if (index->get_compression_method() == Block_Compression::ZLIB_COMPRESSION)
    {
      val_file.read(buffer.get(), (uint64) block_size * index->get_blocks()[pos].size, "Random_File:25");
      Zlib_Inflate().decompress
          (buffer.get(), block_size * index->get_blocks()[pos].size, cache.get(), block_size * index->get_compression_factor());
    }
    else if (index->get_compression_method() == Block_Compression::LZ4_COMPRESSION)
    {
      val_file.read(buffer.get(), (uint64) block_size * index->get_blocks()[pos].size, "Random_File:26");
      LZ4_Inflate().decompress
          (buffer.get(), block_size * index->get_blocks()[pos].size, cache.get(), block_size * index->get_compression_factor());
    }
  }
  cache_pos = pos;
}


// Finds an appropriate block, removes it from the list of available blocks, and returns it
template< typename Key, typename Value >
uint32 Random_File< Key, Value >::allocate_block(uint32 data_size)
{
  uint32 result = this->index->block_count;

  if (this->index->get_void_blocks().empty())
    this->index->block_count += data_size;
  else
  {
    auto pos_it
    = std::lower_bound(this->index->get_void_blocks().begin(), this->index->get_void_blocks().end(),
        std::make_pair(data_size, uint32(0)));

    if (pos_it != this->index->get_void_blocks().end() && pos_it->first == data_size)
    {
      // We have a gap of exactly the needed size.
      result = pos_it->second;
      this->index->get_void_blocks().erase(pos_it);
    }
    else
    {
      pos_it = --(this->index->get_void_blocks().end());
      uint32 last_size = pos_it->first;
      while (pos_it != this->index->get_void_blocks().begin() && last_size > data_size)
      {
        --pos_it;
        if (last_size == pos_it->first)
        {
          // We have a gap size that appears twice (or more often).
          // This is a good heuristic choice.
          result = pos_it->second;
          pos_it->first -= data_size;
          pos_it->second += data_size;
          rearrange_block(this->index->get_void_blocks().begin(), pos_it, *pos_it);
          return result;
        }
        last_size = pos_it->first;
      }

      pos_it = --(this->index->get_void_blocks().end());
      if (pos_it->first >= data_size)
      {
        // If no really matching block exists then we choose the largest one.
        result = pos_it->second;
        pos_it->first -= data_size;
        pos_it->second += data_size;
        rearrange_block(this->index->get_void_blocks().begin(), pos_it, *pos_it);
      }
      else
        this->index->block_count += data_size;
    }
  }

  return result;
}


#endif
