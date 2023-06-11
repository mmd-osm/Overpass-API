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

#ifndef DE__OSM3S___TEMPLATE_DB__RANGES_H
#define DE__OSM3S___TEMPLATE_DB__RANGES_H

#include <set>


template< typename Index >
class Ranges
{
public:
  Ranges() = default;
  Ranges(const std::set< std::pair< Index, Index > >& data_) : data(data_) {}
  Ranges(std::set< std::pair< Index, Index > >&& data_) : data(std::move(data_)) {}
  Ranges(Index begin, Index end) : data({{ begin, end }}) {}

  Ranges(const Ranges< Index> & rng) = default;
  Ranges(Ranges< Index> & rng) = default;
  Ranges(Ranges< Index> && rng) = default;

  Ranges< Index>& operator=(Ranges< Index>&&) = default;
  Ranges< Index>& operator=(const Ranges< Index>&) = default;

  class Iterator
  {
  public:
    Iterator(typename std::set< std::pair< Index, Index > >::const_iterator it_) : it(it_) {}
    Iterator() = default;
    const Index& lower_bound() const { return it->first; }
    const Index& upper_bound() const { return it->second; }
    const std::pair< Index, Index >& operator*() const { return *it; }
    const Iterator& operator++()
    {
      ++it;
      return *this;
    }
    bool operator==(const Iterator& rhs) const { return it == rhs.it; }
    bool operator!=(const Iterator& rhs) const { return it != rhs.it; }
  private:
    typename std::set< std::pair< Index, Index > >::const_iterator it; 
  };
  
  Iterator begin() const { return Iterator(data.begin()); }
  Iterator end() const { return Iterator(data.end()); }
  bool empty() const { return data.empty(); }
  
  Ranges intersect(const Ranges< Index >& rhs) const;
//  Ranges union_(const Ranges& rhs) const;
  Ranges skip_start(Index lower_bound);
  void swap(Ranges& rhs) { data.swap(rhs.data); }

  void sort();

  void push_back(Index begin, Index end)
  {
    data.insert({ begin, end });
  }

  bool is_global() const
  {
    return (data.size() == 1 &&
            data.begin()->first == Index::min() &&
            data.begin()->second == Index::max());
  }

  static Ranges global() { return Ranges(Index::min(), Index::max()); }

  const std::set< std::pair< Index, Index > > & get_ranges() const { return data; }

private:
  std::set< std::pair< Index, Index > > data;
};


template< typename Index >
Ranges< Index > Ranges< Index >::skip_start(Index lower_bound)
{
  Ranges< Index > result;
  for (const auto& i : data)
  {
    if (lower_bound < i.second)
      result.data.insert({ std::max(i.first, lower_bound), i.second });
  }
  return result;
}


template< typename Index >
Ranges< Index > Ranges< Index >::intersect(const Ranges< Index >& rhs) const
{
  if (rhs.is_global())
    return *this;
  else if (this->is_global())
    return rhs;

  Ranges< Index > result;
  auto it_a = data.begin();
  auto it_b = rhs.data.begin();

  while (it_a != data.end() && it_b != rhs.data.end())
  {
    if (!(it_a->first < it_b->second))
      ++it_b;
    else if (!(it_b->first < it_a->second))
      ++it_a;
    else if (it_b->second < it_a->second)
    {
      result.data.insert({std::max(it_a->first, it_b->first), it_b->second});
      ++it_b;
    }
    else
    {
      result.data.insert({std::max(it_a->first, it_b->first), it_a->second});
      ++it_a;
    }
  }

  return result;
}

//template< typename Index >
//Ranges< Index > Ranges< Index >::union_(const Ranges< Index >& rhs) const
//{
//  Ranges< Index > result;
//
//  auto it_a = data.begin();
//  auto it_b = rhs.data.begin();
//
//  while (it_a != data.end() && it_b != rhs.data.end())
//  {
//    if (it_a->first < it_b->first)
//    {
//      result.data.push_back(*it_a);
//      ++it_a;
//    }
//    else
//    {
//      result.data.push_back(*it_b);
//      ++it_b;
//    }
//  }
//  while (it_a != data.end())
//  {
//    result.data.push_back(*it_a);
//    ++it_a;
//  }
//  while (it_b != rhs.data.end())
//  {
//    result.data.push_back(*it_b);
//    ++it_b;
//  }
//
//  //result.sort();
//  return result;
//}

template< typename Index >
void Ranges< Index >::sort()
{
  if (!data.empty())
  {
    decltype(data) result;
    auto it = data.begin();
    std::pair< Index, Index > buf = *it;
    for (++it; it != data.end(); ++it)
    {
      if (buf.second < it->first)
      {
        result.insert(buf);
        buf = *it;
      }
      else
        buf.second = std::max(buf.second, it->second);
    }
    result.insert(buf);
    result.swap(data);
  }
}




#endif
