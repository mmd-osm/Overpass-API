# C++ port of mourner/flatbush

This is a straightforward port of https://github.com/mourner/flatbush.

To test: `clang++ --std=c++11 -Wall -O2 -o test test.cpp && ./test`

Example:
```cpp
flatbush::FlatBush<float> fb;
fb.Reserve(items.size()); // this is optional, to avoid heap allocs as the array grows during insertion
for (const auto& item: items) {
    fb.Add(item.MinX(), item.MinY(), item.MaxX(), item.MaxY());
}
fb.Finish();

// query
auto results = fb.Search(minx, miny, maxx, maxy);
// results are an array of indices. The index starts at zero, and is the order in which you inserted
// elements into the tree.
```

## Performance

These results are from an i7-4770 @ 3.4GHz, clang 3.8, x64

* Inserting 1,000,000 rectangles: 98 ms
* Query that retrieves 7 results: 150 ns
* Query that retrieves 25 results: 350 ns