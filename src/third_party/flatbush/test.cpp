#include <assert.h>
#include <stdio.h>
#include <time.h>
#include "flatbush.h"

int main(int argc, char** argv) {
	{
		// empty set
		flatbush::FlatBush<float>                   f;
		std::vector<flatbush::FlatBush<float>::Box> boxes;
		f.Reserve(0);
		f.Finish();
		if (f.Search(0, 0, 1, 1).size() != 0) {
			printf("expected empty search results\n");
			exit(1);
		}
	}
	{
		flatbush::FlatBush<float>                   f;
		std::vector<flatbush::FlatBush<float>::Box> boxes;
		int                                         dim = 100;
		f.Reserve(dim * dim);
		size_t index = 0;
		for (float x = 0; x < dim; x++) {
			for (float y = 0; y < dim; y++) {
				boxes.push_back({index, x + 0.1f, y + 0.1f, x + 0.9f, y + 0.9f});
				size_t checkIndex = f.Add(x + 0.1f, y + 0.1f, x + 0.9f, y + 0.9f);
				assert(checkIndex == index);
				index++;
			}
		}
		f.Finish();

		for (int i = 0; i < 1000; i++) {
			int   precision      = 100;
			int   maxQueryWindow = 5;
			float minx           = (float) (rand() % (dim * precision)) / (float) precision;
			float miny           = (float) (rand() % (dim * precision)) / (float) precision;
			float maxx           = minx + (float) (rand() % (maxQueryWindow * precision)) / (float) precision;
			float maxy           = miny + (float) (rand() % (maxQueryWindow * precision)) / (float) precision;
			auto  results        = f.Search(minx, miny, maxx, maxy);
			// brute force validation that there are no false negatives
			flatbush::FlatBush<float>::Box qbox = {0, minx, miny, maxx, maxy};
			for (auto box : boxes) {
				if (box.PositiveUnion(qbox)) {
					// if object crosses the query rectangle, then it should be included in the result set
					size_t k = 0;
					for (; k < results.size(); k++) {
						if (results[k] == box.Index)
							break;
					}
					if (k == results.size()) {
						printf("error\n");
						exit(1);
					}
				}
			}
		}
		printf("Validation OK\n");
	}
	{
		printf("Benchmark:\n");
		flatbush::FlatBush<float> f;
		int                       dim   = 1000;
		auto                      start = clock();
		f.Reserve(dim * dim);
		for (float x = 0; x < dim; x++) {
			for (float y = 0; y < dim; y++)
				f.Add(x + 0.1, y + 0.1, x + 0.9, y + 0.9);
		}
		f.Finish();
		printf("Time to insert %d elements: %.0f milliseconds\n", dim * dim, 1000 * (clock() - start) / (double) CLOCKS_PER_SEC);

		start                      = clock();
		size_t              nquery = 10 * 1000 * 1000;
		std::vector<size_t> results;
		int                 sx       = 0;
		int                 sy       = 0;
		int64_t             nresults = 0;
		for (size_t i = 0; i < nquery; i++) {
			results.clear();
			float minx = sx % dim;
			float miny = sy % dim;
			float maxx = minx + 3.0;
			float maxy = miny + 3.0;
			f.Search(minx, miny, maxx, maxy, results);
			nresults += results.size();
		}
		printf("Time per query, returning average of %.0f elements: %.2f nanoseconds\n", (double) nresults / (double) nquery, (1000000000.0 / (double) nquery) * (clock() - start) / (double) CLOCKS_PER_SEC);
	}
	return 0;
}
