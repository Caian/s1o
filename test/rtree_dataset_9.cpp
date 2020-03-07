/*
 * Copyright (C) 2019 Caian Benedicto <caianbene@gmail.com>
 *
 * This file is part of s1o.
 *
 * s1o is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * s1o is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with s1o.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "test_rtree.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <iostream>
#include <vector>
#include <cmath>

#include <stdlib.h>

namespace {

/**
 *
 */
DATASET_TEST(CreateFromExistingSingleSlot, Boundaries)

    const int N = 5000;
    const int slots = 1;

    unsigned int seed = 123456;

    test::meta_vector stuff;
    stuff.resize(N);

    float minx = 0;
    float maxx = 0;
    float miny = 0;
    float maxy = 0;

    for (int i = 0; i < N; i++) {

        int size = 33 * i + 1;

        float fi = static_cast<float>(i);
        float fN = static_cast<float>(N);
        float x = (float)((fN - 0.8f*fi)*cos(fi/100));
        float y = (float)((fN - 0.8f*fi)*sin(fi/100));

        stuff[i].uid = i+1;
        stuff[i].size = size;
        stuff[i].x = 100 * x;
        stuff[i].y = 100 * y;

        if (i == 0) {
            minx = stuff[i].x;
            maxx = stuff[i].x;
            miny = stuff[i].y;
            maxy = stuff[i].y;
        }
        else {
            minx = std::min(minx, stuff[i].x);
            maxx = std::max(maxx, stuff[i].x);
            miny = std::min(miny, stuff[i].y);
            maxy = std::max(maxy, stuff[i].y);
        }
    }

    try {
        test::my_dataset dataset(dataset_name,
            0, slots, stuff.begin(), stuff.end());

        test::my_dataset::spatial_point_type minpoint;
        test::my_dataset::spatial_point_type maxpoint;

        dataset.bounds(minpoint, maxpoint);

        ASSERT_TRUE(minx == minpoint.template get<0>());
        ASSERT_TRUE(maxx == maxpoint.template get<0>());
        ASSERT_TRUE(miny == minpoint.template get<1>());
        ASSERT_TRUE(maxy == maxpoint.template get<1>());
    }
    catch (const std::exception& e) {
        std::cerr
            << boost::current_exception_diagnostic_information()
            << std::endl;
        FAIL();
    }

    // Remove the dataset
    ASSERT_NO_THROW(test::my_dataset::unlink(dataset_name));

    // Remove the dataset again because it shouldn't be an error
    ASSERT_NO_THROW(test::my_dataset::unlink(dataset_name));
}

}
