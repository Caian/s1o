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

#include "test_rtree_disk_slim_mi.hpp"

#include <boost/fusion/sequence/intrinsic/at_c.hpp>
#include <boost/fusion/include/at_c.hpp>

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
DATASET_TEST(CreateFromExistingSingleSlot, SecondBoundaries)

    const int N = 5000;
    const int slots = 1;

    unsigned int seed = 123456;

    test::meta_vector stuff;
    stuff.resize(N);

    short min1 = 0;
    short max1 = 0;
    char min2 = 0;
    char max2 = 0;

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
        stuff[i].value1 = static_cast<short>(i);
        stuff[i].value2 = static_cast<char>(i);

        if (i == 0) {
            min1 = stuff[i].value1;
            max1 = stuff[i].value1;
            min2 = stuff[i].value2;
            max2 = stuff[i].value2;
        }
        else {
            min1 = std::min(min1, stuff[i].value1);
            max1 = std::max(max1, stuff[i].value1);
            min2 = std::min(min2, stuff[i].value2);
            max2 = std::max(max2, stuff[i].value2);
        }
    }

    try {
        test::my_dataset dataset(dataset_name,
            0, slots, stuff.begin(), stuff.end());

        test::secondary_adapter_impl::spatial_point_type minpoint;
        test::secondary_adapter_impl::spatial_point_type maxpoint;

        dataset.secondary_bounds(minpoint, maxpoint);

        ASSERT_TRUE(min1 == boost::fusion::at_c<0>(minpoint).value);
        ASSERT_TRUE(max1 == boost::fusion::at_c<0>(maxpoint).value);
        ASSERT_TRUE(min2 == boost::fusion::at_c<1>(minpoint).value);
        ASSERT_TRUE(max2 == boost::fusion::at_c<1>(maxpoint).value);
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
