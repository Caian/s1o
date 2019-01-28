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
 * along with GNU Make; see the file COPYING.  If not, write to
 * the Free Software Foundation, 675 Mass Ave, Cambridge, MA 02139, USA.
 */

#include "test_rtree_disk.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <iostream>
#include <vector>
#include <cmath>
#include <set>

#include <stdlib.h>

namespace {

/**
 *
 */
DATASET_TEST(MultipleSlot, RIndexCustomSize)

    const int N = 400;
    const int slots = 3;

    test::meta_vector stuff;
    stuff.resize(N);

    for (int i = 0; i < N; i++) {

        float fi = static_cast<float>(i);
        float fN = static_cast<float>(N);
        float x = (float)((fN - 0.8f*fi)*cos(fi/100));
        float y = (float)((fN - 0.8f*fi)*sin(fi/100));

        stuff[i].uid = i+1;
        stuff[i].size = 21;
        stuff[i].x = 100 * x;
        stuff[i].y = 100 * y;
    }

    test::my_adapter meta_adapter;
    test::params_t params;
    test::my_dataset::TSpatialAdapterImpl spatial_adapter(
        params, 256ULL*1024*1024, 128ULL*1024*1024, 5);

    try {
        test::my_dataset dataset(dataset_name, 0, slots, stuff.begin(),
            stuff.end(), meta_adapter, spatial_adapter);

        ASSERT_TRUE(256ULL*1024*1024 == dataset.get_spatial_storage().
            _info.rfile_size_bytes);
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

/**
 *
 */
DATASET_TEST(MultipleSlot, RIndexRetry)

    const int N = 400000;
    const int slots = 3;

    test::meta_vector stuff;
    stuff.resize(N);

    for (int i = 0; i < N; i++) {

        float fi = static_cast<float>(i);
        float fN = static_cast<float>(N);
        float x = (float)((fN - 0.8f*fi)*cos(fi/100));
        float y = (float)((fN - 0.8f*fi)*sin(fi/100));

        stuff[i].uid = i+1;
        stuff[i].size = 21;
        stuff[i].x = 100 * x;
        stuff[i].y = 100 * y;
    }

    test::my_adapter meta_adapter;
    test::params_t params;
    test::my_dataset::TSpatialAdapterImpl spatial_adapter(
        params, 10ULL*1024*1024, 5ULL*1024*1024, 5);

    try {
        test::my_dataset dataset(dataset_name, 0, slots, stuff.begin(),
            stuff.end(), meta_adapter, spatial_adapter);

        ASSERT_TRUE(1 < dataset.get_spatial_storage()._info.rfile_attempts);
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

/**
 *
 */
DATASET_TEST(MultipleSlot, RIndexRetryFail)

    const int N = 400000;
    const int slots = 3;

    test::meta_vector stuff;
    stuff.resize(N);

    for (int i = 0; i < N; i++) {

        float fi = static_cast<float>(i);
        float fN = static_cast<float>(N);
        float x = (float)((fN - 0.8f*fi)*cos(fi/100));
        float y = (float)((fN - 0.8f*fi)*sin(fi/100));

        stuff[i].uid = i+1;
        stuff[i].size = 21;
        stuff[i].x = 100 * x;
        stuff[i].y = 100 * y;
    }

    test::my_adapter meta_adapter;
    test::params_t params;
    test::my_dataset::TSpatialAdapterImpl spatial_adapter(
        params, 1ULL*1024*1024, 1ULL*1024*1024, 5);

    ASSERT_THROW(test::my_dataset dataset(dataset_name, 0, slots,
        stuff.begin(), stuff.end(), meta_adapter, spatial_adapter),
        s1o::index_size_too_big_exception);

    // Remove the dataset
    ASSERT_NO_THROW(test::my_dataset::unlink(dataset_name));

    // Remove the dataset again because it shouldn't be an error
    ASSERT_NO_THROW(test::my_dataset::unlink(dataset_name));
}

/**
 *
 */
DATASET_TEST(SingleSlot, RIndexChanged)

    const int N = 400;
    const int slots = 1;

    test::meta_vector stuff;
    stuff.resize(N);

    for (int i = 0; i < N; i++) {

        float fi = static_cast<float>(i);
        float fN = static_cast<float>(N);
        float x = (float)((fN - 0.8f*fi)*cos(fi/100));
        float y = (float)((fN - 0.8f*fi)*sin(fi/100));

        stuff[i].uid = i+1;
        stuff[i].size = 21;
        stuff[i].x = 100 * x;
        stuff[i].y = 100 * y;
    }

    {
        ASSERT_NO_THROW(test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end()));
    }

    {
        ASSERT_NO_THROW(test::my_dataset dataset(dataset_name, 0, 0, slots));
    }

    try {
        test::my_dataset dataset(dataset_name, s1o::S1O_OPEN_WRITE,
            s1o::S1O_FLAGS_RWP | s1o::S1O_FLAGS_ALLOW_UNSORTED, slots);

        ASSERT_NO_THROW(dataset.push_element(stuff[0]));
    }
    catch (const std::exception& e) {
        std::cerr
            << boost::current_exception_diagnostic_information()
            << std::endl;
        FAIL();
    }

    {
        ASSERT_THROW(test::my_dataset dataset(dataset_name, 0, 0, slots),
            s1o::inconsistent_index_exception);
    }

    // Remove the dataset
    ASSERT_NO_THROW(test::my_dataset::unlink(dataset_name));

    // Remove the dataset again because it shouldn't be an error
    ASSERT_NO_THROW(test::my_dataset::unlink(dataset_name));
}

}
