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
DATASET_TEST(MultipleSlot, QueryNearestMeta)

    const int N = 4000;
    const int slots = 3;

    unsigned int seed = 123456;

    test::meta_vector stuff;
    stuff.resize(N);

    test::data_vector stuff_data;
    stuff_data.resize(N);

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

        stuff_data[i].resize(slots);

        for (size_t j = 0; j < slots; j++) {

            stuff_data[i][j].resize(size);

            for (int k = 0; k < size; k++) {
                stuff_data[i][j][k] = static_cast<char>(rand_r(&seed));
            }
        }
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        for (int i = 0; i < N; i++) {
            for (int j = 0; j < slots; j++) {

                char* data = dataset.get_data(i+1, j);

                std::copy(stuff_data[i][j].begin(),
                    stuff_data[i][j].end(), data);
            }
        }

        typedef typename s1o::traits::spatial_point_type<
            test::my_dataset
            >::type point;

        point point1(1.4562e-3f, -0.526e-2f);
        point point2;

        const size_t Who = 1337;

        ASSERT_NO_THROW(dataset.get_meta_adapter().
            get_location(stuff[Who], point2));

        test::my_metadata meta;

        ASSERT_THROW(dataset.find_metadata(point1),
            s1o::location_mismatch_exception);

        ASSERT_NO_THROW(meta = dataset.find_metadata(point2));

        ASSERT_TRUE(stuff[Who] == meta);
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
DATASET_TEST(MultipleSlot, QueryNearestElem)

    const int N = 4000;
    const int slots = 3;

    unsigned int seed = 123456;

    test::meta_vector stuff;
    stuff.resize(N);

    test::data_vector stuff_data;
    stuff_data.resize(N);

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

        stuff_data[i].resize(slots);

        for (size_t j = 0; j < slots; j++) {

            stuff_data[i][j].resize(size);

            for (int k = 0; k < size; k++) {
                stuff_data[i][j][k] = static_cast<char>(rand_r(&seed));
            }
        }
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        for (int i = 0; i < N; i++) {
            for (int j = 0; j < slots; j++) {

                char* data = dataset.get_data(i+1, j);

                std::copy(stuff_data[i][j].begin(),
                    stuff_data[i][j].end(), data);
            }
        }

        typedef typename s1o::traits::spatial_point_type<
            test::my_dataset
            >::type point;

        point point1(1.4562e-3f, -0.526e-2f);
        point point2;

        const size_t Who = 1337;

        ASSERT_NO_THROW(dataset.get_meta_adapter().
            get_location(stuff[Who], point2));

        test::my_dataset::element_pair elem;

        ASSERT_THROW(dataset.find_element(point1),
            s1o::location_mismatch_exception);

        ASSERT_NO_THROW(elem = dataset.find_element(point2));

        std::pair<std::vector<char>::iterator, const char*> comp;
        comp = std::mismatch(stuff_data[Who][0].begin(),
            stuff_data[Who][0].end(), elem.second);

        ASSERT_TRUE(stuff[Who] == *elem.first);
        ASSERT_TRUE(stuff_data[Who][0].end() == comp.first);
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
DATASET_TEST(MultipleSlot, QueryNearestElemSlot)

    const int N = 4000;
    const int slots = 3;

    unsigned int seed = 123456;

    test::meta_vector stuff;
    stuff.resize(N);

    test::data_vector stuff_data;
    stuff_data.resize(N);

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

        stuff_data[i].resize(slots);

        for (size_t j = 0; j < slots; j++) {

            stuff_data[i][j].resize(size);

            for (int k = 0; k < size; k++) {
                stuff_data[i][j][k] = static_cast<char>(rand_r(&seed));
            }
        }
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        for (int i = 0; i < N; i++) {
            for (int j = 0; j < slots; j++) {

                char* data = dataset.get_data(i+1, j);

                std::copy(stuff_data[i][j].begin(),
                    stuff_data[i][j].end(), data);
            }
        }

        typedef typename s1o::traits::spatial_point_type<
            test::my_dataset
            >::type point;

        point point1(1.4562e-3f, -0.526e-2f);
        point point2;

        const size_t Who = 1337;

        ASSERT_NO_THROW(dataset.get_meta_adapter().
            get_location(stuff[Who], point2));

        test::my_dataset::element_pair elem;

        ASSERT_THROW(dataset.find_element(point1, slots),
            s1o::invalid_slot_exception);

        ASSERT_THROW(dataset.find_element(point1, 2),
            s1o::location_mismatch_exception);

        ASSERT_THROW(dataset.find_element(point2, slots),
            s1o::invalid_slot_exception);

        ASSERT_NO_THROW(elem = dataset.find_element(point2, 2));

        std::pair<std::vector<char>::iterator, const char*> comp;
        comp = std::mismatch(stuff_data[Who][2].begin(),
            stuff_data[Who][2].end(), elem.second);

        ASSERT_TRUE(stuff[Who] == *elem.first);
        ASSERT_TRUE(stuff_data[Who][2].end() == comp.first);
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
