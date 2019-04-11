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

#include "test_rtree_disk_slim_mi.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <iostream>
#include <iterator>
#include <vector>
#include <cmath>
#include <set>

#include <stdlib.h>

namespace {

/**
 *
 */
DATASET_TEST(MultipleSlot, SecondQueryRangeMetaAllTightIndex1)

    const int X = 200;
    const int Y = 200;
    const int N = X*Y;
    const int slots = 3;

    test::meta_vector stuff;
    stuff.resize(N);

    {
        int i = 0;
        for (int iy = 0; iy < Y; iy++) {
            for (int ix = 0; ix < X; ix++, i++) {

                stuff[i].uid = i+1;
                stuff[i].size = 10;
                stuff[i].x = -100.0f * (ix + 1);
                stuff[i].y = 100.0f * (iy + 1);
                stuff[i].value1 = static_cast<short>(i-20000);
                stuff[i].value2 = 0;
            }
        }
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename test::my_dataset::template
            secondary_q_iterator<
            test::value1_index
            >::meta_type index_iterator;

        test::value1_index point1(-20000);
        test::value1_index point2(N-20000-1);

        index_iterator it, begin, end;

        ASSERT_NO_THROW(boost::tie(begin, end) =
            dataset.secondary_query_metadata(
            s1o::queries::make_closed_interval(point1, point2)));

        std::set<int> found_uids;

        for (it = begin; it != end; it++) {

            const test::my_metadata& meta = *it;
            ASSERT_TRUE(found_uids.insert(meta.uid).second);
        }

        ASSERT_TRUE(found_uids.size() == N);
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
DATASET_TEST(MultipleSlot, SecondQueryRangeMetaOneTightIndex1)

    const int X = 200;
    const int Y = 200;
    const int N = X*Y;
    const int slots = 3;

    test::meta_vector stuff;
    stuff.resize(N);

    {
        int i = 0;
        for (int iy = 0; iy < Y; iy++) {
            for (int ix = 0; ix < X; ix++, i++) {

                stuff[i].uid = i+1;
                stuff[i].size = 10;
                stuff[i].x = -100.0f * (ix + 1);
                stuff[i].y = 100.0f * (iy + 1);
                stuff[i].value1 = static_cast<short>(i-20000);
                stuff[i].value2 = 0;
            }
        }
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename test::my_dataset::template
            secondary_q_iterator<
            test::value1_index
            >::meta_type index_iterator;

        const size_t Who = 1234;

        test::value1_index point1(stuff[Who].value1);
        test::value1_index point2(stuff[Who].value1);

        index_iterator it, begin, end;

        ASSERT_NO_THROW(boost::tie(begin, end) =
            dataset.secondary_query_metadata(
            s1o::queries::make_closed_interval(point1, point2)));

        std::set<int> found_uids;

        for (it = begin; it != end; it++) {

            const test::my_metadata& meta = *it;
            ASSERT_TRUE(found_uids.insert(meta.uid).second);
        }

        ASSERT_TRUE(found_uids.size() == 1);
        ASSERT_TRUE(*found_uids.begin() == (Who + 1));
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
DATASET_TEST(MultipleSlot, SecondQueryRangeMetaComplexIndex1)

    const int N = 40000;
    const int slots = 3;

    test::meta_vector stuff;
    stuff.resize(N);

    for (int i = 0; i < N; i++) {

        float fi = static_cast<float>(i);
        float fN = static_cast<float>(N);
        float x = (float)((1.0f + fi/fN)*cos(fi/1000.0f));
        float y = (float)((1.0f + fi/fN)*sin(fi/1000.0f));

        stuff[i].uid = i+1;
        stuff[i].size = 10;
        stuff[i].x = 1000.0f * x;
        stuff[i].y = 1000.0f * y;
        stuff[i].value1 = static_cast<short>(i-20000);
        stuff[i].value2 = 0;
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename test::my_dataset::template
            secondary_q_iterator<
            test::value1_index
            >::meta_type index_iterator;

        test::value1_index point1(-5);
        test::value1_index point2(15);

        index_iterator it, begin, end;

        ASSERT_NO_THROW(boost::tie(begin, end) =
            dataset.secondary_query_metadata(
            s1o::queries::make_closed_interval(point1, point2)));

        std::set<int> found_uids;

        for (it = begin; it != end; it++) {

            const test::my_metadata& meta = *it;
            ASSERT_TRUE(found_uids.insert(meta.uid).second);
        }

        ASSERT_TRUE(found_uids.size() > 0);

        for (int i = 0; i < N; i++) {

            const test::my_metadata& meta = stuff[i];

            short minval = point1.value;
            short maxval = point2.value;

            if (found_uids.find(meta.uid) != found_uids.end()) {

                ASSERT_TRUE(minval <= meta.value1);
                ASSERT_TRUE(maxval >= meta.value1);
            }
            else {

                ASSERT_FALSE(minval <= meta.value1 && \
                             maxval >= meta.value1);
            }
        }
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
DATASET_TEST(MultipleSlot, SecondQueryRangeMetaComplexEmptyIndex1)

    const int N = 40000;
    const int slots = 3;

    test::meta_vector stuff;
    stuff.resize(N);

    for (int i = 0; i < N; i++) {

        float fi = static_cast<float>(i);
        float fN = static_cast<float>(N);
        float x = (float)((1.0f + fi/fN)*cos(fi/1000.0f));
        float y = (float)((1.0f + fi/fN)*sin(fi/1000.0f));

        stuff[i].uid = i+1;
        stuff[i].size = 10;
        stuff[i].x = x;
        stuff[i].y = y;
        stuff[i].value1 = static_cast<short>(i-20000);
        stuff[i].value2 = 0;
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename test::my_dataset::template
            secondary_q_iterator<
            test::value1_index
            >::meta_type index_iterator;

        test::value1_index point1(-30000);
        test::value1_index point2(-20001);

        index_iterator it, begin, end;

        ASSERT_NO_THROW(boost::tie(begin, end) =
            dataset.secondary_query_metadata(
            s1o::queries::make_closed_interval(point1, point2)));

        ASSERT_TRUE(std::distance(begin, end) == 0);
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

///////////////////////////////////////////////////////////////////////////////

/**
 *
 */
DATASET_TEST(MultipleSlot, SecondQueryRangeElemAllTightIndex1)

    const int X = 200;
    const int Y = 200;
    const int N = X*Y;
    const int slots = 3;

    test::meta_vector stuff;
    stuff.resize(N);

    {
        int i = 0;
        for (int iy = 0; iy < Y; iy++) {
            for (int ix = 0; ix < X; ix++, i++) {

                stuff[i].uid = i+1;
                stuff[i].size = 10;
                stuff[i].x = -100.0f * (ix + 1);
                stuff[i].y = 100.0f * (iy + 1);
                stuff[i].value1 = static_cast<short>(i-20000);
                stuff[i].value2 = 0;
            }
        }
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename test::my_dataset::template
            secondary_q_iterator<
            test::value1_index
            >::elem_type index_iterator;

        test::value1_index point1(-20000);
        test::value1_index point2(N-20000-1);

        index_iterator it, begin, end;

        ASSERT_NO_THROW(boost::tie(begin, end) =
            dataset.secondary_query_elements(
            s1o::queries::make_closed_interval(point1, point2)));

        std::set<int> found_uids;

        for (it = begin; it != end; it++) {

            const test::my_metadata& meta = *it->first;
            ASSERT_TRUE(found_uids.insert(meta.uid).second);
        }

        ASSERT_TRUE(found_uids.size() == N);
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
DATASET_TEST(MultipleSlot, SecondQueryRangeElemOneTightIndex1)

    const int X = 200;
    const int Y = 200;
    const int N = X*Y;
    const int slots = 3;

    test::meta_vector stuff;
    stuff.resize(N);

    {
        int i = 0;
        for (int iy = 0; iy < Y; iy++) {
            for (int ix = 0; ix < X; ix++, i++) {

                stuff[i].uid = i+1;
                stuff[i].size = 10;
                stuff[i].x = -100.0f * (ix + 1);
                stuff[i].y = 100.0f * (iy + 1);
                stuff[i].value1 = static_cast<short>(i-20000);
                stuff[i].value2 = 0;
            }
        }
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename test::my_dataset::template
            secondary_q_iterator<
            test::value1_index
            >::elem_type index_iterator;

        const size_t Who = 1234;

        test::value1_index point1(stuff[Who].value1);
        test::value1_index point2(stuff[Who].value1);

        index_iterator it, begin, end;

        ASSERT_NO_THROW(boost::tie(begin, end) =
            dataset.secondary_query_elements(
            s1o::queries::make_closed_interval(point1, point2)));

        std::set<int> found_uids;

        for (it = begin; it != end; it++) {

            const test::my_metadata& meta = *it->first;
            ASSERT_TRUE(found_uids.insert(meta.uid).second);
        }

        ASSERT_TRUE(found_uids.size() == 1);
        ASSERT_TRUE(*found_uids.begin() == (Who + 1));
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
DATASET_TEST(MultipleSlot, SecondQueryRangeElemComplexIndex1)

    const int N = 40000;
    const int slots = 3;

    test::meta_vector stuff;
    stuff.resize(N);

    for (int i = 0; i < N; i++) {

        float fi = static_cast<float>(i);
        float fN = static_cast<float>(N);
        float x = (float)((1.0f + fi/fN)*cos(fi/1000.0f));
        float y = (float)((1.0f + fi/fN)*sin(fi/1000.0f));

        stuff[i].uid = i+1;
        stuff[i].size = 10;
        stuff[i].x = 1000.0f * x;
        stuff[i].y = 1000.0f * y;
        stuff[i].value1 = static_cast<short>(i-20000);
        stuff[i].value2 = 0;
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename test::my_dataset::template
            secondary_q_iterator<
            test::value1_index
            >::elem_type index_iterator;

        test::value1_index point1(-5);
        test::value1_index point2(15);

        index_iterator it, begin, end;

        ASSERT_NO_THROW(boost::tie(begin, end) =
            dataset.secondary_query_elements(
            s1o::queries::make_closed_interval(point1, point2)));

        std::set<int> found_uids;

        for (it = begin; it != end; it++) {

            const test::my_metadata& meta = *it->first;
            ASSERT_TRUE(found_uids.insert(meta.uid).second);
        }

        ASSERT_TRUE(found_uids.size() > 0);

        for (int i = 0; i < N; i++) {

            const test::my_metadata& meta = stuff[i];

            short minval = point1.value;
            short maxval = point2.value;

            if (found_uids.find(meta.uid) != found_uids.end()) {

                ASSERT_TRUE(minval <= meta.value1);
                ASSERT_TRUE(maxval >= meta.value1);
            }
            else {

                ASSERT_FALSE(minval <= meta.value1 && \
                             maxval >= meta.value1);
            }
        }
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
DATASET_TEST(MultipleSlot, SecondQueryRangeElemComplexEmptyIndex1)

    const int N = 40000;
    const int slots = 3;

    test::meta_vector stuff;
    stuff.resize(N);

    for (int i = 0; i < N; i++) {

        float fi = static_cast<float>(i);
        float fN = static_cast<float>(N);
        float x = (float)((1.0f + fi/fN)*cos(fi/1000.0f));
        float y = (float)((1.0f + fi/fN)*sin(fi/1000.0f));

        stuff[i].uid = i+1;
        stuff[i].size = 10;
        stuff[i].x = x;
        stuff[i].y = y;
        stuff[i].value1 = static_cast<short>(i-20000);
        stuff[i].value2 = 0;
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename test::my_dataset::template
            secondary_q_iterator<
            test::value1_index
            >::elem_type index_iterator;

        test::value1_index point1(-30000);
        test::value1_index point2(-20001);

        index_iterator it, begin, end;

        ASSERT_NO_THROW(boost::tie(begin, end) =
            dataset.secondary_query_elements(
            s1o::queries::make_closed_interval(point1, point2)));

        ASSERT_TRUE(std::distance(begin, end) == 0);
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

///////////////////////////////////////////////////////////////////////////////

/**
 *
 */
DATASET_TEST(MultipleSlot, SecondQueryRangeElemSlotAllTightIndex1)

    const int X = 200;
    const int Y = 200;
    const int N = X*Y;
    const int slots = 3;

    test::meta_vector stuff;
    stuff.resize(N);

    {
        int i = 0;
        for (int iy = 0; iy < Y; iy++) {
            for (int ix = 0; ix < X; ix++, i++) {

                stuff[i].uid = i+1;
                stuff[i].size = 10;
                stuff[i].x = -100.0f * (ix + 1);
                stuff[i].y = 100.0f * (iy + 1);
                stuff[i].value1 = static_cast<short>(i-20000);
                stuff[i].value2 = 0;
            }
        }
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename test::my_dataset::template
            secondary_q_iterator<
            test::value1_index
            >::elem_slot_type index_iterator;

        test::value1_index point1(-20000);
        test::value1_index point2(N-20000-1);

        index_iterator it, begin, end;

        ASSERT_THROW(boost::tie(begin, end) =
            dataset.secondary_query_elements(
            s1o::queries::make_closed_interval(point1, point2), slots),
            s1o::invalid_slot_exception);

        ASSERT_NO_THROW(boost::tie(begin, end) =
            dataset.secondary_query_elements(
            s1o::queries::make_closed_interval(point1, point2), 2));

        std::set<int> found_uids;

        for (it = begin; it != end; it++) {

            const test::my_metadata& meta = *it->first;
            ASSERT_TRUE(found_uids.insert(meta.uid).second);
        }

        ASSERT_TRUE(found_uids.size() == N);
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
DATASET_TEST(MultipleSlot, SecondQueryRangeElemSlotOneTightIndex1)

    const int X = 200;
    const int Y = 200;
    const int N = X*Y;
    const int slots = 3;

    test::meta_vector stuff;
    stuff.resize(N);

    {
        int i = 0;
        for (int iy = 0; iy < Y; iy++) {
            for (int ix = 0; ix < X; ix++, i++) {

                stuff[i].uid = i+1;
                stuff[i].size = 10;
                stuff[i].x = -100.0f * (ix + 1);
                stuff[i].y = 100.0f * (iy + 1);
                stuff[i].value1 = static_cast<short>(i-20000);
                stuff[i].value2 = 0;
            }
        }
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename test::my_dataset::template
            secondary_q_iterator<
            test::value1_index
            >::elem_slot_type index_iterator;

        const size_t Who = 1234;

        test::value1_index point1(stuff[Who].value1);
        test::value1_index point2(stuff[Who].value1);

        index_iterator it, begin, end;

        ASSERT_THROW(boost::tie(begin, end) =
            dataset.secondary_query_elements(
            s1o::queries::make_closed_interval(point1, point2), slots),
            s1o::invalid_slot_exception);

        ASSERT_NO_THROW(boost::tie(begin, end) =
            dataset.secondary_query_elements(
            s1o::queries::make_closed_interval(point1, point2), 2));

        std::set<int> found_uids;

        for (it = begin; it != end; it++) {

            const test::my_metadata& meta = *it->first;
            ASSERT_TRUE(found_uids.insert(meta.uid).second);
        }

        ASSERT_TRUE(found_uids.size() == 1);
        ASSERT_TRUE(*found_uids.begin() == (Who + 1));
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
DATASET_TEST(MultipleSlot, SecondQueryRangeElemSlotComplexIndex1)

    const int N = 40000;
    const int slots = 3;

    test::meta_vector stuff;
    stuff.resize(N);

    for (int i = 0; i < N; i++) {

        float fi = static_cast<float>(i);
        float fN = static_cast<float>(N);
        float x = (float)((1.0f + fi/fN)*cos(fi/1000.0f));
        float y = (float)((1.0f + fi/fN)*sin(fi/1000.0f));

        stuff[i].uid = i+1;
        stuff[i].size = 10;
        stuff[i].x = 1000.0f * x;
        stuff[i].y = 1000.0f * y;
        stuff[i].value1 = static_cast<short>(i-20000);
        stuff[i].value2 = 0;
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename test::my_dataset::template
            secondary_q_iterator<
            test::value1_index
            >::elem_slot_type index_iterator;

        test::value1_index point1(-5);
        test::value1_index point2(15);

        index_iterator it, begin, end;

        ASSERT_THROW(boost::tie(begin, end) =
            dataset.secondary_query_elements(
            s1o::queries::make_closed_interval(point1, point2), slots),
            s1o::invalid_slot_exception);

        ASSERT_NO_THROW(boost::tie(begin, end) =
            dataset.secondary_query_elements(
            s1o::queries::make_closed_interval(point1, point2), 2));

        std::set<int> found_uids;

        for (it = begin; it != end; it++) {

            const test::my_metadata& meta = *it->first;
            ASSERT_TRUE(found_uids.insert(meta.uid).second);
        }

        ASSERT_TRUE(found_uids.size() > 0);

        for (int i = 0; i < N; i++) {

            const test::my_metadata& meta = stuff[i];

            short minval = point1.value;
            short maxval = point2.value;

            if (found_uids.find(meta.uid) != found_uids.end()) {

                ASSERT_TRUE(minval <= meta.value1);
                ASSERT_TRUE(maxval >= meta.value1);
            }
            else {

                ASSERT_FALSE(minval <= meta.value1 && \
                             maxval >= meta.value1);
            }
        }
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
DATASET_TEST(MultipleSlot, SecondQueryRangeElemSlotComplexEmptyIndex1)

    const int N = 40000;
    const int slots = 3;

    test::meta_vector stuff;
    stuff.resize(N);

    for (int i = 0; i < N; i++) {

        float fi = static_cast<float>(i);
        float fN = static_cast<float>(N);
        float x = (float)((1.0f + fi/fN)*cos(fi/1000.0f));
        float y = (float)((1.0f + fi/fN)*sin(fi/1000.0f));

        stuff[i].uid = i+1;
        stuff[i].size = 10;
        stuff[i].x = x;
        stuff[i].y = y;
        stuff[i].value1 = static_cast<short>(i-20000);
        stuff[i].value2 = 0;
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename test::my_dataset::template
            secondary_q_iterator<
            test::value1_index
            >::elem_slot_type index_iterator;

        test::value1_index point1(-30000);
        test::value1_index point2(-20001);

        index_iterator it, begin, end;

        ASSERT_THROW(boost::tie(begin, end) =
            dataset.secondary_query_elements(
            s1o::queries::make_closed_interval(point1, point2), slots),
            s1o::invalid_slot_exception);

        ASSERT_NO_THROW(boost::tie(begin, end) =
            dataset.secondary_query_elements(
            s1o::queries::make_closed_interval(point1, point2), 2));

        ASSERT_TRUE(std::distance(begin, end) == 0);
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
