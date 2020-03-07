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
#include <iterator>
#include <vector>
#include <cmath>
#include <set>

#include <stdlib.h>

namespace {

/**
 *
 */
DATASET_TEST(MultipleSlot, QueryRangeMetaAllTight)

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
            }
        }
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename s1o::traits::spatial_point_type<
            test::my_dataset
            >::type point;

        point point1(-100.0f * (X+1), 100.0f);
        point point2(-100.0f, 100.0f * (Y+1));

        test::my_meta_qiter it, begin, end;

        ASSERT_NO_THROW(boost::tie(begin, end) = dataset.query_metadata(
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
DATASET_TEST(MultipleSlot, QueryRangeMetaOneTight)

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
            }
        }
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename s1o::traits::spatial_point_type<
            test::my_dataset
            >::type point;

        const size_t Who = 1234;

        point point1(stuff[Who].x, stuff[Who].y);
        point point2(stuff[Who].x, stuff[Who].y);

        test::my_meta_qiter it, begin, end;

        ASSERT_NO_THROW(boost::tie(begin, end) = dataset.query_metadata(
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
DATASET_TEST(MultipleSlot, QueryRangeMetaComplex)

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
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename s1o::traits::spatial_point_type<
            test::my_dataset
            >::type point;

        point point1(1.32e3f, -2.20e3f);
        point point2(1.85e3f, -0.92e3f);

        test::my_meta_qiter it, begin, end;

        ASSERT_NO_THROW(boost::tie(begin, end) = dataset.query_metadata(
            s1o::queries::make_closed_interval(point1, point2)));

        std::set<int> found_uids;

        for (it = begin; it != end; it++) {

            const test::my_metadata& meta = *it;
            ASSERT_TRUE(found_uids.insert(meta.uid).second);
        }

        ASSERT_TRUE(found_uids.size() > 0);

        for (int i = 0; i < N; i++) {

            const test::my_metadata& meta = stuff[i];

            float minx = point1.template get<0>();
            float maxx = point2.template get<0>();
            float miny = point1.template get<1>();
            float maxy = point2.template get<1>();

            if (found_uids.find(meta.uid) != found_uids.end()) {

                ASSERT_TRUE(minx <= meta.x);
                ASSERT_TRUE(maxx >= meta.x);
                ASSERT_TRUE(miny <= meta.y);
                ASSERT_TRUE(maxy >= meta.y);
            }
            else {

                ASSERT_FALSE(minx <= meta.x && \
                             maxx >= meta.x && \
                             miny <= meta.y && \
                             maxy >= meta.y);
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
DATASET_TEST(MultipleSlot, QueryRangeMetaComplexEmpty)

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
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename s1o::traits::spatial_point_type<
            test::my_dataset
            >::type point;

        point point1(-0.15f, -0.29f);
        point point2(0.66f, 0.37f);

        test::my_meta_qiter it, begin, end;

        ASSERT_NO_THROW(boost::tie(begin, end) = dataset.query_metadata(
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
DATASET_TEST(MultipleSlot, QueryRangeElemAllTight)

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
            }
        }
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename s1o::traits::spatial_point_type<
            test::my_dataset
            >::type point;

        point point1(-100.0f * (X+1), 100.0f);
        point point2(-100.0f, 100.0f * (Y+1));

        test::my_elem_qiter it, begin, end;

        ASSERT_NO_THROW(boost::tie(begin, end) = dataset.query_elements(
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
DATASET_TEST(MultipleSlot, QueryRangeElemOneTight)

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
            }
        }
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename s1o::traits::spatial_point_type<
            test::my_dataset
            >::type point;

        const size_t Who = 1234;

        point point1(stuff[Who].x, stuff[Who].y);
        point point2(stuff[Who].x, stuff[Who].y);

        test::my_elem_qiter it, begin, end;

        ASSERT_NO_THROW(boost::tie(begin, end) = dataset.query_elements(
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
DATASET_TEST(MultipleSlot, QueryRangeElemComplex)

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
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename s1o::traits::spatial_point_type<
            test::my_dataset
            >::type point;

        point point1(1.32e3f, -2.20e3f);
        point point2(1.85e3f, -0.92e3f);

        test::my_elem_qiter it, begin, end;

        ASSERT_NO_THROW(boost::tie(begin, end) = dataset.query_elements(
            s1o::queries::make_closed_interval(point1, point2)));

        std::set<int> found_uids;

        for (it = begin; it != end; it++) {

            const test::my_metadata& meta = *it->first;
            ASSERT_TRUE(found_uids.insert(meta.uid).second);
        }

        ASSERT_TRUE(found_uids.size() > 0);

        for (int i = 0; i < N; i++) {

            const test::my_metadata& meta = stuff[i];

            float minx = point1.template get<0>();
            float maxx = point2.template get<0>();
            float miny = point1.template get<1>();
            float maxy = point2.template get<1>();

            if (found_uids.find(meta.uid) != found_uids.end()) {

                ASSERT_TRUE(minx <= meta.x);
                ASSERT_TRUE(maxx >= meta.x);
                ASSERT_TRUE(miny <= meta.y);
                ASSERT_TRUE(maxy >= meta.y);
            }
            else {

                ASSERT_FALSE(minx <= meta.x && \
                             maxx >= meta.x && \
                             miny <= meta.y && \
                             maxy >= meta.y);
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
DATASET_TEST(MultipleSlot, QueryRangeElemComplexEmpty)

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
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename s1o::traits::spatial_point_type<
            test::my_dataset
            >::type point;

        point point1(-0.15f, -0.29f);
        point point2(0.66f, 0.37f);

        test::my_meta_qiter it, begin, end;

        ASSERT_NO_THROW(boost::tie(begin, end) = dataset.query_metadata(
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
DATASET_TEST(MultipleSlot, QueryRangeElemSlotAllTight)

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
            }
        }
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename s1o::traits::spatial_point_type<
            test::my_dataset
            >::type point;

        point point1(-100.0f * (X+1), 100.0f);
        point point2(-100.0f, 100.0f * (Y+1));

        test::my_elem_qiter_s it, begin, end;

        ASSERT_THROW(boost::tie(begin, end) = dataset.query_elements(
            s1o::queries::make_closed_interval(point1, point2), slots),
            s1o::invalid_slot_exception);

        ASSERT_NO_THROW(boost::tie(begin, end) = dataset.query_elements(
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
DATASET_TEST(MultipleSlot, QueryRangeElemSlotOneTight)

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
            }
        }
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename s1o::traits::spatial_point_type<
            test::my_dataset
            >::type point;

        const size_t Who = 1234;

        point point1(stuff[Who].x, stuff[Who].y);
        point point2(stuff[Who].x, stuff[Who].y);

        test::my_elem_qiter_s it, begin, end;

        ASSERT_THROW(boost::tie(begin, end) = dataset.query_elements(
            s1o::queries::make_closed_interval(point1, point2), slots),
            s1o::invalid_slot_exception);

        ASSERT_NO_THROW(boost::tie(begin, end) = dataset.query_elements(
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
DATASET_TEST(MultipleSlot, QueryRangeElemSlotComplex)

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
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename s1o::traits::spatial_point_type<
            test::my_dataset
            >::type point;

        point point1(1.32e3f, -2.20e3f);
        point point2(1.85e3f, -0.92e3f);

        test::my_elem_qiter_s it, begin, end;

        ASSERT_THROW(boost::tie(begin, end) = dataset.query_elements(
            s1o::queries::make_closed_interval(point1, point2), slots),
            s1o::invalid_slot_exception);

        ASSERT_NO_THROW(boost::tie(begin, end) = dataset.query_elements(
            s1o::queries::make_closed_interval(point1, point2), 2));

        std::set<int> found_uids;

        for (it = begin; it != end; it++) {

            const test::my_metadata& meta = *it->first;
            ASSERT_TRUE(found_uids.insert(meta.uid).second);
        }

        ASSERT_TRUE(found_uids.size() > 0);

        for (int i = 0; i < N; i++) {

            const test::my_metadata& meta = stuff[i];

            float minx = point1.template get<0>();
            float maxx = point2.template get<0>();
            float miny = point1.template get<1>();
            float maxy = point2.template get<1>();

            if (found_uids.find(meta.uid) != found_uids.end()) {

                ASSERT_TRUE(minx <= meta.x);
                ASSERT_TRUE(maxx >= meta.x);
                ASSERT_TRUE(miny <= meta.y);
                ASSERT_TRUE(maxy >= meta.y);
            }
            else {

                ASSERT_FALSE(minx <= meta.x && \
                             maxx >= meta.x && \
                             miny <= meta.y && \
                             maxy >= meta.y);
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
DATASET_TEST(MultipleSlot, QueryRangeElemSlotComplexEmpty)

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
    }

    try {
        test::my_dataset dataset(dataset_name, 0,
            slots, stuff.begin(), stuff.end());

        typedef typename s1o::traits::spatial_point_type<
            test::my_dataset
            >::type point;

        point point1(-0.15f, -0.29f);
        point point2(0.66f, 0.37f);

        test::my_elem_qiter_s it, begin, end;

        ASSERT_THROW(boost::tie(begin, end) = dataset.query_elements(
            s1o::queries::make_closed_interval(point1, point2), slots),
            s1o::invalid_slot_exception);

        ASSERT_NO_THROW(boost::tie(begin, end) = dataset.query_elements(
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
