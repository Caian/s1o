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

#pragma once

#include <s1o/traits/first_point_argument.hpp>
#include <s1o/traits/spatial_point_type.hpp>

namespace s1o {
namespace queries {

/**
 * @brief The query predicate for a k-nearest neighbor search.
 *
 * @tparam Point The type of the spatial point.
 */
template <typename Point>
struct nearest
{
    typedef Point spatial_point_type;

    Point point;
    unsigned int k;

    /**
     * @brief Construct a new nearest object.
     *
     * @param point The reference location when searching for neighbors.
     * @param k The number of nearest neighbors to search.
     */
    nearest(
        const Point& point,
        unsigned int k
    ) :
        point(point),
        k(k)
    {
    }
};

/**
 * @brief Create a nearest predicate.
 *
 * @tparam T The type of the spatial point.
 *
 * @param point The reference location when searching for neighbors.
 * @param k The number of nearest neighbors to search.
 *
 * @return nearest<T> The nearest predicate.
 */
template <typename T>
nearest<T> make_nearest(
    const T& point,
    unsigned int k
)
{
    return nearest<T>(point, k);
}

}

namespace traits {

template <typename Point>
struct first_point_argument<queries::nearest<Point> >
{
    typedef Point type;

    static const type& call(const queries::nearest<Point>& t)
    {
        return t.point;
    }
};

template <typename Point>
struct spatial_point_type<queries::nearest<Point> >
{
    typedef Point type;
};

}

}
