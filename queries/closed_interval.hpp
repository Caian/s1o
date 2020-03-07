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

#pragma once

#include <s1o/traits/first_point_argument.hpp>
#include <s1o/traits/spatial_point_type.hpp>

namespace s1o {
namespace queries {

/**
 * @brief The query predicate for a spatial interval search.
 *
 * @tparam Point The type of the spatial point.
 */
template <typename Point>
struct closed_interval
{
    typedef closed_interval<Point> this_type;
    typedef Point spatial_point_type;

    Point point_min;
    Point point_max;

    /**
     * @brief Construct a new closed_interval object.
     *
     * @param point_min The corner of the hypercube with the smallest
     * coordinate values.
     * @param point_max The corner of the hypercube with the largest
     * coordinate values.
     */
    closed_interval(
        const Point& point_min,
        const Point& point_max
    ) :
        point_min(point_min),
        point_max(point_max)
    {
    }

    /**
     * @brief Construct a new closed_interval object.
     *
     * @param other Another closed_interval object to copy.
     */
    closed_interval(
        const this_type& other
    ) :
        point_min(other.point_min),
        point_max(other.point_max)
    {
    }
};

/**
 * @brief Create a closed interval predicate.
 *
 * @tparam T The type of the spatial point.
 *
 * @param point_min The corner of the hypercube with the smallest
 * coordinate values.
 * @param point_max The corner of the hypercube with the largest
 * coordinate values.
 *
 * @return closed_interval<T> The closed interval predicate.
 */
template <typename T>
closed_interval<T> make_closed_interval(
    const T& point_min,
    const T& point_max
)
{
    return closed_interval<T>(point_min, point_max);
}

}

namespace traits {

template <typename Point>
struct first_point_argument<queries::closed_interval<Point> >
{
    typedef Point type;

    static const type& call(const queries::closed_interval<Point>& t)
    {
        return t.minpoint;
    }
};

template <typename Point>
struct spatial_point_type<queries::closed_interval<Point> >
{
    typedef Point type;
};

}

}
