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

#include "mi_key_extractor.hpp"

#include <boost/multi_index/ordered_index.hpp>
#include <boost/mpl/push_front.hpp>
#include <boost/mpl/pop_front.hpp>
#include <boost/mpl/vector.hpp>
#include <boost/mpl/front.hpp>
#include <boost/mpl/empty.hpp>
#include <boost/fusion/container/list/cons.hpp>
#include <boost/fusion/include/cons.hpp>

namespace s1o {
namespace helpers {

namespace detail {

template <typename T, unsigned int I, bool empty>
struct mi_vector_to_indices_impl
{
    typedef typename boost::mpl::pop_front<
        T
        >::type T_next;

    typedef mi_vector_to_indices_impl<
        T_next,
        I+1,
        boost::mpl::empty<T_next>::value
        > next;

    typedef typename boost::mpl::front<
        T
        >::type key_type;

    typedef typename boost::mpl::push_front<
        typename next::type,
        boost::multi_index::ordered_non_unique<
            boost::multi_index::tag<key_type>,
            mi_key_extractor<key_type, I>
            >
        >::type type;
};

template <typename T, unsigned int I>
struct mi_vector_to_indices_impl<T, I, true>
{
    typedef boost::mpl::vector<> type;
};

}

/**
 * @brief Helper struct to convert a boost::mpl vector of indices into another
 * vector, but with index specification required the a multiindex container.
 *
 * @tparam T The type of the boost::mpl vector containing the indices.
 *
 * @note Indices are always 'ordered_non_unique'.
 *
 * @note The type of each index must also be usable as value.
 */
template <typename T>
struct mi_vector_to_indices
{
    typedef typename detail::mi_vector_to_indices_impl<
        T,
        0,
        boost::mpl::empty<T>::value
        >::type type;
};

}}
