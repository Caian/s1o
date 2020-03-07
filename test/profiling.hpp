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

#include <ostream>

#include <unistd.h>
#include <malloc.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/resource.h>

namespace profiling {

/**
 * @brief Profiling event with start and end times.
 *
 */
class event
{
private:

    struct timeval tpi; /** Starting point time. */
    struct timeval tpf; /** Ending point time. */

protected:

    inline double tv_to_secs(const struct timeval tv) const
    {
        return static_cast<double>(tv.tv_sec) +
            static_cast<double>(tv.tv_usec) / 1000000.0;
    }

public:

    /**
     * @brief Construct a new event object
     *
     */
    inline event()
    {
        timerclear(&tpi);
        timerclear(&tpf);
    }

    /**
     * Constructor that initializes the
     * internal time references
     *
     * @param tpi The starting point reference
     * @param tpf The ending point reference
     */
    inline event(const struct timeval& tpi, const struct timeval& tpf) :
        tpi(tpi),
        tpf(tpf)
    {
    }

    /**
     * Set the starting point of the event
     */
    inline void set_start()
    {
        gettimeofday(&tpi, 0);
    }

    /**
     * Set the ending point of the event
     */
    inline void set_end()
    {
        gettimeofday(&tpf, 0);
    }

    /**
     * Get the starting point of the event, in seconds
     *
     * @return The starting point of the event in seconds with respect to
     * some arbitrary point in time. This point is the same for the
     * entire application
     */
    inline double get_start() const
    {
        return tv_to_secs(tpi);
    }

    /**
     * Get the ending point of the event, in seconds
     *
     * @return The ending point of the event in seconds with respect to
     * some arbitrary point in time. This point is the same for the
     * entire application
     */
    inline double get_end() const
    {
        return tv_to_secs(tpf);
    }

    /**
     * Get the starting point of the event, as a timeval struct
     *
     * @return The starting point of the event as a timeval struct
     */
    inline const timeval& get_tv_start() const
    {
        return tpi;
    }

    /**
     * Get the ending point of the event, as a timeval struct
     *
     * @return The ending point of the event as a timeval struct
     */
    inline const timeval& get_tv_end() const
    {
        return tpf;
    }

    /**
     * Return the number of seconds in the previous set_start/set_end interval.
     * It requires that both set_start and set_end have been previously called.
     *
     * @return The number of seconds in the previous set_start/set_end interval.
     */
    inline double secs_elapsed() const
    {
        struct timeval diff;
        tv_elapsed(diff);
        return tv_to_secs(diff);
    }

    /**
     * Get the difference, as a timeval struct, between the interval defined
     * by set_start/set_end interval. It requires that both set_start and set_end
     * have been previously called.
     *
     */
    inline void tv_elapsed(timeval& tv) const
    {
        timersub(&tpf, &tpi, &tv);
    }
};

/**
 * Profiling timer with accumulator
 */
class timer : public event
{
private:

    struct timeval acc; /** Accumulator */

public:

    /**
     * Constructor
     */
    inline timer() :
        event()
    {
        timerclear(&acc);
    }

    /**
     * Clear the accumulator
     */
    inline void clear_acc()
    {
        timerclear(&acc);
    }

    /**
     * Add the last SetStart/SetEnd interval to the accumulator. It requires
     * that both SetStart and SetEnd have been previously called
     */
    inline void accumulate()
    {
        const struct timeval& tpi = get_tv_start();
        const struct timeval& tpf = get_tv_end();
        struct timeval diff, temp;
        timersub(&tpf, &tpi, &diff);
        timeradd(&diff, &acc, &temp);
        acc = temp;
    }

    /**
     * Set an ending point and accumulate the elapsed time to the
     * accumulator. It requires a previous SetStart call
     */
    inline void lap()
    {
        set_end();
        accumulate();
    }

    /**
     * Return the number of seconds in the accumulator
     *
     * @return The number of seconds in the accumulator
     */
    inline double acc_secs_elapsed() const
    {
        return tv_to_secs(acc);
    }
};

/**
 * RAII-based timer trigger.
 */
class trigger
{
private:

    timer& t; /** Reference to the target timer */
    bool lap; /** Trigger a timer lap on destruction */

public:

    /**
     * Constructor.
     *
     * @param t Reference to the target timer
     * @param autoStart Automatically set a starting point for the timer
     * @param lapOnEnd trigger a timer lap on destruction
     */
    inline trigger(timer& t, bool autoStart = true, bool lapOnEnd = true) :
        t(t),
        lap(lapOnEnd)
    {
        if (autoStart) {
            t.set_start();
        }
    }

    /**
     * Destructor
     */
    inline ~trigger()
    {
        if (lap) {
            t.lap();
        }
        else {
            t.set_end();
        }
    }
};

class resources
{
private:

    rusage usage;

public:

    resources() :
        usage()
    {
        update();
    }

    void update()
    {
        getrusage(RUSAGE_SELF, &usage);
    }

    const rusage& get_usage() const
    {
        return usage;
    }
};

class mallinfo_printer
{
private:

    struct PRINT_INFO
    {
        std::ostream& stream;
        unsigned int delayms;

        PRINT_INFO(
            std::ostream& stream,
            unsigned int delayms
        ) :
            stream(stream),
            delayms(delayms)
        {
        }
    };

    static void* printer(void* data)
    {
        const PRINT_INFO* info = reinterpret_cast<const PRINT_INFO*>(data);

        const double MiB = 1024.0 * 1024.0;

        struct mallinfo mi;

        while (true) {

            mi = mallinfo();

            info->stream
                << "!! malloc arena / hblkhd / uordblks (MiB): "
                << (static_cast<double>(mi.arena) / MiB) << " "
                << (static_cast<double>(mi.hblkhd) / MiB) << " "
                << (static_cast<double>(mi.uordblks) / MiB)
                << std::endl;

            usleep(1000 * info->delayms);
        }

        delete info;
        return NULL;
    }

public:

    mallinfo_printer(
        std::ostream& stream,
        unsigned int delayms
    )
    {
        pthread_t thread;

        PRINT_INFO* info = new PRINT_INFO(stream, delayms);

        if(pthread_create(&thread, NULL, mallinfo_printer::printer, info)) {

            stream
                << "!! Failed to create thread!"
                << std::endl;
        }

    }
};

}
