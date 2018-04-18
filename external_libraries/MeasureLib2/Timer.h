/*
 * Timer.h
 *
 *  Created on: Aug 17, 2012
 *      Author: hadianeh
 *  Modified by: Emmanuel Amaro
 */

#ifndef TIMER_H_
#define TIMER_H_

#include <chrono>

class Timer {
public:
    Timer() { }

    inline void start() {
      _start = std::chrono::high_resolution_clock::now();
    }

    // return the duration
    inline std::chrono::duration<double> end() {
      _end = std::chrono::high_resolution_clock::now();
      return _end - _start;
    }

    inline long long int toMicrosecs(std::chrono::duration<double> &duration) {
      return std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
    }

    inline long long int toSecs(std::chrono::duration<double> &duration) {
      return std::chrono::duration_cast<std::chrono::seconds>(duration).count();
    }

    inline long long int toMillisec(std::chrono::duration<double> &duration) {
      return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    }

		inline long long int toNanosec(std::chrono::duration<double> &duration) {
      return std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
    }
private:
    std::chrono::time_point<std::chrono::high_resolution_clock> _start;
    std::chrono::time_point<std::chrono::high_resolution_clock> _end;
};

#endif /* TIMER_H_ */
