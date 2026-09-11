// Minimal POSIX TCP helpers shared by the two sequenced-session protocols
// this extension adds: the gap-recovery retransmit path (gap_recovery.hpp)
// and the OUCH-style order-entry session (ouch_session.hpp).
//
// Both of those are, structurally, the same thing a real exchange gateway
// has: a length-prefixed message stream over a reliable byte pipe, used
// precisely because UDP multicast cannot retransmit anything -- once a
// datagram is gone, the only way back is a connection that can. This file
// is deliberately small: accept/connect/bind-to-ephemeral-port and
// exact-length read/write, nothing else. Framing and message semantics live
// in the two headers that use this.
#pragma once
#include <cstdint>
#include <cstring>
#include <string>
#include <stdexcept>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <csignal>

namespace tickcap {

constexpr const char* kLoopbackAddr = "127.0.0.1";

// A write() to a socket whose peer already closed its read side raises
// SIGPIPE, and SIGPIPE's default disposition kills the entire process, not
// just the writing thread -- found the hard way, by a real repro: the
// OUCH-session resend-on-reconnect test disconnects a client mid-stream
// (exactly the scenario resend-on-reconnect exists to recover from) while a
// server-side handler thread is still writing its queued replies, and the
// whole process, all threads, died with "killed by SIGPIPE" (see
// docs/ouch_session_sigpipe_repro.txt, captured with strace before this
// fix). write_all() already checks its return value and treats a failed
// write as a clean session-drop (see tcp_io.hpp above), which is exactly
// right, provided the process survives long enough to see that return
// value. Ignoring SIGPIPE process-wide, once, is the standard fix: it turns
// the write into an ordinary EPIPE-returning failure instead of a signal.
// Every TCP entry point below calls this before doing anything else, so any
// executable that uses tcp_io.hpp gets the fix without having to remember
// it itself.
inline void ignore_sigpipe_once() {
    static bool done = [] {
        std::signal(SIGPIPE, SIG_IGN);
        return true;
    }();
    (void)done;
}

// Binds to loopback on the given port (0 = kernel picks a free ephemeral
// port -- the process-hygiene rule this project runs under is "a busy port
// is not yours to clear, pick a free one", and port 0 is how a POSIX socket
// asks the kernel to do exactly that) and starts listening. Returns the fd;
// call tcp_bound_port() to find out which port was actually bound.
inline int tcp_listen(uint16_t port, int backlog = 16) {
    ignore_sigpipe_once();
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error("tcp_listen: socket() failed");
    int on = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = ::inet_addr(kLoopbackAddr);
    addr.sin_port = htons(port);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        throw std::runtime_error("tcp_listen: bind() failed");
    }
    if (::listen(fd, backlog) < 0) {
        ::close(fd);
        throw std::runtime_error("tcp_listen: listen() failed");
    }
    return fd;
}

inline uint16_t tcp_bound_port(int fd) {
    sockaddr_in addr{};
    socklen_t len = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) < 0)
        throw std::runtime_error("tcp_bound_port: getsockname() failed");
    return ntohs(addr.sin_port);
}

inline int tcp_accept(int listen_fd) {
    int fd = ::accept(listen_fd, nullptr, nullptr);
    if (fd >= 0) {
        int on = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
    }
    return fd;
}

inline int tcp_connect(const std::string& host, uint16_t port) {
    ignore_sigpipe_once();
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error("tcp_connect: socket() failed");
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = ::inet_addr(host.c_str());
    addr.sin_port = htons(port);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        throw std::runtime_error("tcp_connect: connect() failed");
    }
    int on = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
    return fd;
}

inline void tcp_set_recv_timeout_ms(int fd, int ms) {
    timeval tv{};
    tv.tv_sec = ms / 1000;
    tv.tv_usec = (ms % 1000) * 1000;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}

// Writes exactly n bytes or returns false. A short write on a blocking TCP
// socket is rare but legal (interrupted by a signal, or the kernel just
// decided to hand back less than requested), so this loops rather than
// assuming one write() covers the buffer -- an assumption that would work
// on every test run right up until it silently didn't.
inline bool write_all(int fd, const void* buf, size_t n) {
    const uint8_t* p = static_cast<const uint8_t*>(buf);
    size_t done = 0;
    while (done < n) {
        ssize_t k = ::write(fd, p + done, n - done);
        if (k < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (k == 0) return false;
        done += static_cast<size_t>(k);
    }
    return true;
}

// Reads exactly n bytes or returns false (EOF or error before n bytes).
inline bool read_all(int fd, void* buf, size_t n) {
    uint8_t* p = static_cast<uint8_t*>(buf);
    size_t done = 0;
    while (done < n) {
        ssize_t k = ::read(fd, p + done, n - done);
        if (k < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (k == 0) return false;
        done += static_cast<size_t>(k);
    }
    return true;
}

} // namespace tickcap
