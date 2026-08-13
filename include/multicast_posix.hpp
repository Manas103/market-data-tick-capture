// POSIX UDP multicast sender/receiver.
//
// Real IGMP group membership via setsockopt(IP_ADD_MEMBERSHIP), not unicast
// wearing a multicast address. The demos in src/ genuinely publish to a
// 239.x.x.x group and genuinely join it on the receive side.
//
// Interface selection is explicit on both ends. That is not boilerplate:
// a multicast socket that does not pin IP_MULTICAST_IF sends out whichever
// interface the routing table prefers, which in a WSL2 VM is the NAT'd eth0,
// and the datagrams then leave the machine instead of looping back. Pinning
// both the sender's egress interface and the receiver's membership
// interface to the loopback address is what makes a single-host demo work
// deterministically -- see the "Engineering findings" section of the README
// for what this looked like before it was pinned.
#pragma once
#include <cstdint>
#include <cstring>
#include <string>
#include <stdexcept>
#include <vector>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>

namespace tickcap {

constexpr const char* kDefaultGroup = "239.7.13.21";
constexpr uint16_t    kDefaultPort  = 34712;
constexpr const char* kLoopbackIf   = "127.0.0.1";

class MulticastSender {
public:
    MulticastSender(const std::string& group, uint16_t port,
                    const std::string& iface = kLoopbackIf) {
        fd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
        if (fd_ < 0) throw std::runtime_error("MulticastSender: socket() failed");

        in_addr mcif{};
        mcif.s_addr = ::inet_addr(iface.c_str());
        if (::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_IF, &mcif, sizeof(mcif)) < 0)
            throw std::runtime_error("MulticastSender: IP_MULTICAST_IF failed");

        unsigned char loop = 1;   // deliver to listeners on this host
        ::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop));
        unsigned char ttl = 1;    // stay on the local segment
        ::setsockopt(fd_, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));

        std::memset(&dst_, 0, sizeof(dst_));
        dst_.sin_family = AF_INET;
        dst_.sin_addr.s_addr = ::inet_addr(group.c_str());
        dst_.sin_port = htons(port);
    }

    ~MulticastSender() { if (fd_ >= 0) ::close(fd_); }

    ssize_t send(const void* data, size_t len) {
        return ::sendto(fd_, data, len, 0,
                        reinterpret_cast<sockaddr*>(&dst_), sizeof(dst_));
    }

    void set_send_buffer(int bytes) {
        ::setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &bytes, sizeof(bytes));
    }

private:
    int fd_ = -1;
    sockaddr_in dst_{};
};

class MulticastReceiver {
public:
    MulticastReceiver(const std::string& group, uint16_t port,
                      const std::string& iface = kLoopbackIf) {
        fd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
        if (fd_ < 0) throw std::runtime_error("MulticastReceiver: socket() failed");

        int on = 1;
        ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

        sockaddr_in bindaddr{};
        std::memset(&bindaddr, 0, sizeof(bindaddr));
        bindaddr.sin_family = AF_INET;
        bindaddr.sin_addr.s_addr = htonl(INADDR_ANY);
        bindaddr.sin_port = htons(port);
        if (::bind(fd_, reinterpret_cast<sockaddr*>(&bindaddr), sizeof(bindaddr)) < 0)
            throw std::runtime_error("MulticastReceiver: bind() failed");

        ip_mreq mreq{};
        std::memset(&mreq, 0, sizeof(mreq));
        mreq.imr_multiaddr.s_addr = ::inet_addr(group.c_str());
        mreq.imr_interface.s_addr = ::inet_addr(iface.c_str());
        if (::setsockopt(fd_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0) {
            // Fall back to letting the kernel choose the membership
            // interface; reported by the caller rather than hidden.
            mreq.imr_interface.s_addr = htonl(INADDR_ANY);
            if (::setsockopt(fd_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0)
                throw std::runtime_error("MulticastReceiver: IP_ADD_MEMBERSHIP failed");
            joined_on_any_ = true;
        }
    }

    ~MulticastReceiver() { if (fd_ >= 0) ::close(fd_); }

    // Requests a receive buffer and reports what the kernel actually gave.
    //
    // This deliberately returns the granted size instead of swallowing it.
    // setsockopt() does NOT fail when the request exceeds
    // net.core.rmem_max -- it silently clamps, so a program can ask for
    // 32 MB, be given 208 KB, get no error, and then lose datagrams for
    // reasons that look like nothing to do with buffering. Reporting the
    // real number turns that into something visible.
    //
    // Note the kernel doubles the requested value internally to account for
    // bookkeeping overhead, so getsockopt returns roughly 2x what was asked
    // for when the request is under the cap.
    int set_recv_buffer(int bytes) {
        ::setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &bytes, sizeof(bytes));
        int actual = 0;
        socklen_t len = sizeof(actual);
        ::getsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &actual, &len);
        return actual;
    }

    void set_timeout_ms(int ms) {
        timeval tv{};
        tv.tv_sec  = ms / 1000;
        tv.tv_usec = (ms % 1000) * 1000;
        ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }

    // Returns bytes received, or -1 on timeout/error (errno distinguishes).
    ssize_t recv(void* buf, size_t len) {
        return ::recv(fd_, buf, len, 0);
    }

    bool joined_on_any() const { return joined_on_any_; }

private:
    int fd_ = -1;
    bool joined_on_any_ = false;
};

} // namespace tickcap
