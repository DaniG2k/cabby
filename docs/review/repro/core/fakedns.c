#define _GNU_SOURCE
#include <dlfcn.h>
#include <netdb.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
int getaddrinfo(const char *node, const char *service, const struct addrinfo *hints, struct addrinfo **res) {
    static int (*real)(const char *, const char *, const struct addrinfo *, struct addrinfo **);
    if (!real) real = dlsym(RTLD_NEXT, "getaddrinfo");
    if (node && strcmp(node, "rpc.test") == 0) {
        const char *path = getenv("FAKE_DNS_FILE");
        char ip[64] = "127.0.0.2";
        FILE *f = path ? fopen(path, "r") : NULL;
        if (f) { if (fscanf(f, "%63s", ip) != 1) strcpy(ip, "127.0.0.2"); fclose(f); }
        const char *log = getenv("FAKE_DNS_LOG");
        if (log) { FILE *l = fopen(log, "a"); if (l) { fprintf(l, "resolve rpc.test -> %s\n", ip); fclose(l); } }
        return real(ip, service, hints, res);
    }
    return real(node, service, hints, res);
}
