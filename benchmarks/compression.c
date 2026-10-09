/* Independent-block comparison. Synthetic inputs, not a real-time latency test.
 * See compression.md for the build command and interpretation. */
#define _GNU_SOURCE
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>
#include <zdict.h>
#include <lz4.h>
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sched.h>

enum { BLOCKS = 64, TRIALS = 7 };
static uint64_t random_state = 1;
static uint32_t random_word(void) {
    random_state ^= random_state << 13;
    random_state ^= random_state >> 7;
    random_state ^= random_state << 17;
    return (uint32_t)random_state;
}
static uint64_t now_ns(void) {
    struct timespec t;
    assert(clock_gettime(CLOCK_MONOTONIC, &t) == 0);
    return (uint64_t)t.tv_sec * 1000000000 + t.tv_nsec;
}
static void put32(void *p, uint32_t n) { memcpy(p, &n, 4); }
static void put64(void *p, uint64_t n) { memcpy(p, &n, 8); }

/* Kernel-like page and event headers, changing timestamps/task IDs, repeated
 * task names, and either scheduler payloads or a mix including random values.
 * The sparse case has the zero-filled tail that the recorder also compresses. */
static void generate(unsigned char *p, size_t size, int workload, unsigned page) {
    memset(p, 0, size);
    random_state = 1234567 + page;
    put64(p, 100000000000ULL + page * 1000000ULL);
    size_t end = workload == 2 ? size / 4 : size;
    size_t pos = 16;
    unsigned previous = 1;
    while (pos + 104 <= end) {
        unsigned next = 1 + random_word() % 256;
        int waking = random_word() % 3 == 0;
        size_t payload = waking ? 36 : 64;
        if (workload == 1) payload = 24 + 8 * (random_word() % 10);
        put32(p + pos, (random_word() % 100000 + 1) << 5 | (unsigned)(payload / 4));
        unsigned char *event = p + pos + 4;
        uint16_t id = workload == 1 ? 200 + random_word() % 8 : 100u + (unsigned)waking;
        memcpy(event, &id, 2);
        put32(event + 4, previous);
        if (workload == 1) {
            for (size_t i = 8; i < payload; i += 8) {
                uint64_t value = random_word();
                if (i % 24 == 0) value = 0xffff800000000000ULL | (value & 0xffffff) << 4;
                else if (i % 24 == 8) value %= 256;
                put64(event + i, value);
            }
        } else {
            snprintf((char *)event + 8, 16, "worker-%u", previous);
            put32(event + 24, previous);
            put32(event + 28, 120);
            if (waking) put32(event + 32, random_word() % 8);
            else {
                put64(event + 32, random_word() % 3);
                snprintf((char *)event + 40, 16, "worker-%u", next);
                put32(event + 56, next);
                put32(event + 60, 120);
            }
        }
        previous = next;
        pos += payload + 4;
    }
    put64(p + 8, pos - 16);
}

struct codec {
    const char *name;
    int level; /* 0 selects LZ4 acceleration 1. */
    void *workspace;
    size_t workspace_size;
    ZSTD_CCtx *zstd;
    ZSTD_CDict *dictionary;
};
static size_t compress_block(struct codec *c, const void *src, size_t size,
                             void *dst, size_t capacity) {
    if (c->level == 0) {
        int n = LZ4_compress_fast_extState(c->workspace, src, dst,
                                         (int)size, (int)capacity, 1);
        assert(n > 0);
        return (size_t)n;
    }
    size_t n = c->dictionary
        ? ZSTD_compress_usingCDict(c->zstd, dst, capacity, src, size, c->dictionary)
        : ZSTD_compressCCtx(c->zstd, dst, capacity, src, size, c->level);
    assert(!ZSTD_isError(n));
    return n;
}
static int compare(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}
int main(void) {
    /* Pin to one available logical CPU. Frequency scaling remains unchanged. */
    cpu_set_t available, chosen;
    assert(sched_getaffinity(0, sizeof available, &available) == 0);
    int cpu = 0;
    while (!CPU_ISSET(cpu, &available)) ++cpu;
    CPU_ZERO(&chosen);
    CPU_SET(cpu, &chosen);
    assert(sched_setaffinity(0, sizeof chosen, &chosen) == 0);
    fprintf(stderr, "cpu=%d lz4=%s zstd=%s blocks=%d trials=%d\n",
            cpu, LZ4_versionString(), ZSTD_versionString(), BLOCKS, TRIALS);
    struct codec codecs[] = {
        { .name = "lz4", .level = 0 }, { .name = "zstd-fast3", .level = -3 },
        { .name = "zstd-1", .level = 1 }, { .name = "zstd-3", .level = 3 },
        { .name = "zstd-1-dict8k", .level = 1 }
    };
    for (unsigned c = 0; c < 5; ++c) {
        struct codec *codec = &codecs[c];
        codec->workspace_size = codec->level == 0 ? (size_t)LZ4_sizeofState()
            : ZSTD_estimateCCtxSize(codec->level);
        assert(!ZSTD_isError(codec->workspace_size));
        assert(posix_memalign(&codec->workspace, 8, codec->workspace_size) == 0);
        memset(codec->workspace, 0, codec->workspace_size);
        if (codec->level) {
            codec->zstd = ZSTD_initStaticCCtx(codec->workspace, codec->workspace_size);
            assert(codec->zstd);
        }
    }
    puts("workload,block_bytes,codec,workspace_bytes,stored_bytes,ratio,median_us_per_block,min_us_per_block,max_us_per_block,input_MB_per_s");
    const char *names[] = { "scheduler", "mixed", "scheduler-quarter-full" };
    const size_t sizes[] = { 4096, 16384, 65536, 262144 };
    for (unsigned workload = 0; workload < 3; ++workload) {
        for (unsigned s = 0; s < 4; ++s) {
            size_t size = sizes[s], capacity = ZSTD_compressBound(size);
            if ((size_t)LZ4_compressBound((int)size) > capacity)
                capacity = (size_t)LZ4_compressBound((int)size);
            unsigned char *input = malloc(size * BLOCKS), *output = malloc(capacity);
            unsigned char *decoded = malloc(size);
            assert(input && output && decoded);
            for (unsigned b = 0; b < BLOCKS; ++b)
                generate(input + b * size, size, (int)workload, b);
            /* Train on different page seeds, outside the timing loop. */
            unsigned char *training = malloc(size * BLOCKS);
            assert(training);
            size_t sample_sizes[BLOCKS];
            for (unsigned b = 0; b < BLOCKS; ++b) {
                generate(training + b * size, size, (int)workload, b + 10000);
                sample_sizes[b] = size;
            }
            unsigned char dictionary_bytes[8192];
            size_t dictionary_size = ZDICT_trainFromBuffer(dictionary_bytes, sizeof dictionary_bytes,
                                                           training, sample_sizes, BLOCKS);
            assert(!ZDICT_isError(dictionary_size));
            free(training);
            codecs[4].dictionary = ZSTD_createCDict(dictionary_bytes, dictionary_size, 1);
            assert(codecs[4].dictionary);
            for (unsigned c = 0; c < 5; ++c) {
                struct codec *codec = &codecs[c];
                uint64_t stored = 0;
                /* Warm up and verify every distinct compressed block. */
                for (unsigned b = 0; b < BLOCKS; ++b) {
                    unsigned char *src = input + b * size;
                    size_t n = compress_block(codec, src, size, output, capacity);
                    stored += n < size ? n : size; /* Recorder's raw fallback. */
                    if (codec->dictionary) {
                        ZSTD_DCtx *dctx = ZSTD_createDCtx();
                        assert(dctx);
                        assert(ZSTD_decompress_usingDict(dctx, decoded, size, output, n,
                                                         dictionary_bytes, dictionary_size) == size);
                        ZSTD_freeDCtx(dctx);
                    } else if (codec->level) assert(ZSTD_decompress(decoded, size, output, n) == size);
                    else assert(LZ4_decompress_safe((char *)output, (char *)decoded,
                                                   (int)n, (int)size) == (int)size);
                    assert(memcmp(src, decoded, size) == 0);
                }
                double times[TRIALS];
                for (unsigned trial = 0; trial < TRIALS; ++trial) {
                    uint64_t start = now_ns(), elapsed = 0, count = 0;
                    do {
                        for (unsigned b = 0; b < BLOCKS; ++b)
                            compress_block(codec, input + b * size, size, output, capacity);
                        count += BLOCKS;
                        elapsed = now_ns() - start;
                    } while (elapsed < 30000000); /* At least 30 ms per trial. */
                    times[trial] = (double)elapsed / count / 1000;
                }
                qsort(times, TRIALS, sizeof *times, compare);
                printf("%s,%zu,%s,%zu,%.1f,%.3f,%.3f,%.3f,%.3f,%.1f\n",
                       names[workload], size, codec->name, codec->workspace_size + ZSTD_sizeof_CDict(codec->dictionary),
                       (double)stored / BLOCKS, (double)(size * BLOCKS) / stored,
                       times[TRIALS / 2], times[0], times[TRIALS - 1],
                       size / times[TRIALS / 2]);
                fflush(stdout);
            }
            ZSTD_freeCDict(codecs[4].dictionary);
            codecs[4].dictionary = NULL;
            free(input); free(output); free(decoded);
        }
    }
    for (unsigned c = 0; c < 5; ++c) free(codecs[c].workspace);
}
