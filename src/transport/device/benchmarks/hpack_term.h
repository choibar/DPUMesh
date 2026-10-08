/* Termination baseline for the HPACK bench: FULL decode (materialize every
 * field) + re-encode into a fresh HPACK context — the header work a
 * terminating proxy must do per request. Dependency-free (no libc), portable
 * to the DPA. Uses the varint/huffman-FSM primitives from hpack_walk.h and
 * the huffman ENCODE tables from dsb_blocks.h (huff_enc_code/bits).
 *
 * Scope: supports the representations and static-table indices the bench
 * generator emits (1,2,3,4,6,7,31,58). Unknown static refs return -1.
 */
#ifndef HPACK_TERM_H
#define HPACK_TERM_H

#include "../hpack_walk.h"

#define HT_NAME_CAP 24
#define HT_VAL_CAP  128
#define HT_TBL_CAP  64
#define HT_MAX_FIELDS 16

struct ht_field {
    unsigned char nlen, vlen;
    unsigned char noindex;       /* arrived as literal WITHOUT indexing     */
    char name[HT_NAME_CAP];
    char val[HT_VAL_CAP];
};

struct ht_entry {
    unsigned char nlen, vlen;
    char name[HT_NAME_CAP];
    char val[HT_VAL_CAP];
};

struct ht_table {                /* one per direction/endpoint */
    unsigned int head, count, size, max_size;
    struct ht_entry tbl[HT_TBL_CAP];
};

static inline void ht_init(struct ht_table *t)
{
    t->head = 0; t->count = 0; t->size = 0; t->max_size = 4096;
}

static inline struct ht_entry *ht_get(struct ht_table *t, unsigned int n)
{
    if (n >= t->count) return 0;
    return &t->tbl[(t->head + HT_TBL_CAP - n) % HT_TBL_CAP];
}

static inline void ht_evict(struct ht_table *t)
{
    while (t->count && t->size > t->max_size) {
        struct ht_entry *e =
            &t->tbl[(t->head + HT_TBL_CAP - (t->count - 1)) % HT_TBL_CAP];
        t->size -= (unsigned int)e->nlen + e->vlen + 32;
        t->count--;
    }
}

static inline void ht_insert(struct ht_table *t, const char *n, int nlen,
                             const char *v, int vlen)
{
    struct ht_entry *e;
    int j;
    t->head = (t->head + 1) % HT_TBL_CAP;
    if (t->count < HT_TBL_CAP) t->count++;
    e = &t->tbl[t->head];
    e->nlen = (unsigned char)nlen; e->vlen = (unsigned char)vlen;
    for (j = 0; j < nlen; j++) e->name[j] = n[j];
    for (j = 0; j < vlen; j++) e->val[j] = v[j];
    t->size += (unsigned int)nlen + vlen + 32;
    ht_evict(t);
}

/* --- static table subset (indices the generator emits) ------------------ */

static inline int ht_static(unsigned int idx, const char **n, int *nlen,
                            const char **v, int *vlen)
{
    switch (idx) {
    case 1:  *n = ":authority"; *nlen = 10; *v = "";      *vlen = 0; return 0;
    case 2:  *n = ":method";    *nlen = 7;  *v = "GET";   *vlen = 3; return 0;
    case 3:  *n = ":method";    *nlen = 7;  *v = "POST";  *vlen = 4; return 0;
    case 4:  *n = ":path";      *nlen = 5;  *v = "/";     *vlen = 1; return 0;
    case 6:  *n = ":scheme";    *nlen = 7;  *v = "http";  *vlen = 4; return 0;
    case 7:  *n = ":scheme";    *nlen = 7;  *v = "https"; *vlen = 5; return 0;
    case 31: *n = "content-type"; *nlen = 12; *v = "";    *vlen = 0; return 0;
    case 58: *n = "user-agent";   *nlen = 10; *v = "";    *vlen = 0; return 0;
    default: return -1;
    }
}

/* --- FULL decode: materialize all fields -------------------------------- */

static inline int ht_read_str(const unsigned char *b, int len, int *ip,
                              char *dst, int cap)
{
    unsigned int l; int h, dlen, j, i = *ip;
    if (i >= len) return -1;
    h = b[i] & 0x80;
    if (hw_varint(b, len, &i, 7, &l) || i + (int)l > len) return -1;
    if (h) {
        dlen = hw_huff(b + i, (int)l, dst, cap);
        if (dlen < 0) return -1;
    } else {
        if ((int)l > cap) return -1;
        for (j = 0; j < (int)l; j++) dst[j] = (char)b[i + j];
        dlen = (int)l;
    }
    *ip = i + (int)l;
    return dlen;
}

static inline int ht_decode(const unsigned char *b, int len,
                            struct ht_table *t,
                            struct ht_field *out, int *nout)
{
    int i = 0, nf = 0, j;
    while (i < len) {
        unsigned char c = b[i];
        unsigned int idx;
        struct ht_field *f;
        if (nf >= HT_MAX_FIELDS) return -1;
        f = &out[nf];

        if (c & 0x80) {                              /* indexed            */
            if (hw_varint(b, len, &i, 7, &idx) || idx == 0) return -1;
            f->noindex = 0;
            if (idx <= 61) {
                const char *n, *v; int nl, vl;
                if (ht_static(idx, &n, &nl, &v, &vl)) return -1;
                f->nlen = (unsigned char)nl; f->vlen = (unsigned char)vl;
                for (j = 0; j < nl; j++) f->name[j] = n[j];
                for (j = 0; j < vl; j++) f->val[j] = v[j];
            } else {
                struct ht_entry *e = ht_get(t, idx - 62);
                if (!e) return -1;
                f->nlen = e->nlen; f->vlen = e->vlen;
                for (j = 0; j < e->nlen; j++) f->name[j] = e->name[j];
                for (j = 0; j < e->vlen; j++) f->val[j] = e->val[j];
            }
            nf++;
        } else if ((c & 0x40) || !(c & 0x20)) {      /* literal            */
            int insert = (c & 0x40) != 0;
            int prefix = insert ? 6 : 4;
            int nl, vl;
            if (hw_varint(b, len, &i, prefix, &idx)) return -1;
            if (idx) {
                if (idx <= 61) {
                    const char *n, *v; int svl;
                    if (ht_static(idx, &n, &nl, &v, &svl)) return -1;
                    for (j = 0; j < nl; j++) f->name[j] = n[j];
                } else {
                    struct ht_entry *e = ht_get(t, idx - 62);
                    if (!e) return -1;
                    nl = e->nlen;
                    for (j = 0; j < nl; j++) f->name[j] = e->name[j];
                }
            } else {
                nl = ht_read_str(b, len, &i, f->name, HT_NAME_CAP);
                if (nl < 0) return -1;
            }
            vl = ht_read_str(b, len, &i, f->val, HT_VAL_CAP);
            if (vl < 0) return -1;
            f->nlen = (unsigned char)nl; f->vlen = (unsigned char)vl;
            f->noindex = (unsigned char)!insert;
            if (insert) ht_insert(t, f->name, nl, f->val, vl);
            nf++;
        } else {                                     /* table size update  */
            if (hw_varint(b, len, &i, 5, &idx)) return -1;
            t->max_size = idx;
            ht_evict(t);
        }
    }
    *nout = nf;
    return 0;
}

/* --- re-ENCODE into a fresh connection context -------------------------- */

struct ht_bitbuf { unsigned char *out; int cap, len; unsigned long long acc; int nbits; };

static inline int ht_huff_len(const char *s, int n)
{
    int i, bits = 0;
    for (i = 0; i < n; i++) bits += huff_enc_bits[(unsigned char)s[i]];
    return (bits + 7) / 8;
}

static inline int ht_put(struct ht_bitbuf *bb, unsigned char byte)
{
    if (bb->len >= bb->cap) return -1;
    bb->out[bb->len++] = byte;
    return 0;
}

static inline int ht_varint_out(struct ht_bitbuf *bb, unsigned int v,
                                int prefix, unsigned char first)
{
    unsigned int mask = (1u << prefix) - 1;
    if (v < mask) return ht_put(bb, first | (unsigned char)v);
    if (ht_put(bb, first | (unsigned char)mask)) return -1;
    v -= mask;
    while (v >= 128) {
        if (ht_put(bb, 0x80 | (v & 0x7f))) return -1;
        v >>= 7;
    }
    return ht_put(bb, (unsigned char)v);
}

static inline int ht_str_out(struct ht_bitbuf *bb, const char *s, int n)
{
    int hl = ht_huff_len(s, n), i;
    if (hl < n) {
        unsigned long long acc = 0; int nbits = 0;
        if (ht_varint_out(bb, (unsigned int)hl, 7, 0x80)) return -1;
        for (i = 0; i < n; i++) {
            unsigned char c = (unsigned char)s[i];
            acc = (acc << huff_enc_bits[c]) | huff_enc_code[c];
            nbits += huff_enc_bits[c];
            while (nbits >= 8) {
                nbits -= 8;
                if (ht_put(bb, (unsigned char)(acc >> nbits))) return -1;
            }
        }
        if (nbits) {
            int pad = 8 - nbits;
            if (ht_put(bb, (unsigned char)((acc << pad) | ((1u << pad) - 1))))
                return -1;
        }
    } else {
        if (ht_varint_out(bb, (unsigned int)n, 7, 0x00)) return -1;
        for (i = 0; i < n; i++)
            if (ht_put(bb, (unsigned char)s[i])) return -1;
    }
    return 0;
}

static inline int ht_memeq(const char *a, const char *b, int n)
{
    int i;
    for (i = 0; i < n; i++) if (a[i] != b[i]) return 0;
    return 1;
}

/* static exact / name matches for what we emit */
static inline unsigned int ht_static_exact(const struct ht_field *f)
{
    if (f->nlen == 7 && ht_memeq(f->name, ":method", 7)) {
        if (f->vlen == 4 && ht_memeq(f->val, "POST", 4)) return 3;
        if (f->vlen == 3 && ht_memeq(f->val, "GET", 3)) return 2;
    }
    if (f->nlen == 7 && ht_memeq(f->name, ":scheme", 7)) {
        if (f->vlen == 4 && ht_memeq(f->val, "http", 4)) return 6;
        if (f->vlen == 5 && ht_memeq(f->val, "https", 5)) return 7;
    }
    return 0;
}

static inline unsigned int ht_static_name(const struct ht_field *f)
{
    if (f->nlen == 10 && ht_memeq(f->name, ":authority", 10)) return 1;
    if (f->nlen == 5 && ht_memeq(f->name, ":path", 5)) return 4;
    if (f->nlen == 12 && ht_memeq(f->name, "content-type", 12)) return 31;
    if (f->nlen == 10 && ht_memeq(f->name, "user-agent", 10)) return 58;
    return 0;
}

static inline int ht_encode(const struct ht_field *fields, int nf,
                            struct ht_table *t,
                            unsigned char *out, int cap)
{
    struct ht_bitbuf bb = { out, cap, 0, 0, 0 };
    int k;
    for (k = 0; k < nf; k++) {
        const struct ht_field *f = &fields[k];
        unsigned int sidx = ht_static_exact(f);
        unsigned int i, hit = 0, namehit = 0;

        if (f->noindex) {        /* preserve without-indexing policy       */
            for (i = 0; i < t->count; i++) {
                struct ht_entry *e = ht_get(t, i);
                if (e->nlen == f->nlen && ht_memeq(e->name, f->name, f->nlen)) {
                    namehit = 62 + i; break;
                }
            }
            if (namehit) {
                if (ht_varint_out(&bb, namehit, 4, 0x00)) return -1;
            } else {
                sidx = ht_static_name(f);
                if (ht_varint_out(&bb, sidx, 4, 0x00)) return -1;
                if (!sidx && ht_str_out(&bb, f->name, f->nlen)) return -1;
            }
            if (ht_str_out(&bb, f->val, f->vlen)) return -1;
            continue;            /* no table insert */
        }
        if (sidx) {                                  /* fully indexed (static) */
            if (ht_varint_out(&bb, sidx, 7, 0x80)) return -1;
            continue;
        }
        for (i = 0; i < t->count; i++) {             /* dynamic exact match    */
            struct ht_entry *e = ht_get(t, i);
            if (e->nlen == f->nlen && e->vlen == f->vlen &&
                ht_memeq(e->name, f->name, f->nlen) &&
                ht_memeq(e->val, f->val, f->vlen)) { hit = 62 + i; break; }
            if (!namehit && e->nlen == f->nlen &&
                ht_memeq(e->name, f->name, f->nlen)) namehit = 62 + i;
        }
        if (hit) {
            if (ht_varint_out(&bb, hit, 7, 0x80)) return -1;
            continue;
        }
        /* literal with incremental indexing */
        if (namehit) {
            if (ht_varint_out(&bb, namehit, 6, 0x40)) return -1;
        } else {
            sidx = ht_static_name(f);
            if (ht_varint_out(&bb, sidx, 6, 0x40)) return -1;   /* 0 → literal name */
            if (!sidx && ht_str_out(&bb, f->name, f->nlen)) return -1;
        }
        if (ht_str_out(&bb, f->val, f->vlen)) return -1;
        ht_insert(t, f->name, f->nlen, f->val, f->vlen);
    }
    return bb.len;
}

#endif /* HPACK_TERM_H */
