/*
 * Author: cfsego
 */
#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_stream.h>


static ngx_rbtree_t                  ngx_stream_limit_ups_loc_rbtree;
static ngx_rbtree_node_t             ngx_stream_limit_ups_sentinel;


typedef struct {
    ngx_uint_t                       limit_conn;
    ngx_uint_t                       backlog;
    ngx_msec_t                       timeout;
    ngx_shm_zone_t                  *shm_zone;

    ngx_stream_upstream_init_peer_pt init;

    ngx_uint_t                       log_level;

    unsigned                         hooked:1;
} ngx_stream_limit_ups_conf_t;


typedef struct {
    ngx_rbtree_t                    *rbtree;
    ngx_queue_t                     *queue;
} ngx_stream_limit_ups_zone_t;


typedef struct {
    u_char                           color;
    unsigned short                   port;
    ngx_uint_t                       counter;
} ngx_stream_limit_ups_node_t;


typedef struct {
    volatile ngx_uint_t              counter;
    ngx_msec_t                       last;
    ngx_queue_t                      queue;
} ngx_stream_limit_ups_shm_t;


typedef struct {
    ngx_uint_t                       counter;
    ngx_uint_t                       qlen;
    ngx_uint_t                       work;
    ngx_queue_t                      wait;
} ngx_stream_limit_ups_loc_t;


typedef struct {
    ngx_stream_session_t            *s;
    ngx_stream_limit_ups_conf_t     *lucf;
    ngx_stream_limit_ups_loc_t      *lnode;
    void                            *data;
    void                            *wait;

    unsigned                         in_proc:1;
    unsigned                         cln:1;

    ngx_event_get_peer_pt            get;
    ngx_event_free_peer_pt           free;
    ngx_event_notify_peer_pt         notify;

#if (NGX_HTTP_SSL)
    ngx_event_set_peer_session_pt    set_session;
    ngx_event_save_peer_session_pt   save_session;
#endif

    struct sockaddr                 *sockaddr;
} ngx_stream_limit_ups_ctx_t;


typedef struct {
    ngx_queue_t                      queue;
    ngx_stream_limit_ups_ctx_t      *ctx;
    ngx_event_handler_pt             read_event_handler;
    ngx_event_handler_pt             write_event_handler;
    unsigned                         r_timer_set:1;
    unsigned                         w_timer_set:1;
} ngx_stream_limit_ups_wait_t;


typedef struct {
    ngx_msec_t                       connect_timeout;
    ngx_msec_t                       timeout;
} ngx_stream_proxy_srv_conf_t;


static ngx_conf_enum_t ngx_stream_limit_ups_log_levels[] = {
    { ngx_string("info"), NGX_LOG_INFO },
    { ngx_string("notice"), NGX_LOG_NOTICE },
    { ngx_string("warn"), NGX_LOG_WARN },
    { ngx_string("error"), NGX_LOG_ERR },
    { ngx_null_string, 0 }
};


static void ngx_stream_limit_ups_block_reading(ngx_event_t *ev);
static void ngx_stream_limit_ups_timeout(ngx_event_t *ev);
static void ngx_stream_limit_ups_cleanup(void *data);
static void ngx_stream_limit_ups_notify_peer(ngx_peer_connection_t *pc,
    void *data, ngx_uint_t state);
static void ngx_stream_limit_ups_free_peer(ngx_peer_connection_t *pc,
    void *data, ngx_uint_t state);
static ngx_int_t ngx_stream_limit_ups_get_peer(ngx_peer_connection_t *pc,
    void *data);
#if (NGX_HTTP_SSL)
static ngx_int_t ngx_stream_limit_ups_set_peer_session(
    ngx_peer_connection_t *pc, void *data);
static void ngx_stream_limit_ups_save_peer_session(ngx_peer_connection_t *pc,
    void *data);
#endif

static ngx_int_t ngx_stream_limit_ups_init_peer(ngx_stream_session_t *s,
    ngx_stream_upstream_srv_conf_t *us);

static void *ngx_stream_limit_ups_create_srv_conf(ngx_conf_t *cf);
static char *ngx_stream_limit_ups_merge_srv_conf(ngx_conf_t *cf, void *parent,
    void *child);
static char *ngx_stream_limit_ups_zone(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static char *ngx_stream_limit_ups_conn(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static ngx_int_t ngx_stream_limit_ups_init(ngx_conf_t *cf);

static ngx_int_t ngx_stream_limit_ups_init_zone(ngx_shm_zone_t *shm_zone,
    void *data);
static void ngx_stream_limit_ups_zone_expire(ngx_shm_zone_t *shm_zone);

static void
    ngx_stream_limit_ups_rbtree_insert_value(ngx_rbtree_node_t *temp,
    ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel);
static ngx_rbtree_node_t *
    ngx_stream_limit_ups_rbtree_lookup(ngx_rbtree_t *rbtree,
    unsigned long addr, unsigned short port);


static ngx_command_t ngx_stream_limit_ups_commands[] = {

    { ngx_string("limit_upstream_zone"),
      NGX_STREAM_MAIN_CONF|NGX_CONF_TAKE2,
      ngx_stream_limit_ups_zone,
      0,
      0,
      NULL },

    { ngx_string("limit_upstream_conn"),
      NGX_STREAM_UPS_CONF|NGX_CONF_TAKE1234|NGX_CONF_TAKE5,
      ngx_stream_limit_ups_conn,
      NGX_STREAM_SRV_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("limit_upstream_log_level"),
      NGX_STREAM_MAIN_CONF|NGX_STREAM_UPS_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_enum_slot,
      NGX_STREAM_SRV_CONF_OFFSET,
      offsetof(ngx_stream_limit_ups_conf_t, log_level),
      &ngx_stream_limit_ups_log_levels },

      ngx_null_command
};


static ngx_stream_module_t ngx_stream_limit_ups_module_ctx = {
    NULL,
    ngx_stream_limit_ups_init,

    NULL,
    NULL,

    ngx_stream_limit_ups_create_srv_conf,
    ngx_stream_limit_ups_merge_srv_conf
};


ngx_module_t ngx_stream_limit_upstream_module = {
    NGX_MODULE_V1,
    &ngx_stream_limit_ups_module_ctx,
    ngx_stream_limit_ups_commands,
    NGX_STREAM_MODULE,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NGX_MODULE_V1_PADDING
};


extern void ngx_stream_proxy_finalize(ngx_stream_session_t *s, ngx_uint_t rc);
extern void ngx_stream_proxy_connect(ngx_stream_session_t *s);
extern ngx_module_t ngx_stream_proxy_module;


static void
ngx_stream_limit_ups_block_reading(ngx_event_t *ev)
{
    ngx_connection_t             *c;
    ngx_stream_session_t         *s;

    c = ev->data;
    s = c->data;

    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, c->log, 0,
                   "limit upstream: stream reading blocked");

    /* aio does not call this handler */

    if ((ngx_event_flags & NGX_USE_LEVEL_EVENT) && c->read->active)
    {
        if (ngx_del_event(c->read, NGX_READ_EVENT, 0) != NGX_OK) {
            ngx_stream_proxy_finalize(s, NGX_STREAM_INTERNAL_SERVER_ERROR);
        }
    }
}


static void
ngx_stream_limit_ups_timeout(ngx_event_t *ev)
{
    ngx_connection_t             *c;
    ngx_stream_session_t         *s;
    ngx_stream_upstream_t        *u;
    ngx_stream_limit_ups_wait_t  *w;
    ngx_stream_limit_ups_loc_t   *l;
    ngx_stream_limit_ups_ctx_t   *ctx;

    c = ev->data;
    s = c->data;

    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, c->log, 0,
                   "limit upstream: into timeout");

    if (!ev->timedout) {
        ngx_log_debug0(NGX_LOG_DEBUG_STREAM, c->log, 0,
                       "limit upstream: wev ignored");
        return;
    }

    u = s->upstream;

    ctx = (ngx_stream_limit_ups_ctx_t *) u->peer.data;

    ngx_log_error(ctx->lucf->log_level, c->log, 0,
                  "limit upstream: session[%p] is timeout", s);

    ev->timedout = 0;

    w = ctx->wait;
    ngx_queue_remove(&w->queue);

    l = ctx->lnode;
    l->qlen--;

    ngx_stream_proxy_finalize(s, NGX_STREAM_SERVICE_UNAVAILABLE);
}


static void
ngx_stream_limit_ups_cleanup(void *data)
{
    ngx_queue_t                  *q;
    ngx_rbtree_node_t            *node_s, *node_l;
    struct sockaddr_in           *sin;
#if NGX_DEBUG
    ngx_peer_connection_t        *pc;
#endif
    ngx_stream_limit_ups_ctx_t   *ctx;
    ngx_stream_limit_ups_loc_t   *lnode;
    ngx_stream_limit_ups_shm_t   *snode;
    ngx_stream_limit_ups_node_t  *cnode;
    ngx_stream_limit_ups_wait_t  *wnode;
    ngx_stream_limit_ups_zone_t  *shmctx;

    ctx = (ngx_stream_limit_ups_ctx_t *) data;
    shmctx = ctx->lucf->shm_zone->data;

    if (!ctx->in_proc) {
        return;
    }

#if NGX_DEBUG
    pc = &ctx->s->upstream->peer;
#endif
    sin = (struct sockaddr_in *) ctx->sockaddr;

    node_s = ngx_stream_limit_ups_rbtree_lookup(shmctx->rbtree,
                                                sin->sin_addr.s_addr,
                                                sin->sin_port);

    node_l = ngx_stream_limit_ups_rbtree_lookup(
                    &ngx_stream_limit_ups_loc_rbtree,
                    sin->sin_addr.s_addr,
                    sin->sin_port);

    if (node_l == NULL && node_s == NULL) {
        return;
    }

#if 1

    if ((node_l == NULL && node_s) || (node_s == NULL && node_l)) {
        ngx_log_error(NGX_LOG_EMERG, ctx->s->connection->log, 0,
                      "limit upstream: only local or shm node exists");
        return;
    }

#endif

    cnode = (ngx_stream_limit_ups_node_t *) &node_s->color;
    snode = (ngx_stream_limit_ups_shm_t *) &cnode->counter;

    cnode = (ngx_stream_limit_ups_node_t *) &node_l->color;
    lnode = (ngx_stream_limit_ups_loc_t *) &cnode->counter;

    lnode->work--;

    if ((snode->counter > ctx->lucf->limit_conn && lnode->work)
        || lnode->qlen == 0)
    {
        ngx_log_debug4(NGX_LOG_DEBUG_STREAM, ctx->s->connection->log, 0,
                       "limit upstream: will not resume session for %V "
                       "(counter: %d, active: %d, wait queue: %d)",
                       pc->name, snode->counter, lnode->work, lnode->qlen);

        snode->counter--;
        lnode->counter--;
        return;
    }

    do {
        q = ngx_queue_last(&lnode->wait);
        wnode = ngx_queue_data(q, ngx_stream_limit_ups_wait_t, queue);

        ngx_log_debug5(NGX_LOG_DEBUG_STREAM, ctx->s->connection->log, 0,
                       "limit upstream: remove queue node: %p for %V "
                       "(counter: %d, active: %d, wait queue: %d)",
                       q, pc->name, snode->counter,
                       lnode->work, lnode->qlen);

        ngx_queue_remove(q);
        lnode->qlen--;

        /* resume a request in wait queue */

        ctx = wnode->ctx;

        ngx_log_error(ctx->lucf->log_level, ctx->s->connection->log, 0,
                      "limit upstream: session[%p] is resumed", ctx->s);

        ctx->s->connection->read->handler = wnode->read_event_handler;
        ctx->s->connection->write->handler = wnode->write_event_handler;

        if (wnode->r_timer_set) {
            if (ctx->s->connection->read->timedout) {
                ctx->s->connection->read->handler(ctx->s->connection->read);
                return;
            }

            if (ngx_handle_read_event(ctx->s->connection->read, 0)
                != NGX_OK)
            {
                ngx_stream_proxy_finalize(ctx->s,
                                          NGX_STREAM_INTERNAL_SERVER_ERROR);
                return;
            }

            ngx_add_timer(ctx->s->connection->read,
                          ctx->s->connection->read->timer.key);
        }

        ngx_del_timer(ctx->s->connection->write);

        if (wnode->w_timer_set) {
            if (ctx->s->connection->write->timedout) {
                ctx->s->connection->write->handler(ctx->s->connection->write);
                return;
            }


            if (ngx_handle_write_event(ctx->s->connection->write, 0)
                != NGX_OK)
            {
                ngx_stream_proxy_finalize(ctx->s,
                                          NGX_STREAM_INTERNAL_SERVER_ERROR);
                return;
            }

            ngx_add_timer(ctx->s->connection->write,
                          ctx->s->connection->write->timer.key);
        }

        lnode->work++;

        ngx_stream_proxy_connect(ctx->s);
    } while ((ngx_uint_t) ngx_atomic_fetch_add(&snode->counter, 1)
             < ctx->lucf->limit_conn && lnode->qlen);

    snode->counter--;
}


static void
ngx_stream_limit_ups_notify_peer(ngx_peer_connection_t *pc, void *data,
    ngx_uint_t state)
{
    ngx_stream_limit_ups_ctx_t   *ctx;

    ctx = (ngx_stream_limit_ups_ctx_t *) data;

    if (ctx->notify) {
        ctx->notify(pc, ctx->data, state);
    }
}


static void
ngx_stream_limit_ups_free_peer(ngx_peer_connection_t *pc, void *data,
    ngx_uint_t state)
{
    ngx_stream_limit_ups_ctx_t   *ctx;

    ctx = (ngx_stream_limit_ups_ctx_t *) data;

    ctx->free(pc, ctx->data, state);
}


static ngx_int_t
ngx_stream_limit_ups_get_peer(ngx_peer_connection_t *pc, void *data)
{
    size_t                        n;
    ngx_int_t                     rc;
#if (NGX_DEBUG)
    ngx_uint_t                    active;
#endif
    ngx_slab_pool_t              *shpool;
    ngx_rbtree_node_t            *node_s, *node_l;
    struct sockaddr_in           *sin;
    ngx_pool_cleanup_t           *cln;
    ngx_rbtree_key_int_t          t;
    ngx_stream_limit_ups_ctx_t   *ctx;
    ngx_stream_limit_ups_loc_t   *lnode;
    ngx_stream_limit_ups_shm_t   *snode;
    ngx_stream_limit_ups_node_t  *cnode;
    ngx_stream_limit_ups_wait_t  *wnode;
    ngx_stream_limit_ups_zone_t  *shmctx;
    ngx_stream_proxy_srv_conf_t  *pscf;

    ctx = (ngx_stream_limit_ups_ctx_t *) data;
    shpool = (ngx_slab_pool_t *) ctx->lucf->shm_zone->shm.addr;
    shmctx = ctx->lucf->shm_zone->data;

    if (ctx->s->upstream->blocked) {
        rc = NGX_OK;
        ctx->s->upstream->blocked = 0;

        goto set_and_ret;

    } else {

        if (ctx->in_proc) {

            sin = (struct sockaddr_in *) ctx->sockaddr;
            node_s = ngx_stream_limit_ups_rbtree_lookup(shmctx->rbtree,
                                                        sin->sin_addr.s_addr,
                                                        sin->sin_port);

            node_l = ngx_stream_limit_ups_rbtree_lookup(
                                            &ngx_stream_limit_ups_loc_rbtree,
                                            sin->sin_addr.s_addr,
                                            sin->sin_port);

            if (node_l && node_s) {
                cnode = (ngx_stream_limit_ups_node_t *) &node_s->color;
                snode = (ngx_stream_limit_ups_shm_t *) &cnode->counter;

                cnode = (ngx_stream_limit_ups_node_t *) &node_l->color;
                lnode = (ngx_stream_limit_ups_loc_t *) &cnode->counter;

                lnode->work--;
                snode->counter--;
                lnode->counter--;
            }

            ctx->in_proc = 0;
        }

        rc = ctx->get(pc, ctx->data);
        if (rc != NGX_OK && rc != NGX_DONE) {
            return rc;
        }
    }

    ctx->sockaddr = ngx_palloc(ctx->s->connection->pool, pc->socklen);
    if (ctx->sockaddr == NULL) {
        return NGX_ERROR;
    }

    ngx_memcpy(ctx->sockaddr, pc->sockaddr, pc->socklen);

    sin = (struct sockaddr_in *) ctx->sockaddr;

    node_s = ngx_stream_limit_ups_rbtree_lookup(shmctx->rbtree,
                                                sin->sin_addr.s_addr,
                                                sin->sin_port);

    node_l = ngx_stream_limit_ups_rbtree_lookup(
                    &ngx_stream_limit_ups_loc_rbtree,
                    sin->sin_addr.s_addr,
                    sin->sin_port);

    if (node_l == NULL) {
        n = offsetof(ngx_rbtree_node_t, color)
          + offsetof(ngx_stream_limit_ups_node_t, counter)
          + sizeof(ngx_stream_limit_ups_loc_t);

        node_l = ngx_pcalloc(ngx_cycle->pool, n);
        if (node_l == NULL) {
            return NGX_ERROR;
        }

        cnode = (ngx_stream_limit_ups_node_t *) &node_l->color;
        lnode = (ngx_stream_limit_ups_loc_t *) &cnode->counter;

        node_l->key = sin->sin_addr.s_addr;
        cnode->port = sin->sin_port;

        ngx_queue_init(&lnode->wait);

        ngx_rbtree_insert(&ngx_stream_limit_ups_loc_rbtree, node_l);

    } else {
        cnode = (ngx_stream_limit_ups_node_t *) &node_l->color;
        lnode = (ngx_stream_limit_ups_loc_t *) &cnode->counter;
    }

    if (node_s == NULL) {

        ngx_shmtx_lock(&shpool->mutex);

        n = offsetof(ngx_rbtree_node_t, color)
          + offsetof(ngx_stream_limit_ups_node_t, counter)
          + sizeof(ngx_stream_limit_ups_shm_t);

        node_s = ngx_slab_alloc_locked(shpool, n);
        if (node_s == NULL) {
            return NGX_ERROR;
        }

        cnode = (ngx_stream_limit_ups_node_t *) &node_s->color;
        snode = (ngx_stream_limit_ups_shm_t *) &cnode->counter;

        node_s->key = sin->sin_addr.s_addr;
        cnode->port = sin->sin_port;

        ngx_queue_insert_head(shmctx->queue, &snode->queue);

        ngx_rbtree_insert(shmctx->rbtree, node_s);

        ngx_shmtx_unlock(&shpool->mutex);

    } else {
        cnode = (ngx_stream_limit_ups_node_t *) &node_s->color;
        snode = (ngx_stream_limit_ups_shm_t *) &cnode->counter;
    }

    snode->last = ngx_current_msec;

    if (rc == NGX_DONE) {
        snode->counter++;
        lnode->counter++;
        lnode->work++;
        goto set_and_ret;
    }

    ngx_log_debug4(NGX_LOG_DEBUG_STREAM, ctx->s->connection->log, 0,
                   "limit upstream: status for %V "
                   "(counter: %d, active: %d, wait queue: %d)",
                   pc->name, snode->counter, lnode->work, lnode->qlen);

#if (NGX_DEBUG)
    active = ngx_atomic_fetch_add(&snode->counter, 1);

    if (active >= ctx->lucf->limit_conn && lnode->work) {
#else
    if ((ngx_uint_t) ngx_atomic_fetch_add(&snode->counter, 1)
        >= ctx->lucf->limit_conn && lnode->work)
    {
#endif

        snode->counter--;

        if (lnode->qlen >= ctx->lucf->backlog) {
            ngx_log_error(ctx->lucf->log_level, ctx->s->connection->log, 0,
                          "limit upstream: session[%p] is dropped", ctx->s);
            return NGX_DECLINED;
        }

        wnode = ngx_pcalloc(ctx->s->connection->pool,
                            sizeof(ngx_stream_limit_ups_wait_t));
        if (wnode == NULL) {
            return NGX_ERROR;
        }

        ngx_queue_insert_head(&lnode->wait, &wnode->queue);
        lnode->qlen++;

        ngx_log_debug1(NGX_LOG_DEBUG_STREAM, ctx->s->connection->log, 0,
                       "limit upstream: add queue node: %p", &wnode->queue);

        ngx_log_error(ctx->lucf->log_level, ctx->s->connection->log, 0,
                      "limit upstream: session[%p] is blocked", ctx->s);

        wnode->ctx = ctx;
        ctx->wait = wnode;
        ctx->lnode = lnode;

        wnode->read_event_handler = ctx->s->connection->read->handler;
        wnode->r_timer_set = ctx->s->connection->read->timer_set;
        ctx->s->connection->read->handler = ngx_stream_limit_ups_block_reading;
        if (wnode->r_timer_set) {
            ngx_del_timer(ctx->s->connection->read);

            t = ctx->s->connection->read->timer.key;
            t -= (ngx_rbtree_key_int_t) ngx_current_msec;

            if (t > 0) {
                ctx->s->connection->read->timer.key = t;

            } else {
                ctx->s->connection->read->timedout = 1;
            }
        }

        wnode->write_event_handler = ctx->s->connection->write->handler;
        wnode->w_timer_set = ctx->s->connection->write->timer_set;
        ctx->s->connection->write->handler = ngx_stream_limit_ups_timeout;
        if (wnode->w_timer_set) {
            ngx_del_timer(ctx->s->connection->write);

            t = ctx->s->connection->write->timer.key;
            t -= (ngx_rbtree_key_int_t) ngx_current_msec;

            if (t > 0) {
                ctx->s->connection->write->timer.key = t;
            } else {
                ctx->s->connection->write->timedout = 1;
            }
        }

        if (ctx->lucf->timeout == 0) {
            pscf = ngx_stream_get_module_srv_conf(ctx->s,
                                                  ngx_stream_proxy_module);
            ngx_add_timer(ctx->s->connection->write, pscf->timeout);

        } else {
            ngx_add_timer(ctx->s->connection->write, ctx->lucf->timeout);
        }

        ctx->s->upstream->blocked = 1;

        return NGX_BLOCK;
    }

#if (NGX_DEBUG)

    if (active >= ctx->lucf->limit_conn) {
        ngx_log_debug0(NGX_LOG_DEBUG_STREAM, ctx->s->connection->log, 0,
                       "limit upstream: force continue request");
    }

#endif

    lnode->counter++;
    lnode->work++;

set_and_ret:

    if (!ctx->cln) {
        cln = ngx_pool_cleanup_add(ctx->s->connection->pool, 0);
        if (cln == NULL) {
            return NGX_ERROR;
        }

        cln->handler = ngx_stream_limit_ups_cleanup;
        cln->data = ctx;

        ctx->cln = 1;
    }

    ctx->in_proc = 1;

    return rc;
}


static ngx_int_t
ngx_stream_limit_ups_init_peer(ngx_stream_session_t *s,
    ngx_stream_upstream_srv_conf_t *us)
{
    ngx_int_t                     rc;
    ngx_stream_limit_ups_ctx_t   *ctx;
    ngx_stream_limit_ups_conf_t  *lucf;

    ctx = ngx_pcalloc(s->connection->pool,
                      sizeof(ngx_stream_limit_ups_ctx_t));
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    lucf = us->srv_conf[ngx_stream_limit_upstream_module.ctx_index];

    rc = lucf->init(s, us);

    if (rc != NGX_OK) {
        return rc;
    }

    ctx->data = s->upstream->peer.data;
    s->upstream->peer.data = ctx;

    ctx->get = s->upstream->peer.get;
    s->upstream->peer.get = ngx_stream_limit_ups_get_peer;

    ctx->free = s->upstream->peer.free;
    s->upstream->peer.free = ngx_stream_limit_ups_free_peer;

    ctx->notify = s->upstream->peer.notify;
    s->upstream->peer.notify = ngx_stream_limit_ups_notify_peer;

#if (NGX_HTTP_SSL)
    ctx->set_session = s->upstream->peer.set_session;
    s->upstream->peer.set_session = ngx_stream_limit_ups_set_peer_session;

    ctx->save_session = s->upstream->peer.save_session;
    s->upstream->peer.save_session = ngx_stream_limit_ups_save_peer_session;
#endif

    ctx->s = s;
    ctx->lucf = lucf;
    s->upstream->blocked = 0;

    return NGX_OK;
}


static ngx_int_t
ngx_stream_limit_ups_init(ngx_conf_t *cf)
{
    ngx_uint_t                         i;
    ngx_stream_limit_ups_conf_t       *lucf, *mlucf;
    ngx_stream_upstream_srv_conf_t   **uscfp;
    ngx_stream_upstream_main_conf_t   *umcf;

    umcf = ngx_stream_conf_get_module_main_conf(cf,
                                                ngx_stream_upstream_module);
    mlucf = ngx_stream_conf_get_module_srv_conf(cf,
                                              ngx_stream_limit_upstream_module);
    uscfp = umcf->upstreams.elts;

    ngx_rbtree_init(&ngx_stream_limit_ups_loc_rbtree,
                    &ngx_stream_limit_ups_sentinel,
                    ngx_stream_limit_ups_rbtree_insert_value);

    for (i = 0; i < umcf->upstreams.nelts; i++) {

        if (uscfp[i]->srv_conf) {
            lucf = uscfp[i]->
                srv_conf[ngx_stream_limit_upstream_module.ctx_index];

            if (lucf->limit_conn != NGX_CONF_UNSET_UINT && !lucf->hooked) {
                lucf->init = uscfp[i]->peer.init;
                uscfp[i]->peer.init = ngx_stream_limit_ups_init_peer;

                if (lucf->backlog == NGX_CONF_UNSET_UINT) {
                    lucf->backlog = 1000;
                }

                if (lucf->timeout == NGX_CONF_UNSET_MSEC) {
                    lucf->timeout = 0;
                }

                if (ngx_stream_limit_ups_merge_srv_conf(cf, mlucf, lucf)
                    != NGX_CONF_OK)
                {
                    return NGX_ERROR;
                }
            }
        }
    }

    return NGX_OK;
}


static void *
ngx_stream_limit_ups_create_srv_conf(ngx_conf_t *cf)
{
    ngx_stream_limit_ups_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_stream_limit_ups_conf_t));
    if (conf == NULL) {
        return NULL;
    }

    /*
     * set by ngx_pcalloc();
     *
     * conf->init = NULL;
     * conf->shm_zone = NULL;
     * conf->hooked = 0;
     */

    conf->limit_conn = NGX_CONF_UNSET_UINT;
    conf->backlog = NGX_CONF_UNSET_UINT;
    conf->timeout = NGX_CONF_UNSET_MSEC;
    conf->log_level = NGX_CONF_UNSET_UINT;

    return conf;
}


static char *
ngx_stream_limit_ups_merge_srv_conf(ngx_conf_t *cf, void *parent,
    void *child)
{
    ngx_stream_limit_ups_conf_t  *conf = child;
    ngx_stream_limit_ups_conf_t  *prev = parent;

    ngx_conf_merge_uint_value(conf->log_level,
                              prev->log_level, NGX_LOG_NOTICE);

    return NGX_CONF_OK;
}


static char *
ngx_stream_limit_ups_zone(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ssize_t                       n;
    ngx_str_t                    *value;
    ngx_shm_zone_t               *shm_zone;
    ngx_stream_limit_ups_zone_t  *ctx;

    value = cf->args->elts;

    n = ngx_parse_size(&value[2]);
    if (n == NGX_ERROR) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "invalid size of limit_upstream_zone \"%V\"",
                           &value[2]);
        return NGX_CONF_ERROR;
    }

    if (n < (ngx_int_t) (8 * ngx_pagesize)) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "limit_upstream_zone \"%V\" is too small",
                           &value[1]);
        return NGX_CONF_ERROR;
    }

    shm_zone = ngx_shared_memory_add(cf, &value[1], n,
                                     &ngx_stream_limit_upstream_module);
    if (shm_zone == NULL) {
        return NGX_CONF_ERROR;
    }

    if (shm_zone->data) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "limit_upstream_zone \"%V\" is duplicate",
                           &value[1]);
        return NGX_CONF_ERROR;
    }

    ctx = ngx_pcalloc(cf->pool, sizeof(ngx_stream_limit_ups_zone_t));
    if (ctx == NULL) {
        return NGX_CONF_ERROR;
    }

    shm_zone->data = ctx;
    shm_zone->init = ngx_stream_limit_ups_init_zone;

    return NGX_CONF_OK;
}


static char *
ngx_stream_limit_ups_conn(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_str_t                         *value, s;
    ngx_uint_t                         i;
    ngx_stream_upstream_srv_conf_t    *uscf;

    ngx_stream_limit_ups_conf_t       *lucf = conf;

    if (lucf->shm_zone) {
        return "is duplicate";
    }

    uscf = ngx_stream_conf_get_module_srv_conf(cf, ngx_stream_upstream_module);

    value = cf->args->elts;

    for (i = 1; i < cf->args->nelts; i++) {

        if (ngx_strncmp("zone=", value[i].data, 5) == 0) {

            s.len = value[i].len - 5;
            s.data = value[i].data + 5;

            lucf->shm_zone = ngx_shared_memory_add(cf, &s, 0,
                                             &ngx_stream_limit_upstream_module);
            if (lucf->shm_zone == NULL) {
                return NGX_CONF_ERROR;
            }

            continue;
        }

        if (ngx_strncmp("limit=", value[i].data, 6) == 0) {
            lucf->limit_conn = ngx_atoi(value[i].data + 6, value[i].len - 6);
            if (lucf->limit_conn <= 0) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                                   "invalid limit \"%V\"", &value[i]);
                return NGX_CONF_ERROR;
            }

            continue;
        }

        if (ngx_strncmp("backlog=", value[i].data, 8) == 0) {

            lucf->backlog = ngx_atoi(value[i].data + 8, value[i].len - 8);
            if (lucf->backlog <= 0) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                                   "invalid backlog \"%V\"", &value[i]);
                return NGX_CONF_ERROR;
            }

            continue;
        }

        if (ngx_strncmp("nodelay", value[i].data, 7) == 0) {
            lucf->backlog = 0;
            continue;
        }

        if (ngx_strncmp("timeout=", value[i].data, 8) == 0) {

            s.len = value[i].len - 8;
            s.data = value[i].data + 8;

            lucf->timeout = ngx_parse_time(&s, 0);
            if (lucf->timeout == (ngx_msec_t) NGX_ERROR) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                                   "invalid timeout \"%V\"", &value[i]);
                return NGX_CONF_ERROR;
            }

            continue;
        }

        if (ngx_strncmp("instant_hook", value[i].data, 12) == 0) {
            lucf->init = uscf->peer.init;
            uscf->peer.init = ngx_stream_limit_ups_init_peer;
            lucf->hooked = 1;
            continue;
        }

        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "invalid parameter \"%V\"", &value[i]);
        return NGX_CONF_ERROR;
    }

    if (lucf->limit_conn == NGX_CONF_UNSET_UINT) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "\"%V\" must have \"limit\" parameter",
                           &cmd->name);
        return NGX_CONF_ERROR;
    }

    if (lucf->shm_zone == NULL) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "\"%V\" must have \"zone\" parameter",
                           &cmd->name);
        return NGX_CONF_ERROR;
    }

    if (lucf->hooked) {
        if (lucf->backlog == NGX_CONF_UNSET_UINT) {
            lucf->backlog = 1000;
        }

        if (lucf->timeout == NGX_CONF_UNSET_MSEC) {
            lucf->timeout = 0;
        }

        if (lucf->log_level == NGX_CONF_UNSET_UINT) {
            lucf->log_level = NGX_LOG_NOTICE;
        }
    }

    return NGX_CONF_OK;
}


static void ngx_stream_limit_ups_zone_expire(ngx_shm_zone_t *shm_zone)
{
    ngx_queue_t                  *q, *t;
    ngx_msec_int_t                ms;
    ngx_slab_pool_t              *shpool;
    ngx_rbtree_node_t            *node;
    ngx_stream_limit_ups_shm_t   *data;
    ngx_stream_limit_ups_zone_t  *ctx;

    shpool = (ngx_slab_pool_t *) shm_zone->shm.addr;
    ctx = shm_zone->data;

    ngx_shmtx_lock(&shpool->mutex);

    for (q = ngx_queue_head(ctx->queue), t = NULL;
         q != ctx->queue;
         q = t)
    {
        data = ngx_queue_data(q, ngx_stream_limit_ups_shm_t, queue);

        ms = (ngx_msec_int_t) ngx_current_msec - data->last;
        ms = ngx_abs(ms);

        t = ngx_queue_next(q);

        /* remove nodes not used for at least one hour */

        if (ms >= 3600000) {

            ngx_queue_remove(q);

            node = (ngx_rbtree_node_t *) ((u_char *) data
                 - offsetof(ngx_stream_limit_ups_node_t, counter)
                 - offsetof(ngx_rbtree_node_t, color));

            ngx_rbtree_delete(ctx->rbtree, node);

            ngx_slab_free_locked(shpool, node);
        }
    }

    ngx_shmtx_unlock(&shpool->mutex);
}


static ngx_int_t
ngx_stream_limit_ups_init_zone(ngx_shm_zone_t *shm_zone, void *data)
{
    ngx_stream_limit_ups_zone_t  *octx = data;

    ngx_slab_pool_t              *shpool;
    ngx_stream_limit_ups_zone_t  *ctx;

    ctx = shm_zone->data;

    if (octx) {
        ctx->rbtree = octx->rbtree;
        ctx->queue = octx->queue;

        ngx_stream_limit_ups_zone_expire(shm_zone);

        return NGX_OK;
    }

    shpool = (ngx_slab_pool_t *) shm_zone->shm.addr;

    ctx->rbtree = ngx_slab_alloc(shpool, sizeof(ngx_rbtree_t));
    if (ctx->rbtree == NULL) {
        return NGX_ERROR;
    }

    ctx->queue = ngx_slab_alloc(shpool, sizeof(ngx_queue_t));
    if (ctx->queue == NULL) {
        return NGX_ERROR;
    }

    ngx_rbtree_init(ctx->rbtree, &ngx_stream_limit_ups_sentinel,
                    ngx_stream_limit_ups_rbtree_insert_value);

    ngx_queue_init(ctx->queue);

    return NGX_OK;
}


static ngx_rbtree_node_t *
ngx_stream_limit_ups_rbtree_lookup(ngx_rbtree_t *rbtree,
    unsigned long addr, unsigned short port)
{
    ngx_rbtree_node_t            *node, *sentinel;
    ngx_stream_limit_ups_node_t  *cnode;

    node = rbtree->root;
    sentinel = rbtree->sentinel;

    while (node != sentinel) {
        if (addr < node->key) {
            node = node->left;
            continue;
        }

        if (addr > node->key) {
            node = node->right;
            continue;
        }

        do {
            cnode = (ngx_stream_limit_ups_node_t *) &node->color;

            if (port == cnode->port) {
                return node;
            }

            node = port < cnode->port ? node->left : node->right;
        } while (node != sentinel && addr == node->key);
    }

    return NULL;
}


static void
ngx_stream_limit_ups_rbtree_insert_value(ngx_rbtree_node_t *temp,
    ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel)
{
    ngx_rbtree_node_t           **p;
    ngx_stream_limit_ups_node_t  *cn, *cnt;

    for ( ;; ) {

        if (node->key < temp->key) {
            p = &temp->left;
        } else if (node->key > temp->key) {
            p = &temp->right;
        } else {
            cn = (ngx_stream_limit_ups_node_t *) &node->color;
            cnt = (ngx_stream_limit_ups_node_t *) &temp->color;

            p = cn->port < cnt->port ? &temp->left : &temp->right;
        }

        if (*p == sentinel) {
            break;
        }

        temp = *p;
    }

    *p = node;
    node->parent = temp;
    node->left = sentinel;
    node->right = sentinel;
    ngx_rbt_red(node);
}


#if (NGX_HTTP_SSL)

static ngx_int_t
ngx_stream_limit_ups_set_peer_session(ngx_peer_connection_t *pc,
    void *data)
{
    ngx_stream_limit_ups_ctx_t   *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, pc->log, 0,
                   "limit upstream: set ssl session");

    ctx = (ngx_stream_limit_ups_ctx_t *) data;

    if (ctx->set_session) {
        return ctx->set_session(pc, ctx->data);
    }

    return NGX_OK;
}


static void
ngx_stream_limit_ups_save_peer_session(ngx_peer_connection_t *pc,
    void *data)
{
    ngx_stream_limit_ups_ctx_t   *ctx;

    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, pc->log, 0,
                   "limit upstream: save session");

    ctx = (ngx_stream_limit_ups_ctx_t *) data;

    if (ctx->save_session) {
        ctx->save_session(pc, ctx->data);
    }
}

#endif
