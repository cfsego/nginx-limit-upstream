/*
 * Author: weiyue
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>


static ngx_rbtree_t                  ngx_http_limit_upstream_loc_rbtree;
static ngx_rbtree_node_t             ngx_http_limit_upstream_sentinel;


typedef struct {
    ngx_uint_t                       limit_conn;
    ngx_uint_t                       backlog;
    ngx_msec_t                       timeout;
    ngx_shm_zone_t                  *shm_zone;

    ngx_http_upstream_init_peer_pt   init;

    ngx_uint_t                       log_level;
} ngx_http_limit_upstream_conf_t;


typedef struct {
    ngx_rbtree_t                    *rbtree;
    ngx_queue_t                     *queue;
} ngx_http_limit_upstream_zone_t;


typedef struct {
    u_char                           color;
    unsigned short                   port;
    ngx_uint_t                       counter;
} ngx_http_limit_upstream_node_t;


typedef struct {
    volatile ngx_uint_t              counter;
    ngx_msec_t                       last;
    ngx_queue_t                      queue;
} ngx_http_limit_upstream_shm_t;


typedef struct {
    ngx_uint_t                       counter;
    ngx_uint_t                       qlen;
    ngx_uint_t                       work;
    ngx_queue_t                      wait;
} ngx_http_limit_upstream_loc_t;


typedef struct {
    ngx_http_request_t              *r;
    ngx_http_limit_upstream_conf_t  *lucf;
    ngx_http_limit_upstream_loc_t   *lnode;
    void                            *data;
    void                            *wait;

    unsigned                         in_proc:1;
    unsigned                         cln:1;

    ngx_event_get_peer_pt            get;
    ngx_event_free_peer_pt           free;

    struct sockaddr                 *sockaddr;
} ngx_http_limit_upstream_ctx_t;


typedef struct {
    ngx_queue_t                      queue;
    ngx_http_limit_upstream_ctx_t   *ctx;
    ngx_http_event_handler_pt        read_event_handler;
    ngx_http_event_handler_pt        write_event_handler;
    unsigned                         r_timer_set:1;
    unsigned                         w_timer_set:1;
} ngx_http_limit_upstream_wait_t;


static ngx_conf_enum_t ngx_http_limit_upstream_log_levels[] = {
    { ngx_string("info"), NGX_LOG_INFO },
    { ngx_string("notice"), NGX_LOG_NOTICE },
    { ngx_string("warn"), NGX_LOG_WARN },
    { ngx_string("error"), NGX_LOG_ERR },
    { ngx_null_string, 0 }
};


static void ngx_http_limit_upstream_timeout(ngx_http_request_t *r);
static void ngx_http_limit_upstream_cleanup(void *data);
static void ngx_http_limit_upstream_free_peer(ngx_peer_connection_t *pc,
    void *data, ngx_uint_t state);
static ngx_int_t ngx_http_limit_upstream_get_peer(ngx_peer_connection_t *pc,
    void *data);
static ngx_int_t ngx_http_limit_upstream_init_peer(ngx_http_request_t *r,
    ngx_http_upstream_srv_conf_t *us);

static void *ngx_http_limit_upstream_create_srv_conf(ngx_conf_t *cf);
static char *ngx_http_limit_upstream_merge_srv_conf(ngx_conf_t *cf, void *parent,
    void *child);
static char *ngx_http_limit_upstream_zone(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static char *ngx_http_limit_upstream_conn(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf);
static ngx_int_t ngx_http_limit_upstream_init(ngx_conf_t *cf);

static ngx_int_t ngx_http_limit_upstream_init_zone(ngx_shm_zone_t *shm_zone,
    void *data);
static void ngx_http_limit_upstream_zone_expire(ngx_shm_zone_t *shm_zone);

static void
    ngx_http_limit_upstream_rbtree_insert_value(ngx_rbtree_node_t *temp,
    ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel);
static ngx_rbtree_node_t *
    ngx_http_limit_upstream_rbtree_lookup(ngx_rbtree_t *rbtree,
    unsigned long addr, unsigned short port);


static ngx_command_t ngx_http_limit_upstream_commands[] = {

    { ngx_string("limit_upstream_zone"),
      NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE2,
      ngx_http_limit_upstream_zone,
      0,
      0,
      NULL },

    { ngx_string("limit_upstream_conn"),
      NGX_HTTP_UPS_CONF|NGX_CONF_TAKE1234,
      ngx_http_limit_upstream_conn,
      NGX_HTTP_SRV_CONF_OFFSET,
      0,
      NULL },

    { ngx_string("limit_upstream_log_level"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_UPS_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_enum_slot,
      NGX_HTTP_SRV_CONF_OFFSET,
      offsetof(ngx_http_limit_upstream_conf_t, log_level),
      &ngx_http_limit_upstream_log_levels },

      ngx_null_command
};


static ngx_http_module_t ngx_http_limit_upstream_module_ctx = {
    NULL,
    ngx_http_limit_upstream_init,

    NULL,
    NULL,

    ngx_http_limit_upstream_create_srv_conf,
    ngx_http_limit_upstream_merge_srv_conf,

    NULL,
    NULL
};


ngx_module_t ngx_http_limit_upstream_module = {
    NGX_MODULE_V1,
    &ngx_http_limit_upstream_module_ctx,
    ngx_http_limit_upstream_commands,
    NGX_HTTP_MODULE,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NGX_MODULE_V1_PADDING
};


static void
ngx_http_limit_upstream_timeout(ngx_http_request_t *r)
{
    ngx_event_t                     *wev;
    ngx_http_upstream_t             *u;
    ngx_http_limit_upstream_wait_t  *w;
    ngx_http_limit_upstream_loc_t   *l;

    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "limit upstream: into timeout");

    wev = r->connection->write;

    if (!wev->timedout) {
        ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                       "limit upstream: wev ignored");
        return;
    }

    ngx_log_error(NGX_LOG_WARN, r->connection->log, 0,
                  "limit upstream: request[%p] is timeout", r);

    wev->timedout = 0;

    u = r->upstream;

    if (u->cleanup) {
        *u->cleanup = NULL;
        u->cleanup = NULL;
    }

    w = ((ngx_http_limit_upstream_ctx_t *) u->peer.data)->wait;
    ngx_queue_remove(&w->queue);

    l = ((ngx_http_limit_upstream_ctx_t *) u->peer.data)->lnode;
    l->qlen--;

    ngx_http_finalize_request(r, NGX_HTTP_REQUEST_TIME_OUT);
}


static void
ngx_http_limit_upstream_cleanup(void *data)
{
    ngx_queue_t                     *q;
    ngx_rbtree_node_t               *node_s, *node_l;
    struct sockaddr_in              *sin;
#if NGX_DEBUG
    ngx_peer_connection_t           *pc;
#endif
    ngx_http_limit_upstream_ctx_t   *ctx;
    ngx_http_limit_upstream_loc_t   *lnode;
    ngx_http_limit_upstream_shm_t   *snode;
    ngx_http_limit_upstream_node_t  *cnode;
    ngx_http_limit_upstream_wait_t  *wnode;
    ngx_http_limit_upstream_zone_t  *shmctx;

    ctx = (ngx_http_limit_upstream_ctx_t *) data;
    shmctx = ctx->lucf->shm_zone->data;

    if (!ctx->in_proc) {
        return;
    }

#if NGX_DEBUG
    pc = &ctx->r->upstream->peer;
#endif
    sin = (struct sockaddr_in *) ctx->sockaddr;

    node_s = ngx_http_limit_upstream_rbtree_lookup(shmctx->rbtree,
                                                   sin->sin_addr.s_addr,
                                                   sin->sin_port);

    node_l = ngx_http_limit_upstream_rbtree_lookup(
                    &ngx_http_limit_upstream_loc_rbtree,
                    sin->sin_addr.s_addr,
                    sin->sin_port);

    if (node_l == NULL && node_s == NULL) {
        return;
    }

#if 1

    if ((node_l == NULL && node_s) || (node_s == NULL && node_l)) {
        ngx_log_error(NGX_LOG_EMERG, ctx->r->connection->log, 0,
                      "limit upstream: only local or shm node exists");
        return;
    }

#endif

    cnode = (ngx_http_limit_upstream_node_t *) &node_s->color;
    snode = (ngx_http_limit_upstream_shm_t *) &cnode->counter;

    cnode = (ngx_http_limit_upstream_node_t *) &node_l->color;
    lnode = (ngx_http_limit_upstream_loc_t *) &cnode->counter;

    lnode->work--;

    if ((snode->counter > ctx->lucf->limit_conn && lnode->work)
        || lnode->qlen == 0)
    {
        ngx_log_debug4(NGX_LOG_DEBUG_HTTP, ctx->r->connection->log, 0,
                       "limit upstream: will not resume request for %V "
                       "(counter: %d, active: %d, wait queue: %d)",
                       pc->name, snode->counter, lnode->work, lnode->qlen);

        snode->counter--;
        lnode->counter--;
        return;
    }

    do {
        q = ngx_queue_last(&lnode->wait);
        wnode = ngx_queue_data(q, ngx_http_limit_upstream_wait_t, queue);

        ngx_log_debug5(NGX_LOG_DEBUG_HTTP, ctx->r->connection->log, 0,
                       "limit upstream: remove queue node: %p for %V "
                       "(counter: %d, active: %d, wait queue: %d)",
                       q, pc->name, snode->counter,
                       lnode->work, lnode->qlen);

        ngx_queue_remove(q);
        lnode->qlen--;

        /* resume a request in wait queue */

        ctx = wnode->ctx;

        ngx_log_error(ctx->lucf->log_level, ctx->r->connection->log, 0,
                      "limit upstream: request[%p] is resumed", ctx->r);

        ctx->r->read_event_handler = wnode->read_event_handler;
        ctx->r->write_event_handler = wnode->write_event_handler;

        if (wnode->r_timer_set) {
            if (ctx->r->connection->read->timedout) {
                ctx->r->read_event_handler(ctx->r);
                return;
            }

            if (ngx_handle_read_event(ctx->r->connection->read, 0)
                != NGX_OK)
            {
                ngx_http_finalize_request(ctx->r,
                                          NGX_HTTP_INTERNAL_SERVER_ERROR);
                return;
            }

            ngx_add_timer(ctx->r->connection->read,
                          ctx->r->connection->read->timer.key);
        }

        ngx_del_timer(ctx->r->connection->write);

        if (wnode->w_timer_set) {
            if (ctx->r->connection->write->timedout) {
                ctx->r->write_event_handler(ctx->r);
                return;
            }

            if (ngx_handle_write_event(ctx->r->connection->write, 0)
                != NGX_OK)
            {
                ngx_http_finalize_request(ctx->r,
                                          NGX_HTTP_INTERNAL_SERVER_ERROR);
                return;
            }

            ngx_add_timer(ctx->r->connection->write,
                          ctx->r->connection->write->timer.key);
        }

        lnode->work++;

        ngx_http_upstream_connect(ctx->r, ctx->r->upstream);
    } while (ngx_atomic_fetch_add(&snode->counter, 1)
             < ctx->lucf->limit_conn && lnode->qlen);

    snode->counter--;
}


static void
ngx_http_limit_upstream_free_peer(ngx_peer_connection_t *pc, void *data,
    ngx_uint_t state)
{
    ngx_http_limit_upstream_ctx_t   *ctx;

    ctx = (ngx_http_limit_upstream_ctx_t *) data;

    ctx->free(pc, ctx->data, state);
}


static ngx_int_t
ngx_http_limit_upstream_get_peer(ngx_peer_connection_t *pc, void *data)
{
    size_t                           n;
    ngx_int_t                        rc;
#if (NGX_DEBUG)
    ngx_uint_t                       active;
#endif
    ngx_slab_pool_t                 *shpool;
    ngx_rbtree_node_t               *node_s, *node_l;
    struct sockaddr_in              *sin;
    ngx_http_cleanup_t              *cln;
    ngx_http_limit_upstream_ctx_t   *ctx;
    ngx_http_limit_upstream_loc_t   *lnode;
    ngx_http_limit_upstream_shm_t   *snode;
    ngx_http_limit_upstream_node_t  *cnode;
    ngx_http_limit_upstream_wait_t  *wnode;
    ngx_http_limit_upstream_zone_t  *shmctx;
    ngx_rbtree_key_int_t             t;

    ctx = (ngx_http_limit_upstream_ctx_t *) data;
    shpool = (ngx_slab_pool_t *) ctx->lucf->shm_zone->shm.addr;
    shmctx = ctx->lucf->shm_zone->data;

    if (ctx->r->upstream->blocked) {
        rc = NGX_OK;
        ctx->r->upstream->blocked = 0;

        goto set_and_ret;

    } else {

        if (ctx->in_proc) {

            sin = (struct sockaddr_in *) ctx->sockaddr;
            node_s = ngx_http_limit_upstream_rbtree_lookup(shmctx->rbtree,
                                                           sin->sin_addr.s_addr,
                                                           sin->sin_port);

            node_l = ngx_http_limit_upstream_rbtree_lookup(
                                            &ngx_http_limit_upstream_loc_rbtree,
                                            sin->sin_addr.s_addr,
                                            sin->sin_port);

            if (node_l && node_s) {
                cnode = (ngx_http_limit_upstream_node_t *) &node_s->color;
                snode = (ngx_http_limit_upstream_shm_t *) &cnode->counter;

                cnode = (ngx_http_limit_upstream_node_t *) &node_l->color;
                lnode = (ngx_http_limit_upstream_loc_t *) &cnode->counter;

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

    ctx->sockaddr = ngx_palloc(ctx->r->pool, pc->socklen);
    if (ctx->sockaddr == NULL) {
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }

    ngx_memcpy(ctx->sockaddr, pc->sockaddr, pc->socklen);

    sin = (struct sockaddr_in *) ctx->sockaddr;

    node_s = ngx_http_limit_upstream_rbtree_lookup(shmctx->rbtree,
                                                   sin->sin_addr.s_addr,
                                                   sin->sin_port);

    node_l = ngx_http_limit_upstream_rbtree_lookup(
                    &ngx_http_limit_upstream_loc_rbtree,
                    sin->sin_addr.s_addr,
                    sin->sin_port);

    if (node_l == NULL) {
        n = offsetof(ngx_rbtree_node_t, color)
          + offsetof(ngx_http_limit_upstream_node_t, counter)
          + sizeof(ngx_http_limit_upstream_loc_t);

        node_l = ngx_pcalloc(ngx_cycle->pool, n);
        if (node_l == NULL) {
            return NGX_ERROR;
        }

        cnode = (ngx_http_limit_upstream_node_t *) &node_l->color;
        lnode = (ngx_http_limit_upstream_loc_t *) &cnode->counter;

        node_l->key = sin->sin_addr.s_addr;
        cnode->port = sin->sin_port;

        ngx_queue_init(&lnode->wait);

        ngx_rbtree_insert(&ngx_http_limit_upstream_loc_rbtree, node_l);
    } else {
        cnode = (ngx_http_limit_upstream_node_t *) &node_l->color;
        lnode = (ngx_http_limit_upstream_loc_t *) &cnode->counter;
    }

    if (node_s == NULL) {

        ngx_shmtx_lock(&shpool->mutex);

        n = offsetof(ngx_rbtree_node_t, color)
          + offsetof(ngx_http_limit_upstream_node_t, counter)
          + sizeof(ngx_http_limit_upstream_shm_t);

        node_s = ngx_slab_alloc_locked(shpool, n);
        if (node_s == NULL) {
            return NGX_ERROR;
        }

        cnode = (ngx_http_limit_upstream_node_t *) &node_s->color;
        snode = (ngx_http_limit_upstream_shm_t *) &cnode->counter;

        node_s->key = sin->sin_addr.s_addr;
        cnode->port = sin->sin_port;

        ngx_queue_insert_head(shmctx->queue, &snode->queue);

        ngx_rbtree_insert(shmctx->rbtree, node_s);

        ngx_shmtx_unlock(&shpool->mutex);
    } else {
        cnode = (ngx_http_limit_upstream_node_t *) &node_s->color;
        snode = (ngx_http_limit_upstream_shm_t *) &cnode->counter;
    }

    snode->last = ngx_current_msec;

    if (rc == NGX_DONE) {
        snode->counter++;
        lnode->counter++;
        lnode->work++;
        goto set_and_ret;
    }

    ngx_log_debug4(NGX_LOG_DEBUG_HTTP, ctx->r->connection->log, 0,
                   "limit upstream: status for %V "
                   "(counter: %d, active: %d, wait queue: %d)",
                   pc->name, snode->counter, lnode->work, lnode->qlen);

#if (NGX_DEBUG)
    active = ngx_atomic_fetch_add(&snode->counter, 1);

    if (active >= ctx->lucf->limit_conn && lnode->work) {
#else
    if (ngx_atomic_fetch_add(&snode->counter, 1) >= ctx->lucf->limit_conn
        && lnode->work)
    {
#endif

        snode->counter--;

        if (lnode->qlen >= ctx->lucf->backlog) {
            ngx_log_error(NGX_LOG_WARN, ctx->r->connection->log, 0,
                          "limit_upstream: request[%p] is dropped", ctx->r);
            return NGX_DECLINED;
        }

        wnode = ngx_pcalloc(ctx->r->pool,
                            sizeof(ngx_http_limit_upstream_wait_t));
        if (wnode == NULL) {
            return NGX_ERROR;
        }

        ngx_queue_insert_head(&lnode->wait, &wnode->queue);
        lnode->qlen++;

        ngx_log_debug1(NGX_LOG_DEBUG_HTTP, ctx->r->connection->log, 0,
                       "limit upstream: add queue node: %p", &wnode->queue);

        ngx_log_error(ctx->lucf->log_level, ctx->r->connection->log, 0,
                      "limit upstream: request[%p] is blocked", ctx->r);

        wnode->ctx = ctx;
        ctx->wait = wnode;
        ctx->lnode = lnode;

        wnode->read_event_handler = ctx->r->read_event_handler;
        wnode->r_timer_set = ctx->r->connection->read->timer_set;
        ctx->r->read_event_handler = ngx_http_block_reading;
        if (wnode->r_timer_set) {
            ngx_del_timer(ctx->r->connection->read);

            t = ctx->r->connection->read->timer.key;
            t -= (ngx_rbtree_key_int_t) ngx_current_msec;

            if (t > 0) {
                ctx->r->connection->read->timer.key = t;
            } else {
                ctx->r->connection->read->timedout = 1;
            }
        }

        wnode->write_event_handler = ctx->r->write_event_handler;
        wnode->w_timer_set = ctx->r->connection->write->timer_set;
        ctx->r->write_event_handler = ngx_http_limit_upstream_timeout;
        if (wnode->w_timer_set) {
            ngx_del_timer(ctx->r->connection->write);

            t = ctx->r->connection->write->timer.key;
            t -= (ngx_rbtree_key_int_t) ngx_current_msec;

            if (t > 0) {
                ctx->r->connection->write->timer.key = t;
            } else {
                ctx->r->connection->write->timedout = 1;
            }
        }
        ngx_add_timer(ctx->r->connection->write, ctx->lucf->timeout);

        ctx->r->upstream->blocked = 1;

        return NGX_BLOCK;
    }

#if (NGX_DEBUG)

    if (active >= ctx->lucf->limit_conn) {
        ngx_log_debug0(NGX_LOG_DEBUG_HTTP, ctx->r->connection->log, 0,
                       "force continue request");
    }

#endif

    lnode->counter++;
    lnode->work++;

set_and_ret:

    if (!ctx->cln) {
        cln = ngx_http_cleanup_add(ctx->r, 0);
        if (cln == NULL) {
            return NGX_HTTP_INTERNAL_SERVER_ERROR;
        }

        cln->handler = ngx_http_limit_upstream_cleanup;
        cln->data = ctx;

        ctx->cln = 1;
    }

    ctx->in_proc = 1;

    return rc;
}


static ngx_int_t
ngx_http_limit_upstream_init_peer(ngx_http_request_t *r,
    ngx_http_upstream_srv_conf_t *us)
{
    ngx_int_t                        rc;
    ngx_http_limit_upstream_ctx_t   *ctx;
    ngx_http_limit_upstream_conf_t  *lucf;

    ctx = ngx_pcalloc(r->pool, sizeof(ngx_http_limit_upstream_ctx_t));
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    lucf = us->srv_conf[ngx_http_limit_upstream_module.ctx_index];

    rc = lucf->init(r, us);

    if (rc != NGX_OK) {
        return rc;
    }

    ctx->data = r->upstream->peer.data;
    r->upstream->peer.data = ctx;

    ctx->get = r->upstream->peer.get;
    r->upstream->peer.get = ngx_http_limit_upstream_get_peer;

    ctx->free = r->upstream->peer.free;
    r->upstream->peer.free = ngx_http_limit_upstream_free_peer;

    ctx->r = r;
    ctx->lucf = lucf;
    r->upstream->blocked = 0;

    return NGX_OK;
}


static ngx_int_t
ngx_http_limit_upstream_init(ngx_conf_t *cf)
{
    ngx_uint_t                       i;
    ngx_http_limit_upstream_conf_t  *lucf, *mlucf;
    ngx_http_upstream_srv_conf_t   **uscfp;
    ngx_http_upstream_main_conf_t   *umcf;

    umcf = ngx_http_conf_get_module_main_conf(cf,
                                              ngx_http_upstream_module);
    mlucf = ngx_http_conf_get_module_srv_conf(cf,
                                           ngx_http_limit_upstream_module);
    uscfp = umcf->upstreams.elts;

    ngx_rbtree_init(&ngx_http_limit_upstream_loc_rbtree,
                    &ngx_http_limit_upstream_sentinel,
                    ngx_http_limit_upstream_rbtree_insert_value);

    for (i = 0; i < umcf->upstreams.nelts; i++) {

        if (uscfp[i]->srv_conf) {
            lucf = uscfp[i]->
                        srv_conf[ngx_http_limit_upstream_module.ctx_index];

            if (lucf->limit_conn != NGX_CONF_UNSET_UINT) {
                lucf->init = uscfp[i]->peer.init;
                uscfp[i]->peer.init = ngx_http_limit_upstream_init_peer;

                if (lucf->backlog == NGX_CONF_UNSET_UINT) {
                    lucf->backlog = 1000;
                }

                if (lucf->timeout == NGX_CONF_UNSET_MSEC) {
                    lucf->timeout = 1000;
                }

                if (ngx_http_limit_upstream_merge_srv_conf(cf, mlucf, lucf)
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
ngx_http_limit_upstream_create_srv_conf(ngx_conf_t *cf)
{
    ngx_http_limit_upstream_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_limit_upstream_conf_t));
    if (conf == NULL) {
        return NULL;
    }

    /*
     * set by ngx_pcalloc();
     *
     * conf->init = NULL;
     * conf->shm_zone = NULL;
     */

    conf->limit_conn = NGX_CONF_UNSET_UINT;
    conf->backlog = NGX_CONF_UNSET_UINT;
    conf->timeout = NGX_CONF_UNSET_MSEC;
    conf->log_level = NGX_CONF_UNSET_UINT;

    return conf;
}


static char *
ngx_http_limit_upstream_merge_srv_conf(ngx_conf_t *cf, void *parent,
    void *child)
{
    ngx_http_limit_upstream_conf_t  *conf = child;
    ngx_http_limit_upstream_conf_t  *prev = parent;

    ngx_conf_merge_uint_value(conf->log_level,
                              prev->log_level, NGX_LOG_NOTICE);

    return NGX_CONF_OK;
}


static char *
ngx_http_limit_upstream_zone(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ssize_t                          n;
    ngx_str_t                       *value;
    ngx_shm_zone_t                  *shm_zone;
    ngx_http_limit_upstream_zone_t  *ctx;

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
                                     &ngx_http_limit_upstream_module);
    if (shm_zone == NULL) {
        return NGX_CONF_ERROR;
    }

    if (shm_zone->data) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "limit_upstream_zone \"%V\" is duplicate",
                           &value[1]);
        return NGX_CONF_ERROR;
    }

    ctx = ngx_pcalloc(cf->pool, sizeof(ngx_http_limit_upstream_zone_t));
    if (ctx == NULL) {
        return NGX_CONF_ERROR;
    }

    shm_zone->data = ctx;
    shm_zone->init = ngx_http_limit_upstream_init_zone;

    return NGX_CONF_OK;
}


static char *
ngx_http_limit_upstream_conn(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_str_t                       *value, s;
    ngx_uint_t                       i;

    ngx_http_limit_upstream_conf_t  *lucf = conf;

    if (lucf->shm_zone) {
        return "is duplicate";
    }

    value = cf->args->elts;

    for (i = 1; i < cf->args->nelts; i++) {

        if (ngx_strncmp(value[i].data, "zone=", 5) == 0) {

            s.len = value[i].len - 5;
            s.data = value[i].data + 5;

            lucf->shm_zone = ngx_shared_memory_add(cf, &s, 0,
                                              &ngx_http_limit_upstream_module);
            if (lucf->shm_zone == NULL) {
                return NGX_CONF_ERROR;
            }

            continue;
        }

        if (ngx_strncmp(value[i].data, "limit=", 6) == 0) {
            lucf->limit_conn = ngx_atoi(value[i].data + 6, value[i].len - 6);
            if (lucf->limit_conn <= 0) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                                   "invalid limit \"%V\"", &value[i]);
                return NGX_CONF_ERROR;
            }

            continue;
        }

        if (ngx_strncmp(value[i].data, "backlog=", 8) == 0) {

            lucf->backlog = ngx_atoi(value[i].data + 8, value[i].len - 8);
            if (lucf->backlog <= 0) {
                ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                                   "invalid backlog \"%V\"", &value[i]);
                return NGX_CONF_ERROR;
            }

            continue;
        }

        if (ngx_strncmp(value[i].data, "nodelay", 7) == 0) {
            lucf->backlog = 0;
            continue;
        }

        if (ngx_strncmp(value[i].data, "timeout=", 8) == 0) {

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

    return NGX_CONF_OK;
}


static void ngx_http_limit_upstream_zone_expire(ngx_shm_zone_t *shm_zone)
{
    ngx_queue_t                     *q, *t;
    ngx_msec_int_t                   ms;
    ngx_slab_pool_t                 *shpool;
    ngx_rbtree_node_t               *node;
    ngx_http_limit_upstream_shm_t   *data;
    ngx_http_limit_upstream_zone_t  *ctx;

    shpool = (ngx_slab_pool_t *) shm_zone->shm.addr;
    ctx = shm_zone->data;

    ngx_shmtx_lock(&shpool->mutex);

    for (q = ngx_queue_head(ctx->queue), t = NULL;
         q != ctx->queue;
         q = t)
    {
        data = ngx_queue_data(q, ngx_http_limit_upstream_shm_t, queue);

        ms = (ngx_msec_int_t) ngx_current_msec - data->last;
        ms = ngx_abs(ms);

        t = ngx_queue_next(q);

        /* remove nodes not used for at least one hour */

        if (ms >= 3600000) {

            ngx_queue_remove(q);

            node = (ngx_rbtree_node_t *) ((u_char *) data
                 - offsetof(ngx_http_limit_upstream_node_t, counter)
                 - offsetof(ngx_rbtree_node_t, color));

            ngx_rbtree_delete(ctx->rbtree, node);

            ngx_slab_free_locked(shpool, node);
        }
    }

    ngx_shmtx_unlock(&shpool->mutex);
}


static ngx_int_t
ngx_http_limit_upstream_init_zone(ngx_shm_zone_t *shm_zone, void *data)
{
    ngx_http_limit_upstream_zone_t  *octx = data;

    ngx_slab_pool_t                 *shpool;
    ngx_http_limit_upstream_zone_t  *ctx;

    ctx = shm_zone->data;

    if (octx) {
        ctx->rbtree = octx->rbtree;
        ctx->queue = octx->queue;

        ngx_http_limit_upstream_zone_expire(shm_zone);

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

    ngx_rbtree_init(ctx->rbtree, &ngx_http_limit_upstream_sentinel,
                    ngx_http_limit_upstream_rbtree_insert_value);

    ngx_queue_init(ctx->queue);

    return NGX_OK;
}


static ngx_rbtree_node_t *
ngx_http_limit_upstream_rbtree_lookup(ngx_rbtree_t *rbtree, unsigned long addr,
    unsigned short port)
{
    ngx_rbtree_node_t               *node, *sentinel;
    ngx_http_limit_upstream_node_t  *cnode;

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
            cnode = (ngx_http_limit_upstream_node_t *) &node->color;

            if (port == cnode->port) {
                return node;
            }

            node = port < cnode->port ? node->left : node->right;
        } while (node != sentinel && addr == node->key);
    }

    return NULL;
}


static void
ngx_http_limit_upstream_rbtree_insert_value(ngx_rbtree_node_t *temp,
    ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel)
{
    ngx_rbtree_node_t              **p;
    ngx_http_limit_upstream_node_t  *cn, *cnt;

    for ( ;; ) {

        if (node->key < temp->key) {
            p = &temp->left;
        } else if (node->key > temp->key) {
            p = &temp->right;
        } else {
            cn = (ngx_http_limit_upstream_node_t *) &node->color;
            cnt = (ngx_http_limit_upstream_node_t *) &temp->color;

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
