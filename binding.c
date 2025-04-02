#include "uv.h"
#include <assert.h>
#include <bare.h>
#include <js.h>
#include <utf.h>
#include <stdlib.h>
#include <string.h>

#include <duckdb.h>

typedef struct {
    duckdb_database handle;
    js_env_t *env;
    duckdb_connection connection;
} bare_duckdb_t;

typedef struct {
  uv_work_t handle;
  bare_duckdb_t *db;
  js_deferred_t *deferred;
  char *error;
} bare_duckdb_connect_t;

typedef struct {
  uv_work_t handle;
  bare_duckdb_t *db;
  js_deferred_t *deferred;
} bare_duckdb_disconnect_t;

typedef struct {
    uv_work_t handle;
    bare_duckdb_t *db;
    js_deferred_t *deferred;
    char *query;
    duckdb_result *result;
    char *error;
} bare_duckdb_query_t;

static js_value_t *
bare_duckdb_open(js_env_t *env, js_callback_info_t *info) {
    int err;
    size_t argc = 1;
    js_value_t *argv[1];

    err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
    assert(err == 0);

    size_t path_len = 0;
    bool is_undefined;
    if (argc > 0 && js_is_undefined(env, argv[0], &is_undefined)) {
        err = js_get_value_string_utf8(env, argv[0], NULL, 0, &path_len);
        assert(err == 0);
    }

    char *path = NULL;
    if (path_len > 0) {
        path = malloc(path_len + 1);
        if (path == NULL) {
            js_throw_error(env, "Error", "Out of memory");
            return NULL;
        }
        err = js_get_value_string_utf8(env, argv[0], (utf8_t *) path, path_len + 1, NULL);
        assert(err == 0);
    }


    bare_duckdb_t *db = malloc(sizeof(bare_duckdb_t));
    if (db == NULL) {
        if (path) free(path);
        js_throw_error(env, "Error", "Out of Memory");
        return NULL;
    }

    db->env = env;
    db->connection = NULL;

    duckdb_config config;
    if (duckdb_create_config(&config) != DuckDBSuccess) {
        if (path) free(path);
        free(db);
        js_throw_error(env, "Error", "Could not create config");
        return NULL;
    }

    // TODO: pass config from js

    char *open_error;
    if (duckdb_open_ext(path, &db->handle, config, &open_error) != DuckDBSuccess) {
        js_value_t *error;
        err = js_create_string_utf8(env, (utf8_t *) open_error, -1, &error);
        assert(err == 0);
        if (open_error) duckdb_free(open_error);
        js_throw(env, error);

        if (path) free(path);
        free(db);
        duckdb_destroy_config(&config);
        return NULL;
    }

    duckdb_destroy_config(&config);
    if (path) free(path);

    js_value_t *handle;
    err = js_create_external(env, db, NULL, NULL, &handle);
    assert(err == 0);
    return handle;
}

static js_value_t *
bare_duckdb_close(js_env_t *env, js_callback_info_t *info) {
    int err;
    size_t argc = 1;
    js_value_t *argv[1];

    err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
    assert(err == 0);

    if (argc != 1) {
        js_throw_error(env, "Error", "Expected exactly one argument: database");
        return NULL;
    }

    bare_duckdb_t *db;
    err = js_get_value_external(env, argv[0], (void **) &db);
    assert(err == 0);

    if (db->connection) {
        duckdb_disconnect(&db->connection);
        db->connection = NULL;
    }

    if (db->handle) {
        duckdb_close(&db->handle);
        db->handle = NULL;
    }

    free(db);

    js_value_t *undefined;
    err = js_get_undefined(env, &undefined);
    assert(err == 0);
    return undefined;
}

static void
bare_duckdb__on_after_connect(uv_work_t *handle, int status) {
    bare_duckdb_connect_t *req  = (bare_duckdb_connect_t *) handle->data;
    js_env_t *env = req->db->env;
    js_handle_scope_t *scope;
    int err;

    err = js_open_handle_scope(env, &scope);
    assert(err == 0);

    js_value_t *result;

    if (status < 0) {
        // libuv error
        err = js_create_string_utf8(env, (const utf8_t *) uv_strerror(status), -1, &result);
        assert(err == 0);
    } else if (req->error != NULL) {
        // duckdb error
        err = js_create_string_utf8(env, (const utf8_t *) req->error, - 1, &result);
        assert(err == 0);
    } else {
        // we did it
        js_value_t *db_handle;
        err = js_create_external(env, (void*) req->db, NULL, NULL, &db_handle);
        assert(err == 0);
        result = db_handle;
    }

    if (status < 0 || req->error != NULL) {
        js_reject_deferred(env, req->deferred, result);
    } else {
        js_resolve_deferred(env, req->deferred, result);
    }

    err = js_close_handle_scope(env, scope);
    assert(err == 0);

    if (req->error) free(req->error);
    free(req);
}

static void
bare_duckdb__on_before_connect(uv_work_t *handle) {
    int err;

    bare_duckdb_connect_t *req = (bare_duckdb_connect_t *) handle->data;
    if (duckdb_connect(req->db->handle, &req->db->connection) != DuckDBSuccess) {
        req->error = strdup("Connection failed");
    }
}

static js_value_t *
bare_duckdb_connect(js_env_t *env, js_callback_info_t *info) {
    int err;
    size_t argc = 1;
    js_value_t *argv[1];

    err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
    assert(err == 0);

    if (argc != 1) {
        js_throw_error(env, "Error", "Expected exactly one argument: database");
        return NULL;
    }

    uv_loop_t *loop;
    err = js_get_env_loop(env, &loop);
    assert(err == 0);

    bare_duckdb_t *db;
    err = js_get_value_external(env, argv[0], (void **) &db);
    assert(err == 0);

    bare_duckdb_connect_t *req = malloc(sizeof(bare_duckdb_connect_t));
    if (!req) {
        js_throw_error(env, "Error", "Out of memory");
        return NULL;
    }

    req->db = db;
    req->handle.data = req;
    req->error = NULL;

    js_value_t *promise;
    err = js_create_promise(env, &req->deferred, &promise);
    assert(err == 0);

    err = uv_queue_work(loop, &req->handle, bare_duckdb__on_before_connect, bare_duckdb__on_after_connect);
    assert(err == 0);

    return promise;
}

static js_value_t *
bare_duckdb_disconnect(js_env_t *env, js_callback_info_t *info) {
  int err;
  size_t argc = 1;
  js_value_t *argv[1];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  if (argc != 1) {
    js_throw_error(env, "Error", "Expected exactly one argument: database object");
    return NULL;
  }

    bare_duckdb_t *db;
    err = js_get_value_external(env, argv[0], (void**) &db);
    assert(err == 0);

    if (!db) {
        js_throw_error(env, "Error", "Invalid database object");
        return NULL;
    }

    if (db->connection != NULL) {
        duckdb_disconnect(&db->connection);
        db->connection = NULL;
    }

    js_value_t *undefined;
    err = js_get_undefined(env, &undefined);
    assert(err == 0);
    return undefined;
}

static void
finalize_duckdb_result(js_env_t *env, void *data, void * hint) {
    duckdb_result *result = (duckdb_result *) data;

    if (result) {
        duckdb_destroy_result(result);
        free(result);
    }
}

static js_value_t *
bare_duckdb_result_to_js(js_env_t *env, duckdb_result *result) {
    int err;
    size_t row_count = duckdb_row_count(result);
    size_t column_count = duckdb_column_count(result);

    js_value_t *rows_array;
    err = js_create_array(env, &rows_array);
    assert(err == 0);

    for (size_t row_idx = 0; row_idx < row_count; row_idx++) {
        js_value_t *row_object;
        err = js_create_object(env, &row_object);
        assert(err == 0);

        for (size_t col_idx = 0; col_idx < column_count; col_idx++) {
            const char *column_name = duckdb_column_name(result, col_idx);
            duckdb_type column_type = duckdb_column_type(result, col_idx);

            js_value_t *cell_value;
            switch (column_type) {
                case DUCKDB_TYPE_VARCHAR: {
                    char *str_val = duckdb_value_varchar(result, row_idx, col_idx);
                    if (str_val) {
                        err = js_create_string_utf8(env, (utf8_t *)str_val, -1, &cell_value);
                        assert(err == 0);
                        duckdb_free(str_val);
                    } else {
                        err = js_get_null(env, &cell_value);
                        assert(err == 0);
                    }
                    break;
                }
                case DUCKDB_TYPE_BIGINT:
                case DUCKDB_TYPE_INTEGER:
                case DUCKDB_TYPE_SMALLINT:
                case DUCKDB_TYPE_TINYINT: {
                    int64_t int_val = duckdb_value_int64(result, row_idx, col_idx);
                    err = js_create_double(env, (double)int_val, &cell_value);
                    assert(err == 0);
                    break;
                }
                case DUCKDB_TYPE_DOUBLE:
                case DUCKDB_TYPE_FLOAT: {
                    double double_val = duckdb_value_double(result, row_idx, col_idx);
                    err = js_create_double(env, double_val, &cell_value);
                    assert(err == 0);
                    break;
                }
                case DUCKDB_TYPE_BOOLEAN: {
                    bool bool_val = duckdb_value_boolean(result, row_idx, col_idx);
                    err = js_get_boolean(env, bool_val, &cell_value);
                    assert(err == 0);
                    break;
                }
                default: {
                    char *str_val = duckdb_value_varchar(result, row_idx, col_idx);
                        if (str_val) {
                        err = js_create_string_utf8(env, (utf8_t *)str_val, -1, &cell_value);
                        assert(err == 0);
                        duckdb_free(str_val);
                    } else {
                        err = js_get_null(env, &cell_value);
                        assert(err == 0);
                    }
                    break;
                }
            }
            err = js_set_named_property(env, row_object, column_name, cell_value);
            assert(err == 0);
        }
        err = js_set_element(env, rows_array, row_idx, row_object);
        assert(err == 0);
    }

    return rows_array;
}

static void
bare_duckdb__on_after_query(uv_work_t *handle, int status) {
    bare_duckdb_query_t *req = (bare_duckdb_query_t *) handle->data;
    js_env_t *env = req->db->env;
    js_handle_scope_t *scope;
    int err;

    err = js_open_handle_scope(env, &scope);
    assert(err == 0);

    js_value_t *result_value;

    if (status < 0) {
        // libuv error
        err = js_create_string_utf8(env, (const utf8_t *) uv_strerror(status), -1, &result_value);
        assert(err == 0);
        js_reject_deferred(env, req->deferred, result_value);
    } else if (req->error) {
        // duckdb error
        err = js_create_string_utf8(env, (const utf8_t *) req->error, -1, &result_value);
        assert(err == 0);
        js_reject_deferred(env, req->deferred, result_value);
    } else {
        // we have a result
        result_value = bare_duckdb_result_to_js(env, req->result);
        js_resolve_deferred(env, req->deferred, result_value);
    }

    err = js_close_handle_scope(env, scope);
    assert(err == 0);

    if (req->error) {
        free(req->error);
    }

    if (req->query) {
        free(req->query);
    }

    free(req);
}

static void
bare_duckdb__on_before_query(uv_work_t *handle) {
    bare_duckdb_query_t *req = (bare_duckdb_query_t *) handle->data;

    req->result = malloc(sizeof(duckdb_result));
    if (req->result == NULL) {
        req->error = strdup("Out of memory");
        return;
    }

    if (duckdb_query(req->db->connection, req->query, req->result) != DuckDBSuccess) {
        req->error = strdup(duckdb_result_error(req->result));
        duckdb_destroy_result(req->result);
        free(req->result);
        req->result = NULL;
    }
}

static js_value_t *
bare_duckdb_query(js_env_t *env, js_callback_info_t *info) {
    int err;
    size_t argc = 2;
    js_value_t *argv[2];

    err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
    assert(err == 0);

    if (argc != 2) {
        js_throw_error(env, "Error", "Expected exactly two arguments: database, query");
        return NULL;
    }

    bare_duckdb_t *db;
    err = js_get_value_external(env, argv[0], (void **) &db);
    assert(err == 0);

    if (!db) {
        js_throw_error(env, "Error", "Database not found");
        return NULL;
    }
    if (!db->connection) {
        js_throw_error(env, "Error", "Connection not found. Call connect first.");
        return NULL;
    }

    size_t query_len;
    err = js_get_value_string_utf8(env, argv[1], NULL, 0, &query_len);
    assert(err == 0);

    char *query = malloc(query_len + 1);
    if (query == NULL) {
        js_throw_error(env, "Error", "Out of memory");
        return NULL;
    }
    err = js_get_value_string_utf8(env, argv[1], (utf8_t *) query, query_len, NULL);
    assert(err == 0);

    bare_duckdb_query_t *req = malloc(sizeof(bare_duckdb_query_t));
    if (!req) {
        free(query);
        js_throw_error(env, "Error", "Out of memory");
        return NULL;
    }

    req->db = db;
    req->query = query;
    req->handle.data = req;
    req->error = NULL;
    req->result = NULL;

    js_value_t *promise;
    err = js_create_promise(env, &req->deferred, &promise);
    assert(err == 0);

    uv_loop_t *loop;
    err = js_get_env_loop(env, &loop);
    assert(err == 0);

    err = uv_queue_work(loop, &req->handle, bare_duckdb__on_before_query, bare_duckdb__on_after_query);
    assert(err == 0);

    return promise;
}

static js_value_t *
bare_duckdb_exports(js_env_t *env, js_value_t *exports) {
  int err;

#define V(name, fn) \
  { \
    js_value_t *val; \
    err = js_create_function(env, name, -1, fn, NULL, &val); \
    assert(err == 0); \
    err = js_set_named_property(env, exports, name, val); \
    assert(err == 0); \
  }

  V("open", bare_duckdb_open)
  V("close", bare_duckdb_close)
  V("connect", bare_duckdb_connect)
  V("disconnect", bare_duckdb_disconnect)
  V("query", bare_duckdb_query)
#undef V

  return exports;
}

BARE_MODULE(bare_duckdb, bare_duckdb_exports)
