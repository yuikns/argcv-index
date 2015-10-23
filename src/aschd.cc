#include <cstdio>

#include <vector>
#include <string>
#include <utility>

#include "argcv/ir/index/analyzer/basic_analyzer.hh"
#include "argcv/ir/index/analyzer/basic_tokenlizer.hh"
#include "argcv/ir/index/field.hh"
#include "argcv/ir/index/index.hh"
#include "argcv/ir/index/util.hh"

#include "argcv/nio/tcp_listen.hh"

#include "argcv/storage/hd_storage.hh"

#include "argcv/string/string.hh"
#include "argcv/string/uuid.hh"

#include "asch/pub.hh"
#include "asch/oper.hh"

#include "index/msg/lmt_msg.pb.h"
#include "index/msg/op_msg.pb.h"
#include "index/msg/resp_msg.pb.h"

using namespace asch;
using namespace argcv::nio;

using argcv::ir::index::aidx;
using argcv::ir::index::analyzer::basic_analyzer;
using argcv::ir::index::analyzer::basic_tokenlizer;

using argcv::ir::index::text_field;

using argcv::ir::index::DB_SEPARATOR;

using argcv::string::as_type;
using argcv::string::as_vec;
using argcv::string::split;
using argcv::string::uuid;

using argcv::storage::hd_storage;
using argcv::storage::hd_bw_handler;

// using asch::ophandler;
// using asch::pub;

void query(const std::string& query, aidx& idx, size_t offset = 0, size_t limit = 10) {
    printf("query: %s \n", query.c_str());
    basic_tokenlizer tkz(query);
    basic_analyzer anz(&tkz);

    std::vector<std::pair<std::string, double>> ids = idx.search("title", anz.to_vec());
    size_t ix_begin = offset;
    size_t ix_end = offset + limit > ids.size() ? ids.size() : offset + limit;
    for (size_t ix = ix_begin; ix < ix_end; ix++) {
        uuid id(ids[ix].first);
        printf("[%zu](%f) %s", ix, ids[ix].second, idx.get(id.data()).c_str());
        // printf("%s (%s) , score : %f \n", idx.get(id.data()).c_str(), id.str().c_str(), ids[ix].second);
    }
}

void add(const std::string& str, aidx& idx) {
    uuid id;
    std::string sid = id.data();
    basic_tokenlizer tkz(str);
    basic_analyzer anz(&tkz);
    text_field t(anz);
    idx.save_field(sid, "title", t);
    idx.save(sid, str);
}

void rpc_server();

void rpc_server() {
    hd_storage stg("config.ini");
    aidx idx(&stg);

    tcp_listen pool(9529, 200000);
    size_t sz_min_sleep = 100;
    size_t sz_max_sleep = 300000;
    size_t sz_sleep = sz_min_sleep;
    if (pool._error_no() != 0) {
        printf("pool establish failed .. %d \n", pool._error_no());
    } else {
        printf("pool established .. %d \n", pool._error_no());
        std::map<int, int32_t> id_2_sz;
        for (;;) {
            int id = pool.poll(0);
            if (id != -1) {
                sz_sleep = sz_min_sleep;
                tcp_listen::conn& c = pool[id];  // connector
                if (id_2_sz.find(id) == id_2_sz.end()) {
                    // new session
                    int32_t st = pool.pull(id, 4);
                    if (st >= 4) {
                        std::string s = c.to_str();
                        int32_t ssz = 0;
                        for (size_t i = 0; i < s.length(); i++) {
                            ssz = (ssz << 8) | (0xff & s[i]);
                        }
                        id_2_sz[id] = ssz;
                        c.flush();
                        c.clear();
                    } else {
                        if (c.closed()) {
                            id_2_sz.erase(id);
                        } else {
                        }
                        c.flush();
                    }
                } else {
                    int32_t st = pool.pull(id, id_2_sz[id]);
                    if (st >= id_2_sz[id]) {
                        // c.to_str()
                        ophandler _o_handler(c.to_str(), &idx);
                        _o_handler.exec();
                        c.write(_o_handler.resp(), _o_handler.resp().length());
                        c.flush();
                        c.clear();
                        id_2_sz.erase(id);
                    } else {
                        // continue
                        if (c.closed()) {
                            id_2_sz.erase(id);
                        }
                        c.flush();
                    }
                }
            } else {
                // wait
                sz_sleep *= 2;
                if (sz_sleep > sz_max_sleep) {
                    sz_sleep = sz_max_sleep;
                }
                usleep(sz_sleep);
            }
        }
        printf("stop..... \n");
    }
}

int main(int argc, char* argv[]) {
    rpc_server();

    // op_msg msg_send;
    // msg_send.set_op("add");
    // msg_send.set_id("1231231231");
    // std::string buffer;
    // msg_send.SerializeToString(&buffer);
    // idx.dump();
    return 0;
}

/*
// aidx &idx(aidx::instance(&stg));
FILE* f = fopen("data.txt", "r");
if (f == NULL) {
    assert(0);
    return -1;
}
char buff[2048];
while (fgets(buff, 2048, f) != NULL) {
    add(buff, idx);
}
fclose(f);
while (fgets(buff, 2048, stdin) != NULL) {
    query(buff, idx);
}*/
// printf("%s\n", p.to_json().c_str());