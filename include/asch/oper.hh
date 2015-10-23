#ifndef ASCH_OPER_HH
#define ASCH_OPER_HH

#include <cstdio>
#include <cstring>

#include <vector>
#include <string>
#include <utility>

#include "argcv/ir/index/analyzer/basic_analyzer.hh"
#include "argcv/ir/index/analyzer/basic_tokenlizer.hh"
#include "argcv/ir/index/field.hh"
#include "argcv/ir/index/index.hh"
#include "argcv/ir/index/util.hh"

#include "argcv/storage/hd_storage.hh"

#include "argcv/string/string.hh"
#include "argcv/string/uuid.hh"

#include "asch/pub.hh"
#include "asch/oper.hh"

#include "index/msg/lmt_msg.pb.h"
#include "index/msg/op_msg.pb.h"
#include "index/msg/resp_msg.pb.h"

namespace asch {
using ::argcv::string::uuid;
using ::argcv::ir::index::aidx;
using ::argcv::ir::index::analyzer::basic_analyzer;
using ::argcv::ir::index::analyzer::basic_tokenlizer;

using ::argcv::ir::index::text_field;

class ophandler {
public:
    ophandler(const std::string& m, aidx* _idx) : _idx(_idx), m(m), _ok(false), _fail(false) {}

    void exec() {
        op_msg msg_recv;
        msg_recv.ParseFromString(m);
        //printf("OP: %s\n", msg_recv.op().c_str());
        if (msg_recv.op() == "add") {
            //printf("add(%s) %s\n", msg_recv.id().c_str(), msg_recv.title().c_str());
            uuid uid;
            std::string sid = uid.data();

            basic_tokenlizer tkz(msg_recv.title());
            basic_analyzer anz(&tkz);
            text_field t(anz);
            _idx->save_field(sid, "title", t);

            basic_tokenlizer ntkz(msg_recv.title());
            basic_analyzer nanz(&tkz);
            text_field nt(anz);
            _idx->save_field(sid, "names", t);
            _idx->save(sid, msg_recv.id());
            _resp = "ok";
        } else if(msg_recv.op() == "query"){  // query
            int32_t offset = msg_recv.lmt().offset();
            int32_t limit = msg_recv.lmt().limit();
            std::vector<std::pair<std::string, double>> oids;
            size_t total = 0;
            if (msg_recv.has_title()) {
                query(msg_recv.title(),"title",offset,limit,oids,total);
            } else if(msg_recv.has_names()) {
                query(msg_recv.names(),"names",offset,limit,oids,total);
            }
            char buff[32];
            sprintf(buff,"%zu",total);
            _resp = "";
            _resp += buff;
            for(size_t ix = 0 ; ix < oids.size() ; ix ++ ) {
                sprintf(buff,",%s|%f",oids[ix].first.c_str(),oids[ix].second);
                _resp += buff;
            }
            //printf("final: %s \n", _resp.c_str()); fflush(NULL);
        }
    }

    bool ok() { return _ok; }

    bool fail() { return _fail; }

    const std::string& resp() { return _resp; }

    void query(const std::string& query, const std::string& field_name, size_t offset, size_t limit,
               std::vector<std::pair<std::string, double>>& oids, size_t& total) {
        printf("query: %s \n", query.c_str());
        basic_tokenlizer tkz(query);
        basic_analyzer anz(&tkz);

        std::vector<std::pair<std::string, double>> ids = _idx->search(field_name, anz.to_vec(),3000);
        total = ids.size();
        size_t ix_begin = offset;
        size_t ix_end = offset + limit > total ? total : offset + limit;
        for (size_t ix = ix_begin; ix < ix_end; ix++) {
            // uuid id(ids[ix].first);
            // printf("[%zu](%f) %s", ix, ids[ix].second, _idx->get(id.data()).c_str());
            // printf("%s (%s) , score : %f \n", idx.get(id.data()).c_str(), id.str().c_str(),
            // ids[ix].second);
            oids.push_back(std::make_pair(_idx->get(ids[ix].first), ids[ix].second));
        }
    }

    void add(const std::string& str, const std::string& field_name) {
        uuid id;
        std::string sid = id.data();
        basic_tokenlizer tkz(str);
        basic_analyzer anz(&tkz);
        text_field t(anz);
        _idx->save_field(sid, field_name, t);
        _idx->save(sid, str);
    }

private:
    aidx* _idx;
    const std::string m;
    bool _ok;
    bool _fail;
    std::string _resp;
};
}

#endif  // ASCH_OPER_HH