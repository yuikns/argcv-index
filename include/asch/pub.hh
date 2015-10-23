#ifndef ASCH_PUB_HH
#define ASCH_PUB_HH

#include <string>
#include <vector>

#include "argcv/string/uuid.hh"

#include "index/msg/lmt_msg.pb.h"
#include "index/msg/op_msg.pb.h"
#include "index/msg/resp_msg.pb.h"

namespace asch {
using ::argcv::string::uuid;

class pub {
public:
    std::string id;
    std::string mid;  // id in mongodb
    std::string title;
    std::string names;
    
};
}

#endif  // ASCH_PUB_HH